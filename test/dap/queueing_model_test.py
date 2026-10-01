#!/usr/bin/env python3
"""Send-time verification on the broker's outgoing path.

Covers DELETE on queued messages (including after a subscriber's queue
overflowed), re-verification of messages whose stamp went stale against
the current MP and SP, and prioritized intake of state changes.

Usage: python3 test/dap/queueing_model_test.py [port]
"""
import os
import socket
import struct
import subprocess
import sys
import tempfile
import time

import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import mosq_test  # noqa: E402
import mqtt5_props  # noqa: E402

ROOT = os.environ.get("BUILD_ROOT") or os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
BROKER = os.path.join(ROOT, "src", "mosquitto")
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 18834
HOST = "127.0.0.1"

OSYS = "$OP_SYS"
OP_PURPOSE = "DAP_OP"

failures = []
clients = []


def check(cond, what):
    print(("ok   - " if cond else "FAIL - ") + what)
    if not cond:
        failures.append(what)


def wait_for(pred, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.05)
    return pred()


class Broker:
    def __init__(self, extra_conf=""):
        self.workdir = tempfile.mkdtemp(prefix="dap-queue-")
        conf = os.path.join(self.workdir, "mosquitto.conf")
        with open(conf, "w") as f:
            f.write(f"listener {PORT} {HOST}\nallow_anonymous true\n" + extra_conf)
        self.log_path = os.path.join(self.workdir, "broker.log")
        self.log = open(self.log_path, "w")
        self.proc = subprocess.Popen([BROKER, "-c", conf, "-v"], stdout=self.log, stderr=subprocess.STDOUT)
        if not wait_for(lambda: "Opening ipv4 listen socket" in self.read_log()) or self.proc.poll() is not None:
            print(self.read_log())
            raise SystemExit("broker failed to start")
        time.sleep(0.1)

    def read_log(self):
        with open(self.log_path, errors="replace") as f:
            return f.read()

    def stop(self):
        if self.proc.poll() is None:
            self.proc.terminate()
        try:
            rc = self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            rc = self.proc.wait()
        self.log.close()
        return rc


class Publisher:
    """paho client that registers MPs, publishes data and sends operations."""

    def __init__(self, client_id="pub1"):
        self.msgs = []
        self.c = mqtt.Client(CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt.MQTTv5)
        self.c.on_message = lambda c, u, m: self.msgs.append(
            (m.topic, dict(getattr(m.properties, "UserProperty", None) or [])))
        self.c.connect(HOST, PORT)
        self.c.loop_start()
        clients.append(self)
        props = Properties(PacketTypes.SUBSCRIBE)
        props.UserProperty = [("DAP-SP", OP_PURPOSE)]
        self.c.subscribe(f"OP_NOTIF/{client_id}", qos=1, properties=props)
        time.sleep(0.2)

    def publish(self, topic, pairs, payload=b"", qos=1):
        props = Properties(PacketTypes.PUBLISH)
        props.UserProperty = [("DAP-Allow", "1")] + pairs
        self.c.publish(topic, payload=payload, qos=qos, properties=props).wait_for_publish(5)

    def register(self, mp, topic):
        self.publish("$MP_REG", [("DAP-MP", f"{mp}:{topic}")], qos=0)
        time.sleep(0.2)

    def operation(self, op, topic_filter, purposes=None):
        pairs = [("DAP-OpType", op), ("DAP-OpTFs", topic_filter)]
        if purposes is not None:
            pairs.append(("DAP-OpPFs", purposes))
        self.publish(OSYS, pairs)

    def got_status(self, status):
        return [m for m in self.msgs if m[1].get("DAP-Status") == status]

    def stop(self):
        self.c.loop_stop()
        self.c.disconnect()


class RawPublisher:
    """Raw MQTT v5 publisher, so several packets reach the broker in one read."""

    def __init__(self, client_id="rawpub"):
        self.sock = socket.create_connection((HOST, PORT), timeout=5)
        self.sock.send(mosq_test.gen_connect(client_id, proto_ver=5))
        connack = self.sock.recv(64)
        assert connack[0] == 0x20 and connack[3] == 0, "CONNACK failed"

    def packet(self, topic, pairs, payload=b"", alias=None, qos=0, mid=0):
        props = b"".join(mqtt5_props.gen_string_pair_prop(mqtt5_props.USER_PROPERTY, k, v)
                         for k, v in [("DAP-Allow", "1")] + pairs)
        if alias is not None:
            props += mqtt5_props.gen_uint16_prop(mqtt5_props.TOPIC_ALIAS, alias)
        return mosq_test.gen_publish(topic, qos, payload, mid=mid, proto_ver=5, properties=props)

    def send(self, *packets):
        self.sock.sendall(b"".join(packets))
        time.sleep(0.2)

    def close(self):
        self.sock.close()


class Subscriber:
    """Raw MQTT v5 client, so the test decides when to read and acknowledge."""

    def __init__(self, client_id, persistent=False, receive_maximum=0, rcvbuf=0):
        self.id = client_id
        self.persistent = persistent
        self.receive_maximum = receive_maximum
        self.rcvbuf = rcvbuf
        self.sock = None
        self.next_mid = 1
        self.connect(clean_start=True)

    def connect(self, clean_start=False):
        props = b""
        if self.persistent:
            props += mqtt5_props.gen_uint32_prop(mqtt5_props.SESSION_EXPIRY_INTERVAL, 3600)
        if self.receive_maximum:
            props += mqtt5_props.gen_uint16_prop(mqtt5_props.RECEIVE_MAXIMUM, self.receive_maximum)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.rcvbuf:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.rcvbuf)
        self.sock.settimeout(5)
        self.sock.connect((HOST, PORT))
        self.sock.send(mosq_test.gen_connect(self.id, proto_ver=5, clean_session=clean_start, properties=props))
        cmd, body = self.read_packet(5)
        assert cmd & 0xF0 == 0x20 and body[1] == 0, "CONNACK failed"

    def disconnect(self):
        self.sock.send(mosq_test.gen_disconnect(proto_ver=5))
        self.sock.close()
        time.sleep(0.2)

    def subscribe(self, topic, sps, qos=1):
        props = b"".join(mqtt5_props.gen_string_pair_prop(mqtt5_props.USER_PROPERTY, "DAP-SP", sp) for sp in sps)
        mid = self.next_mid
        self.next_mid += 1
        self.sock.send(mosq_test.gen_subscribe(mid, topic, qos, proto_ver=5, properties=props))
        # The broker may interleave PUBLISHes with the SUBACK; keep them for later reads.
        self.pending = []
        while True:
            cmd, body = self.read_packet(5)
            if cmd & 0xF0 == 0x90:
                return
            self.pending.append((cmd, body))

    def unsubscribe(self, topic):
        mid = self.next_mid
        self.next_mid += 1
        self.sock.send(mosq_test.gen_unsubscribe(mid, topic, proto_ver=5))
        while self.read_packet(5)[0] & 0xF0 != 0xB0:
            pass

    def puback(self, mid):
        self.sock.send(mosq_test.gen_puback(mid, proto_ver=5))

    def read_packet(self, timeout):
        self.sock.settimeout(timeout)
        cmd = self._recv_exact(1)[0]
        mult, length = 1, 0
        while True:
            b = self._recv_exact(1)[0]
            length += (b & 0x7F) * mult
            mult *= 128
            if not b & 0x80:
                break
        return cmd, self._recv_exact(length)

    def _recv_exact(self, n):
        data = b""
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("connection closed")
            data += chunk
        return data

    def read_publishes(self, idle=1.5, ack=True):
        """Read PUBLISHes until none arrives for `idle` seconds: [(topic, payload, mid)]."""
        out = []
        packets = getattr(self, "pending", [])
        self.pending = []
        while True:
            if packets:
                cmd, body = packets.pop(0)
            else:
                try:
                    cmd, body = self.read_packet(idle)
                except (socket.timeout, ConnectionError):
                    return out
            if cmd & 0xF0 != 0x30:
                continue
            qos = (cmd >> 1) & 3
            tlen = struct.unpack("!H", body[:2])[0]
            topic = body[2:2 + tlen].decode()
            pos = 2 + tlen
            mid = None
            if qos:
                mid = struct.unpack("!H", body[pos:pos + 2])[0]
                pos += 2
            plen, mult = 0, 1
            while True:
                b = body[pos]
                pos += 1
                plen += (b & 0x7F) * mult
                mult *= 128
                if not b & 0x80:
                    break
            out.append((topic, body[pos + plen:], mid))
            if qos and ack:
                self.puback(mid)

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def payloads(publishes, topic):
    return [p for t, p, _ in publishes if t == topic]


def delete_case(overflow=False, gap=False):
    """A DELETE drops a matching message still queued for an offline subscriber."""
    broker = Broker("use_metadata_operation_support true\nmax_queued_messages 1\n")
    try:
        sub = Subscriber("subA", persistent=True)
        if gap:
            # Unsubscribing leaves an empty slot ahead of the subscription below.
            sub.subscribe("t/other", ["qa"])
        sub.subscribe("t/a", ["qa"])
        if gap:
            sub.unsubscribe("t/other")
        pub = Publisher()
        pub.register("qa", "t/a")
        pub.publish("t/a", [], payload=b"m0")
        check(payloads(sub.read_publishes(), "t/a") == [b"m0"], "subscriber receives data before the DELETE")

        if overflow:
            # m2 does not fit in the offline queue and is dropped for this subscriber.
            sub.disconnect()
            pub.publish("t/a", [], payload=b"m1")
            pub.publish("t/a", [], payload=b"m2")
            time.sleep(0.2)
            sub.connect()
            check(payloads(sub.read_publishes(), "t/a") == [b"m1"], "only the message that fit is delivered on reconnect")

        sub.disconnect()
        pub.publish("t/a", [], payload=b"m3")
        pub.operation("DELETE", "t/a")
        check(wait_for(lambda: pub.got_status("Pending")), "requester gets a Pending ack for the DELETE")
        sub.connect()
        got = payloads(sub.read_publishes(), "t/a")
        check(b"m3" not in got, "DELETE drops the queued message%s%s (got %s)"
              % (" after the queue overflowed" if overflow else "",
                 " after an earlier unsubscribe" if gap else "", got))
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def scoped_op_case(op, topic_filter, purposes, sps_kept):
    """An operation reaches the queued copies whose topic matches its DAP-OpTFs and
    whose subscription's SP shares a purpose with its DAP-OpPFs. A RESTRICT revokes
    those purposes, so it drops a copy whose SP uses one of them."""
    broker = Broker("use_metadata_operation_support true\n")
    try:
        subs = {}
        for i, sp in enumerate(sps_kept):
            sub = Subscriber("subS%d" % i, persistent=True)
            sub.subscribe("t/s/1", sp.split("|"))
            subs[sp] = sub
        pub = Publisher()
        pub.register("qa|qb|qc", "t/s/1")
        pub.publish("t/s/1", [], payload=b"m0")
        for sp, sub in subs.items():
            check(payloads(sub.read_publishes(), "t/s/1") == [b"m0"], "SP %s receives data before the %s" % (sp, op))
            sub.disconnect()
        pub.publish("t/s/1", [], payload=b"m1")
        pub.operation(op, topic_filter, purposes)
        check(wait_for(lambda: pub.msgs), "requester gets a reply to the %s" % op)
        for sp, sub in subs.items():
            sub.connect()
            got = payloads(sub.read_publishes(), "t/s/1")
            expected = [b"m1"] if sps_kept[sp] else []
            check(got == expected, "%s on %s for %s: SP %s got %s (expected %s)"
                  % (op, topic_filter, purposes, sp, got, expected))
            sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def same_second_case():
    """A DELETE covers exactly the messages received before it, even within one second."""
    broker = Broker("use_metadata_operation_support true\n")
    try:
        sub = Subscriber("subT", persistent=True)
        sub.subscribe("t/t", ["qa"])
        pub = Publisher()
        pub.register("qa", "t/t")
        pub.publish("t/t", [], payload=b"m0")
        check(payloads(sub.read_publishes(), "t/t") == [b"m0"], "subscriber receives data before the DELETE")
        sub.disconnect()
        pub.publish("t/t", [], payload=b"m1")
        pub.operation("DELETE", "t/t")
        pub.publish("t/t", [], payload=b"m2")
        check(wait_for(lambda: pub.got_status("Pending")), "requester gets a Pending ack for the DELETE")
        sub.connect()
        got = payloads(sub.read_publishes(), "t/t")
        check(got == [b"m2"], "the DELETE drops m1 but not m2 published right after it (got %s)" % got)
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def drop_then_stale_case():
    """After a reconnect, a DELETEd message followed by stale ones keeps publish order."""
    broker = Broker("use_metadata_operation_support true\n")
    try:
        sub = Subscriber("subE", persistent=True, receive_maximum=3)
        sub.subscribe("t/e", ["qa"])
        pub = Publisher()
        pub.register("qa", "t/e")
        pub.publish("t/e", [], payload=b"m0")
        check(payloads(sub.read_publishes(), "t/e") == [b"m0"], "subscriber receives data before the DELETE")
        sub.disconnect()
        pub.publish("t/e", [], payload=b"m1")
        pub.operation("DELETE", "t/e")
        check(wait_for(lambda: pub.got_status("Pending")), "requester gets a Pending ack for the DELETE")
        for i in range(2, 5):
            pub.publish("t/e", [], payload=b"m%d" % i)
        pub.register("qa", "t/e")  # MP version 2: the stamps of m2-m4 are stale
        sub.connect()
        got = payloads(sub.read_publishes(), "t/e")
        check(got == [b"m2", b"m3", b"m4"], "m1 is dropped and the rest keep their order (got %s)" % got)
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def drop_refill_case():
    """A message dropped from the queue does not stall the messages behind it."""
    broker = Broker("use_metadata_operation_support true\n")
    try:
        sub = Subscriber("subF", receive_maximum=1)
        sub.subscribe("t/f", ["qa"])
        pub = Publisher()
        pub.register("qa", "t/f")
        pub.publish("t/f", [], payload=b"m1")
        pub.publish("t/f", [], payload=b"m2")
        first = sub.read_publishes(idle=0.5, ack=False)
        check(payloads(first, "t/f") == [b"m1"], "only m1 is in flight (receive maximum 1)")
        pub.operation("DELETE", "t/f")
        check(wait_for(lambda: pub.got_status("Pending")), "requester gets a Pending ack for the DELETE")
        pub.publish("t/f", [], payload=b"m3")
        sub.puback(first[0][2])
        got = payloads(sub.read_publishes(), "t/f")
        check(got == [b"m3"], "m2 is dropped and m3 follows without further traffic (got %s)" % got)
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def stale_mp_case():
    """QoS 0 messages queued while offline, stamped under an older MP, all arrive in order."""
    broker = Broker("queue_qos0_messages true\n")
    try:
        sub = Subscriber("subB", persistent=True)
        sub.subscribe("t/b", ["qa"], qos=0)
        pub = Publisher()
        pub.register("qa", "t/b")
        sub.disconnect()
        for i in range(1, 4):
            pub.publish("t/b", [], payload=b"m%d" % i, qos=0)
        time.sleep(0.2)
        pub.register("qa|qb", "t/b")  # MP version 2: the queued stamps are stale
        sub.connect()
        got = payloads(sub.read_publishes(), "t/b")
        check(got == [b"m1", b"m2", b"m3"], "re-verified messages are delivered in order (got %s)" % got)
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def qos0_quota_case():
    """QoS 0 messages sent from the offline queue leave the QoS 1 send quota intact."""
    broker = Broker("queue_qos0_messages true\n")
    try:
        sub = Subscriber("subG", persistent=True, receive_maximum=2)
        sub.subscribe("t/g", ["qa"])
        pub = Publisher()
        pub.register("qa", "t/g")
        sub.disconnect()
        for i in range(1, 4):
            pub.publish("t/g", [], payload=b"z%d" % i, qos=0)
        pub.register("qa", "t/g")  # stale stamps: each queued message is bumped once
        sub.connect()
        got = payloads(sub.read_publishes(), "t/g")
        check(got == [b"z1", b"z2", b"z3"], "queued QoS 0 messages are delivered (got %s)" % got)
        for i in range(1, 4):
            pub.publish("t/g", [], payload=b"q%d" % i, qos=1)
        got = payloads(sub.read_publishes(), "t/g")
        check(got == [b"q1", b"q2", b"q3"], "QoS 1 messages still flow afterwards (got %s)" % got)
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def sp_change_case(new_sps, still_allowed):
    """A message waiting while the subscriber's SP changes is re-checked against the new SP."""
    broker = Broker()
    try:
        sub = Subscriber("subC", receive_maximum=1)
        sub.subscribe("t/c", ["qa"])
        pub = Publisher()
        pub.register("qa|qz", "t/c")
        pub.publish("t/c", [], payload=b"m1")
        pub.publish("t/c", [], payload=b"m2")
        first = sub.read_publishes(idle=0.5, ack=False)
        check(payloads(first, "t/c") == [b"m1"], "only m1 is in flight (receive maximum 1)")

        sub.subscribe("t/c", new_sps)  # SP version 2 while m2 waits
        sub.puback(first[0][2])
        time.sleep(0.3)  # m2 is re-checked before the MP below changes
        pub.register("|".join(new_sps), "t/c")
        pub.publish("t/c", [], payload=b"m3")
        got = payloads(sub.read_publishes(), "t/c")
        expected = [b"m2", b"m3"] if still_allowed else [b"m3"]
        check(got == expected, "after the SP becomes %s, delivered %s (expected %s)" % (new_sps, got, expected))
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def mp_change_case(new_mp, still_allowed, conf=""):
    """A message waiting while its publisher changes the topic's MP is re-checked against the new MP."""
    broker = Broker(conf)
    try:
        sub = Subscriber("subM", receive_maximum=1)
        sub.subscribe("t/m", ["qa"])
        pub = Publisher()
        pub.register("qa|qz", "t/m")
        pub.publish("t/m", [], payload=b"m1")
        pub.publish("t/m", [], payload=b"m2")
        first = sub.read_publishes(idle=0.5, ack=False)
        check(payloads(first, "t/m") == [b"m1"], "only m1 is in flight (receive maximum 1)")

        pub.register(new_mp, "t/m")  # MP version 2 while m2 waits
        sub.puback(first[0][2])
        time.sleep(0.3)  # m2 is re-checked before the MP below changes
        pub.register("qa", "t/m")
        pub.publish("t/m", [], payload=b"m3")
        got = payloads(sub.read_publishes(), "t/m")
        expected = [b"m2", b"m3"] if still_allowed else [b"m3"]
        check(got == expected, "after the MP becomes %s%s, delivered %s (expected %s)"
              % (new_mp, " behind a mount point" if conf else "", got, expected))
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def intake_mp_case():
    """An MP update read together with earlier data is applied before that data."""
    broker = Broker()
    try:
        sub = Subscriber("subI")
        sub.subscribe("t/i", ["qa"])
        pub = RawPublisher()
        pub.send(pub.packet("$MP_REG", [("DAP-MP", "qa:t/i")]))
        pub.send(pub.packet("t/i", [], b"m0"))
        check(payloads(sub.read_publishes(), "t/i") == [b"m0"], "subscriber receives data before the MP update")
        pub.send(pub.packet("t/i", [], b"m1"),
                 pub.packet("$MP_REG", [("DAP-MP", "qb:t/i")]),
                 pub.packet("t/i", [], b"m2"))
        got = payloads(sub.read_publishes(), "t/i")
        check(got == [], "the MP update read with m1 and m2 applies to both (got %s)" % got)
        pub.close()
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def intake_delete_case():
    """A DELETE read together with earlier data reaches broker state first and covers it."""
    broker = Broker("use_metadata_operation_support true\n")
    try:
        sub = Subscriber("subJ")
        sub.subscribe("t/j", ["qa"])
        pub = RawPublisher()
        pub.send(pub.packet("$MP_REG", [("DAP-MP", "qa:t/j")]))
        pub.send(pub.packet("t/j", [], b"m0"))
        check(payloads(sub.read_publishes(), "t/j") == [b"m0"], "subscriber receives data before the DELETE")
        pub.send(pub.packet("t/j", [], b"m1"),
                 pub.packet(OSYS, [("DAP-OpType", "DELETE"), ("DAP-OpTFs", "t/j")]),
                 pub.packet("t/j", [], b"m2"))
        got = payloads(sub.read_publishes(), "t/j")
        check(got == [b"m2"], "the DELETE covers m1, read before it, but not m2 (got %s)" % got)
        pub.close()
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def intake_disconnect_case(abrupt=False):
    """Data read together with the end of the publisher's connection is still delivered, in order."""
    broker = Broker()
    try:
        sub = Subscriber("subK")
        sub.subscribe("t/k", ["qa"])
        pub = RawPublisher()
        pub.send(pub.packet("$MP_REG", [("DAP-MP", "qa:t/k")]))
        data = [pub.packet("t/k", [], b"m1"), pub.packet("t/k", [], b"m2")]
        if abrupt:
            pub.sock.sendall(b"".join(data))
            pub.close()
        else:
            pub.send(*data, mosq_test.gen_disconnect(proto_ver=5))
        got = payloads(sub.read_publishes(), "t/k")
        check(got == [b"m1", b"m2"], "data sent before the connection %s is delivered (got %s)"
              % ("closed" if abrupt else "sent DISCONNECT", got))
        pub.close()
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def intake_alias_case():
    """A PUBLISH that names its topic only by alias keeps its place behind the
    PUBLISH that set the alias, even within one read."""
    broker = Broker()
    try:
        sub = Subscriber("subL")
        sub.subscribe("t/l/#", ["qa"])
        pub = RawPublisher()
        pub.send(pub.packet("$MP_REG", [("DAP-MP", "qa:t/l/a"), ("DAP-MP", "qa:t/l/b")]))
        pub.send(pub.packet("t/l/a", [], b"m1", alias=1), pub.packet("", [], b"m2", alias=1))
        pub.send(pub.packet("t/l/b", [], b"m3", alias=1), pub.packet("", [], b"m4", alias=1))
        got = [(t, p) for t, p, _ in sub.read_publishes()]
        expected = [("t/l/a", b"m1"), ("t/l/a", b"m2"), ("t/l/b", b"m3"), ("t/l/b", b"m4")]
        check(got == expected, "aliases set and used within one read resolve in order (got %s)" % got)
        pub.close()
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def intake_own_order_case():
    """A client's SUBSCRIBE does not go ahead of its own earlier PUBLISH."""
    broker = Broker()
    try:
        client = Subscriber("subM2")
        mp = b"".join(mqtt5_props.gen_string_pair_prop(mqtt5_props.USER_PROPERTY, k, v)
                      for k, v in [("DAP-Allow", "1"), ("DAP-MP", "qa:t/o")])
        client.sock.send(mosq_test.gen_publish("$MP_REG", 0, b"", proto_ver=5, properties=mp))
        time.sleep(0.2)
        allow = mqtt5_props.gen_string_pair_prop(mqtt5_props.USER_PROPERTY, "DAP-Allow", "1")
        sp = mqtt5_props.gen_string_pair_prop(mqtt5_props.USER_PROPERTY, "DAP-SP", "qa")
        client.sock.send(mosq_test.gen_publish("t/o", 0, b"m1", proto_ver=5, properties=allow)
                         + mosq_test.gen_subscribe(1, "t/o", 0, proto_ver=5, properties=sp))
        cmd, _ = client.read_packet(5)
        check(cmd & 0xF0 == 0x90, "SUBACK comes first")
        got = payloads(client.read_publishes(), "t/o")
        check(got == [], "the client does not receive its own earlier PUBLISH (got %s)" % got)
        client.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def intake_error_order_case():
    """A state change that goes ahead of the client's data and fails still lets that
    data be acknowledged before the DISCONNECT, which must be the last packet."""
    broker = Broker()
    try:
        sub = Subscriber("subN")
        sub.subscribe("t/n", ["qa"])
        pub = RawPublisher()
        pub.send(pub.packet("$MP_REG", [("DAP-MP", "qa:t/n")]))
        pub.sock.sendall(pub.packet("t/n", [], b"m1", qos=1, mid=7)
                         + pub.packet("$MP_REG", [("DAP-MP", "no-separator")]))
        first = pub.sock.recv(1)
        check(first == b"\x40", "the PUBACK for the earlier data comes before the DISCONNECT (got %s)" % first.hex())
        rest = pub.sock.recv(64)
        check(b"\xe0" in rest, "the malformed registration then ends the connection")
        check(payloads(sub.read_publishes(), "t/n") == [b"m1"], "the earlier data is delivered")
        pub.close()
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def mp_widen_case():
    """A widened MP does not let a waiting message reach a purpose its own MP never permitted."""
    broker = Broker()
    try:
        sub = Subscriber("subW", receive_maximum=1)
        sub.subscribe("t/w", ["qa"])
        pub = Publisher()
        pub.register("qa", "t/w")
        pub.publish("t/w", [], payload=b"m1")
        pub.publish("t/w", [], payload=b"m2")
        first = sub.read_publishes(idle=0.5, ack=False)
        check(payloads(first, "t/w") == [b"m1"], "only m1 is in flight (receive maximum 1)")

        pub.register("qa|qb", "t/w")  # MP widened while m2 waits
        sub.subscribe("t/w", ["qb"])
        sub.puback(first[0][2])
        time.sleep(0.3)
        pub.publish("t/w", [], payload=b"m3")
        got = payloads(sub.read_publishes(), "t/w")
        check(got == [b"m3"], "m2, published for qa only, is not delivered for qb (got %s)" % got)
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def stop_clients():
    for c in clients:
        c.stop()
    clients.clear()


def main():
    print("# DELETE on queued messages")
    delete_case()
    delete_case(overflow=True)
    delete_case(gap=True)
    drop_refill_case()
    same_second_case()
    scoped_op_case("DELETE", "t/+/1", "qb", {"qa": True, "qb": False, "qa|qc": True, "qb|qc": False})
    scoped_op_case("DELETE", "t/s/#", "qx", {"qa": True, "qb": True})
    scoped_op_case("DELETE", "t/other", None, {"qa": True})
    print("# RESTRICT on queued messages")
    scoped_op_case("RESTRICT", "t/s/1", "qa", {"qa": False, "qb": True, "qb|qc": True, "qa|qb": False})
    scoped_op_case("RESTRICT", "t/s/1", None, {"qa": False, "qb|qc": False})
    print("# re-verification of stale stamps")
    stale_mp_case()
    drop_then_stale_case()
    qos0_quota_case()
    # m1 and m2 carry MP qa|qz; an SP is admitted only when the MP permits all of it.
    sp_change_case(["qb"], still_allowed=False)
    sp_change_case(["qa", "qz"], still_allowed=True)
    sp_change_case(["qa", "qb"], still_allowed=False)
    mp_change_case("qa|qb", still_allowed=True)
    mp_change_case("qz", still_allowed=False)
    mp_change_case("qz", still_allowed=False, conf="mount_point m/\n")
    mp_widen_case()
    print("# prioritized intake")
    intake_mp_case()
    intake_delete_case()
    intake_disconnect_case()
    intake_disconnect_case(abrupt=True)
    intake_alias_case()
    intake_own_order_case()
    intake_error_order_case()

    print()
    if failures:
        print("QUEUEING MODEL TEST FAILED: %d check(s)" % len(failures))
        sys.exit(1)
    print("QUEUEING MODEL TEST PASSED")

if __name__ == "__main__":
    main()
