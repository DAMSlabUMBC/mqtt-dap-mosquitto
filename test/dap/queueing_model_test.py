#!/usr/bin/env python3
"""Send-time verification on the broker's outgoing path.

Covers DELETE on queued messages (including after a subscriber's queue
overflowed), re-verification of messages whose stamp went stale against
the current MP and SP, and prioritized intake of state changes.

Usage: python3 test/dap/queueing_model_test.py [port]
"""
import contextlib
import csv
import os
import signal
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

    def operation(self, op, topic_filter, purposes=None, deadline=None, new_mp=None):
        pairs = [("DAP-OpType", op), ("DAP-OpTFs", topic_filter)]
        if new_mp is not None:
            pairs.append(("DAP-OpParam", new_mp))
        if purposes is not None:
            pairs.append(("DAP-OpPFs", purposes))
        if deadline is not None:
            pairs.append(("DAP-Deadline", str(deadline)))
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


def scoped_op_case(op, topic_filter, purposes, sps_kept, new_mp=None):
    """An operation reaches the queued copies whose topic matches its DAP-OpTFs and
    whose subscription's SP shares a purpose with its DAP-OpPFs. A RESTRICT gives those
    copies the MP in its DAP-OpParam, so it drops a copy that MP doesn't permit."""
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
        pub.operation(op, topic_filter, purposes, new_mp=new_mp)
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


def restrict_needs_new_mp_case():
    """A RESTRICT without DAP-OpParam is refused and changes nothing."""
    broker = Broker("use_metadata_operation_support true\n")
    try:
        sub = Subscriber("subN", persistent=True)
        sub.subscribe("t/n", ["qa"])
        pub = Publisher()
        pub.register("qa", "t/n")
        pub.publish("t/n", [], payload=b"m0")
        check(payloads(sub.read_publishes(), "t/n") == [b"m0"], "subscriber receives data before the RESTRICT")
        sub.disconnect()
        pub.publish("t/n", [], payload=b"m1")
        pub.operation("RESTRICT", "t/n")
        check(wait_for(lambda: pub.got_status("Failure")), "a RESTRICT without a new MP gets a Failure")
        check(pub.got_status("Failure")[0][1].get("DAP-Reason") == "Missing DAP-OpParam",
              "the Failure names the missing property")
        sub.connect()
        check(payloads(sub.read_publishes(), "t/n") == [b"m1"], "the refused RESTRICT drops nothing")
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def restrict_spares_later_data_case():
    """A RESTRICT changes the MP of data received before it, not after it."""
    broker = Broker("use_metadata_operation_support true\n")
    try:
        sub = Subscriber("subL", persistent=True)
        sub.subscribe("t/l", ["qa"])
        pub = Publisher()
        pub.register("qa|qb", "t/l")
        pub.publish("t/l", [], payload=b"m0")
        check(payloads(sub.read_publishes(), "t/l") == [b"m0"], "subscriber receives data before the RESTRICT")
        sub.disconnect()
        pub.publish("t/l", [], payload=b"m1")
        pub.operation("RESTRICT", "t/l", new_mp="qb")
        pub.publish("t/l", [], payload=b"m2")
        check(wait_for(lambda: pub.got_status("Pending")), "requester gets a Pending ack for the RESTRICT")
        sub.connect()
        got = payloads(sub.read_publishes(), "t/l")
        check(got == [b"m2"], "the RESTRICT drops m1 but not m2 published right after it (got %s)" % got)
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def op_outlives_deadline_case(op, purposes=None, new_mp=None):
    """Covered data stays dropped for a subscriber that returns after the deadline."""
    broker = Broker("use_metadata_operation_support true\n")
    try:
        sub = Subscriber("subO", persistent=True)
        sub.subscribe("t/o", ["qa"])
        pub = Publisher()
        pub.register("qa|qb", "t/o")
        pub.publish("t/o", [], payload=b"m0")
        check(payloads(sub.read_publishes(), "t/o") == [b"m0"], "subscriber receives data before the %s" % op)
        sub.disconnect()
        pub.publish("t/o", [], payload=b"m1")
        pub.operation(op, "t/o", purposes, deadline=int(time.time()) + 2, new_mp=new_mp)
        check(wait_for(lambda: pub.got_status("Pending")), "requester gets a Pending ack for the %s" % op)
        time.sleep(3.5)  # past the deadline: the operation is reclaimed
        pub.publish("t/o", [], payload=b"m2")
        sub.connect()
        got = payloads(sub.read_publishes(), "t/o")
        check(got == [b"m2"], "data the %s covered is not delivered after its deadline (got %s)" % (op, got))
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def op_request_survives_full_queue_case_with_buffer():
    """An operation request to a subscriber whose queue is full is queued, not dropped."""
    broker = Broker("use_metadata_operation_support true\nmax_queued_messages 2\ndap_op_queue_buffer 2\n")
    try:
        sub = Subscriber("subF", receive_maximum=1)
        sub.subscribe("t/f", ["qa"])
        sub.subscribe("OP_REQ/subF", [OP_PURPOSE])
        pub = Publisher()
        pub.register("qa", "t/f")
        for i in range(6):
            pub.publish("t/f", [], payload=b"m%d" % i)
        first = sub.read_publishes(idle=0.5, ack=False)
        pub.operation("HISTORY", "t/f")
        check(wait_for(lambda: pub.got_status("Pending")), "the HISTORY is accepted")
        sub.puback(first[0][2])
        got = first + sub.read_publishes()
        topics = [t for t, _, _ in got]
        check("OP_REQ/subF" in topics, "the request reaches the subscriber despite its full queue (got %s)" % topics)
        check(len(payloads(got, "t/f")) < 6, "data beyond the queue limit is still dropped")
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")
        
def op_request_survives_full_queue_case_without_buffer():
    """An operation request to a subscriber whose queue is full is re-queued, not dropped."""
    broker = Broker("use_metadata_operation_support true\nmax_queued_messages 2\nmax_inflight_messages 1\n")
    try:
        sub = Subscriber("subF", receive_maximum=1)
        sub.subscribe("t/f", ["qa"])
        sub.subscribe("OP_REQ/subF", [OP_PURPOSE])
        pub = Publisher()
        pub.register("qa", "t/f")
        for i in range(6):
            pub.publish("t/f", [], payload=b"m%d" % i)
        pub.operation("HISTORY", "t/f")
        first = sub.read_publishes(ack=False)
        check(wait_for(lambda: pub.got_status("Pending")), "the HISTORY is accepted")
        sub.puback(first[0][2])
        got = first + sub.read_publishes()
        topics = [t for t, _, _ in got]
        check("OP_REQ/subF" in topics, "the request reaches the subscriber despite its full queue (got %s)" % topics)
        check(len(payloads(got, "t/f")) < 6, "data beyond the queue limit is still dropped")
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
    broker = Broker("queue_qos0_messages true\nmax_queued_messages 4\n")
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


def mp_change_case(new_mp, still_allowed, conf="", label=""):
    """A message waiting while its publisher changes the topic's MP is evaluated against the original MP."""
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
        pub.publish("t/m", [], payload=b"m3")
        time.sleep(0.3)  # m2 is re-checked before the MP below changes
        pub.register("qa", "t/m")
        pub.publish("t/m", [], payload=b"m4")
        got = payloads(sub.read_publishes(), "t/m")
        expected = [b"m2", b"m3", b"m4"] if still_allowed else [b"m2", b"m4"]
        check(got == expected, "after the MP becomes %s%s, delivered %s (expected %s)"
              % (new_mp, label, got, expected))
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def stats_case(new_sps, dropped):
    """A waiting message that fails its re-check against a new SP counts in dropped_on_reeval."""
    broker = Broker("stats_interval_ms 100\n")
    try:
        sub = Subscriber("subS", receive_maximum=1)
        sub.subscribe("t/s", ["qa"])
        pub = Publisher()
        pub.register("qa|qz", "t/s")
        pub.publish("t/s", [], payload=b"m1")
        pub.publish("t/s", [], payload=b"m2")
        first = sub.read_publishes(idle=0.5, ack=False)
        sub.subscribe("t/s", new_sps)
        sub.puback(first[0][2])
        sub.read_publishes()
        time.sleep(0.5)
        with open("/tmp/dap_broker_stats.csv", newline="") as f:
            rows = [r for r in csv.DictReader(f) if r["client_id"] == "subS"]
        check(bool(rows) and int(rows[-1]["dropped_on_reeval"]) == dropped, "dropped_on_reeval is %d after the SP becomes %s (got %s)"
              % (dropped, new_sps, rows[-1]["dropped_on_reeval"] if rows else None))
        check(bool(rows) and int(rows[-1]["total_reevaluated"]) == 1, "the waiting message counts in total_reevaluated (got %s)"
              % (rows[-1]["total_reevaluated"] if rows else None))
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def stats_op_drop_case(deadline_s):
    """A waiting message an operation removes counts as re-evaluated and dropped, whether
    the send gate or the sweep at the operation's deadline removes it."""
    broker = Broker("stats_interval_ms 100\nuse_metadata_operation_support true\n")
    try:
        sub = Subscriber("subD", receive_maximum=1)
        sub.subscribe("t/d", ["qa"])
        pub = Publisher()
        pub.register("qa", "t/d")
        pub.publish("t/d", [], payload=b"m1")
        pub.publish("t/d", [], payload=b"m2")
        first = sub.read_publishes(idle=0.5, ack=False)
        pub.operation("DELETE", "t/d", deadline=int(time.time()) + deadline_s if deadline_s else None)
        check(wait_for(lambda: pub.got_status("Pending")), "requester gets a Pending ack")
        if deadline_s:
            time.sleep(deadline_s + 1.5)
        sub.puback(first[0][2])
        got = payloads(sub.read_publishes(), "t/d")
        check(got == [], "the DELETE removes the waiting message (got %s)" % got)
        time.sleep(0.5)
        with open("/tmp/dap_broker_stats.csv", newline="") as f:
            rows = [r for r in csv.DictReader(f) if r["client_id"] == "subD"]
        label = "sweep" if deadline_s else "send gate"
        check(bool(rows) and int(rows[-1]["dropped_on_reeval"]) == 1, "%s drop counts in dropped_on_reeval (got %s)"
              % (label, rows[-1]["dropped_on_reeval"] if rows else None))
        check(bool(rows) and int(rows[-1]["total_reevaluated"]) == 1, "%s drop counts in total_reevaluated (got %s)"
              % (label, rows[-1]["total_reevaluated"] if rows else None))
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def metrics_row_case(qos):
    """Each message's metrics row is written after its fan-out, with its matched leaves
    and re-evaluations. QoS 2 fans out at PUBREL, not at PUBLISH."""
    with contextlib.suppress(FileNotFoundError):
        os.unlink("/tmp/dap_broker_metrics.csv")
    broker = Broker("dap_message_metrics true\nuse_metadata_operation_support true\n")
    try:
        sub = Subscriber("subR", receive_maximum=1)
        sub.subscribe("t/r", ["qa"])
        pub = Publisher()
        pub.register("qa", "t/r")
        pub.publish("t/r", [], payload=b"m1", qos=qos)
        pub.publish("t/r", [], payload=b"m2", qos=qos)
        first = sub.read_publishes(idle=0.5, ack=False)
        pub.operation("DELETE", "t/r")
        check(wait_for(lambda: pub.got_status("Pending")), "requester gets a Pending ack")
        sub.puback(first[0][2])
        check(payloads(sub.read_publishes(), "t/r") == [], "the DELETE removes the waiting message")
        time.sleep(0.5)
        with open("/tmp/dap_broker_metrics.csv", newline="") as f:
            rows = [r for r in csv.DictReader(f) if r["topic"] == "t/r"]
        check([r["num_subs_matched"] for r in rows] == ["1", "1"],
              "qos %d rows have the matched leaves (got %s)" % (qos, rows))
        check(sorted(r["reeval_count"] for r in rows) == ["0", "1"],
              "qos %d rows have the re-evaluation (got %s)" % (qos, rows))
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def metrics_qos2_timing_case():
    """A QoS 2 row times the PUBLISH handling plus PUBREL to send or drop, so it leaves out
    the wait for PUBREL but keeps a wait for the subscriber's inflight slot."""
    with contextlib.suppress(FileNotFoundError):
        os.unlink("/tmp/dap_broker_metrics.csv")
    broker = Broker("dap_message_metrics true\n")
    try:
        sub = Subscriber("subT", receive_maximum=1)
        sub.subscribe("t/w", ["qa"])
        pub = RawPublisher("rawT")
        pub.send(pub.packet("$MP_REG", [("DAP-MP", "qa:t/w")]),
                 pub.packet("t/w", [], b"m1", qos=2, mid=7), pub.packet("t/w", [], b"m2", qos=2, mid=8))
        time.sleep(1)
        pub.send(mosq_test.gen_pubrel(7, proto_ver=5), mosq_test.gen_pubrel(8, proto_ver=5))
        first = sub.read_publishes(idle=0.5, ack=False)
        check(payloads(first, "t/w") == [b"m1"], "the first message goes out after PUBREL")
        time.sleep(1)
        sub.puback(first[0][2])
        check(payloads(sub.read_publishes(), "t/w") == [b"m2"], "the second goes out once the slot frees")
        time.sleep(0.3)
        with open("/tmp/dap_broker_metrics.csv", newline="") as f:
            rows = [r for r in csv.DictReader(f) if r["topic"] == "t/w"]
        check(len(rows) == 2 and all(r["num_subs_matched"] == "1" for r in rows),
              "both rows have the matched leaf (got %s)" % rows)
        times = sorted(int(r["processing_time_ns"]) for r in rows)
        check(len(times) == 2 and times[0] < 500000000, "the PUBREL wait is left out (got %s)" % times)
        check(len(times) == 2 and 1000000000 < times[1] < 2000000000, "the wait for a slot counts (got %s)" % times)
        pub.close()
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def stats_mp_case():
    """A message keeps its MP, so a new MP does not re-evaluate one already waiting."""
    broker = Broker("stats_interval_ms 100\n")
    try:
        sub = Subscriber("subM", receive_maximum=1)
        sub.subscribe("t/m", ["qa"])
        pub = Publisher()
        pub.register("qa", "t/m")
        pub.publish("t/m", [], payload=b"m1")
        pub.publish("t/m", [], payload=b"m2")
        first = sub.read_publishes(idle=0.5, ack=False)
        pub.register("qb", "t/m")
        time.sleep(0.2)
        sub.puback(first[0][2])
        got = payloads(sub.read_publishes(), "t/m")
        check(got == [b"m2"], "the waiting message is delivered under its own MP (got %s)" % got)
        time.sleep(0.5)
        with open("/tmp/dap_broker_stats.csv", newline="") as f:
            rows = [r for r in csv.DictReader(f) if r["client_id"] == "subM"]
        check(bool(rows) and int(rows[-1]["total_reevaluated"]) == 0, "a new MP re-evaluates nothing (got %s)"
              % (rows[-1]["total_reevaluated"] if rows else None))
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


def intake_other_client_case(size=2):
    """A SUBSCRIBE from one client goes ahead of data another client got in first."""
    broker = Broker()
    try:
        late = Subscriber("subP")
        data = RawPublisher("rawdata")
        data.send(data.packet("$MP_REG", [("DAP-MP", "qa:t/p")]))
        broker.proc.send_signal(signal.SIGSTOP)
        data.send(data.packet("t/p", [], b"m" * size))
        sp = mqtt5_props.gen_string_pair_prop(mqtt5_props.USER_PROPERTY, "DAP-SP", "qa")
        late.sock.send(mosq_test.gen_subscribe(1, "t/p", 0, proto_ver=5, properties=sp))
        time.sleep(0.2)
        broker.proc.send_signal(signal.SIGCONT)
        cmd, _ = late.read_packet(5)
        check(cmd & 0xF0 == 0x90, "SUBACK comes first")
        got = payloads(late.read_publishes(), "t/p")
        check(len(got) == 1, "a SUBSCRIBE covers %d byte data read with it (got %d)" % (size, len(got)))
        data.close()
        late.close()
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
        # The PUBACK and DISCONNECT may arrive in separate reads; the broker then closes.
        rest = b""
        try:
            while b"\xe0" not in rest:
                chunk = pub.sock.recv(64)
                if not chunk:
                    break
                rest += chunk
        except OSError:
            pass
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
    op_request_survives_full_queue_case_with_buffer()
    op_request_survives_full_queue_case_without_buffer()
    op_outlives_deadline_case("DELETE")
    op_outlives_deadline_case("RESTRICT", "qa", new_mp="qb")
    scoped_op_case("DELETE", "t/+/1", "qb", {"qa": True, "qb": False, "qa|qc": True, "qb|qc": False})
    scoped_op_case("DELETE", "t/s/#", "qx", {"qa": True, "qb": True})
    scoped_op_case("DELETE", "t/other", None, {"qa": True})
    print("# RESTRICT on queued messages")
    # DAP-OpPFs picks the copies; DAP-OpParam is what they're checked against.
    scoped_op_case("RESTRICT", "t/s/1", "qa", {"qa": True, "qb": True, "qb|qc": True, "qa|qb": False}, new_mp="qa")
    scoped_op_case("RESTRICT", "t/s/1", "qa", {"qa": False, "qb": True}, new_mp="qb")
    scoped_op_case("RESTRICT", "t/s/1", None, {"qa": False, "qb|qc": True}, new_mp="qb|qc")
    scoped_op_case("RESTRICT", "t/other", None, {"qa": True}, new_mp="qb")
    restrict_needs_new_mp_case()
    restrict_spares_later_data_case()
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
    mp_change_case("qz", still_allowed=False, conf="mount_point m/\n", label=" behind a mount point")
    mp_change_case("qz", still_allowed=False, conf="dap_send_verification false\n", label=" with send verification off")
    mp_widen_case()
    print("# stats")
    stats_case(["qa", "qz"], 0)
    stats_case(["qb"], 1)
    stats_mp_case()
    stats_op_drop_case(0)
    stats_op_drop_case(2)
    metrics_row_case(1)
    metrics_row_case(2)
    metrics_qos2_timing_case()
    print("# prioritized intake")
    intake_mp_case()
    intake_delete_case()
    intake_other_client_case()
    intake_other_client_case(200000)
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
