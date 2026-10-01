#!/usr/bin/env python3
"""Send-time verification and op/PBMR priority on the broker's outgoing path.

Covers DELETE on queued messages (including after a subscriber's queue
overflowed), re-verification of messages whose stamp went stale (MP or SP
change), and op requests overtaking data backed up for a slow subscriber.

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

    def operation(self, op, topic_filter):
        self.publish(OSYS, [("DAP-OpType", op), ("DAP-OpTFs", topic_filter)])

    def got_status(self, status):
        return [m for m in self.msgs if m[1].get("DAP-Status") == status]

    def stop(self):
        self.c.loop_stop()
        self.c.disconnect()


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
        time.sleep(1.1)  # the DELETE must be received after m3 (1 s resolution)
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
        time.sleep(1.1)  # the DELETE covers m1 only
        pub.operation("DELETE", "t/e")
        check(wait_for(lambda: pub.got_status("Pending")), "requester gets a Pending ack for the DELETE")
        time.sleep(1.1)  # m2-m4 are received after the DELETE
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
        time.sleep(1.1)  # the DELETE covers m1 and m2, not m3
        pub.operation("DELETE", "t/f")
        check(wait_for(lambda: pub.got_status("Pending")), "requester gets a Pending ack for the DELETE")
        time.sleep(1.1)  # m3 is received after the DELETE
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
        pub.register("qa", "t/c")
        pub.publish("t/c", [], payload=b"m1")
        pub.publish("t/c", [], payload=b"m2")
        first = sub.read_publishes(idle=0.5, ack=False)
        check(payloads(first, "t/c") == [b"m1"], "only m1 is in flight (receive maximum 1)")

        sub.subscribe("t/c", new_sps)  # SP version 2 while m2 waits
        sub.puback(first[0][2])
        pub.register(new_sps[0], "t/c")
        pub.publish("t/c", [], payload=b"m3")
        got = payloads(sub.read_publishes(), "t/c")
        expected = [b"m2", b"m3"] if still_allowed else [b"m3"]
        check(got == expected, "after the SP becomes %s, delivered %s (expected %s)" % (new_sps, got, expected))
        sub.close()
    finally:
        stop_clients()
        check(broker.stop() == 0, "broker exits cleanly")


def priority_case():
    """An op request overtakes data backed up in a slow subscriber's write queue."""
    broker = Broker("use_metadata_operation_support true\n")
    try:
        sub = Subscriber("subD", rcvbuf=4096)
        sub.subscribe("t/d", ["qa"], qos=0)
        sub.subscribe("OP_REQ/subD", [OP_PURPOSE], qos=0)
        pub = Publisher()
        pub.register("qa", "t/d")
        count = 600
        filler = b"x" * 32768
        for i in range(count):
            pub.publish("t/d", [], payload=b"%04d" % i + filler, qos=0)
        time.sleep(1.0)  # the subscriber is not reading, so the broker's queue for it backs up
        pub.operation("RESTRICT", "t/d")
        time.sleep(0.5)

        got = sub.read_publishes(idle=2.0)
        topics = [t for t, _, _ in got]
        seq = [int(p[:4]) for t, p, _ in got if t == "t/d"]
        check(seq == sorted(seq), "data keeps its order")
        check("OP_REQ/subD" in topics, "the operation request reaches the subscriber")
        if "OP_REQ/subD" in topics:
            after = len(topics) - 1 - topics.index("OP_REQ/subD")
            check(after > 0, "the operation request overtakes queued data (%d data messages after it)" % after)
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
    print("# re-verification of stale stamps")
    stale_mp_case()
    drop_then_stale_case()
    qos0_quota_case()
    sp_change_case(["qb"], still_allowed=False)
    sp_change_case(["qa", "qz"], still_allowed=True)
    print("# op/PBMR priority")
    priority_case()

    print()
    if failures:
        print("QUEUEING MODEL TEST FAILED: %d check(s)" % len(failures))
        sys.exit(1)
    print("QUEUEING MODEL TEST PASSED")

if __name__ == "__main__":
    main()
