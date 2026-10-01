#!/usr/bin/env python3
"""Operation Failure/Success responses must not crash or leak the broker.

Covers an unknown DAP-OpType, a DELETE with no relevant subscribers, and HISTORY
with offline and online subscribers. Each case runs against a fresh broker,
which must exit cleanly (under make WITH_ASAN=yes a leak fails that check).

Usage: python3 test/dap/op_response_paths_test.py [port]
"""
import os
import subprocess
import sys
import tempfile
import time

import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

ROOT = os.environ.get("BUILD_ROOT") or os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
BROKER = os.path.join(ROOT, "src", "mosquitto")
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 18833
HOST = "127.0.0.1"

OSYS = "$OP_SYS"
OP_REQ = "OP_REQ"
OP_NOTIF = "OP_NOTIF"
OP_PURPOSE = "DAP_OP"
MP = "quality/assurance"
TOPIC = "sensors/temp"

failures = []
clients = []  # every Client of the current case, stopped before the next broker starts


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
    def __init__(self):
        self.workdir = tempfile.mkdtemp(prefix="dap-op-resp-")
        self.conf = os.path.join(self.workdir, "mosquitto.conf")
        with open(self.conf, "w") as f:
            f.write(f"listener {PORT} {HOST}\n"
                    "allow_anonymous true\n"
                    "use_metadata_operation_support true\n")
        self.log_path = os.path.join(self.workdir, "broker.log")
        self.log = open(self.log_path, "w")
        self.proc = subprocess.Popen([BROKER, "-c", self.conf, "-v"], stdout=self.log, stderr=subprocess.STDOUT)
        if not wait_for(lambda: "Opening ipv4 listen socket" in self.read_log()) or self.proc.poll() is not None:
            print(self.read_log())
            raise SystemExit("broker failed to start")
        time.sleep(0.1)

    def read_log(self):
        with open(self.log_path, errors="replace") as f:
            return f.read()

    def alive(self):
        c = mqtt.Client(CallbackAPIVersion.VERSION2, client_id="liveness-probe", protocol=mqtt.MQTTv5)
        try:
            c.connect(HOST, PORT)
            c.disconnect()
            return self.proc.poll() is None
        except OSError:
            return False

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


def user_props(msg):
    return dict(getattr(msg.properties, "UserProperty", None) or [])


class Client:
    def __init__(self, client_id):
        self.id = client_id
        self.msgs = []
        self.c = mqtt.Client(CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt.MQTTv5)
        self.c.on_message = lambda c, u, m: self.msgs.append((m.topic, m.payload, user_props(m)))
        clients.append(self)

    def connect(self):
        props = Properties(PacketTypes.CONNECT)
        props.SessionExpiryInterval = 3600
        self.c.connect(HOST, PORT, clean_start=False, properties=props)
        self.c.loop_start()
        return self

    def disconnect(self):
        self.c.disconnect()
        self.c.loop_stop()

    def subscribe(self, topic, sp):
        props = Properties(PacketTypes.SUBSCRIBE)
        props.UserProperty = [("DAP-SP", sp)]
        self.c.subscribe(topic, qos=1, properties=props)

    def publish(self, topic, pairs, payload=b"", qos=1, response_topic=None):
        props = Properties(PacketTypes.PUBLISH)
        props.UserProperty = [("DAP-Allow", "1")] + pairs
        if response_topic:
            props.ResponseTopic = response_topic
        self.c.publish(topic, payload=payload, qos=qos, properties=props).wait_for_publish(5)

    def got(self, topic_prefix, **match):
        return [m for m in self.msgs
                if m[0].startswith(topic_prefix) and all(m[2].get(k) == v for k, v in match.items())]


def requester(response_topic=None):
    pub = Client("pub1").connect()
    pub.subscribe(response_topic or f"{OP_NOTIF}/pub1", OP_PURPOSE)
    pub.publish("$MP_REG", [("DAP-MP", f"{MP}:{TOPIC}")], qos=0)
    time.sleep(0.2)
    return pub


def subscribers_with_data(pub, ids):
    """Subscribe each id to TOPIC, deliver one data message to all of them."""
    subs = [Client(i).connect() for i in ids]
    for s in subs:
        s.subscribe(TOPIC, MP)
        s.subscribe(f"{OP_REQ}/{s.id}", OP_PURPOSE)
    time.sleep(0.3)
    pub.publish(TOPIC, [], payload=b"data")
    check(wait_for(lambda: all(s.got(TOPIC) for s in subs)), "%d subscriber(s) receive the data" % len(subs))
    return subs


def case_unknown_op(pub):
    pub.publish(OSYS, [("DAP-OpType", "BOGUS")])
    return wait_for(lambda: pub.got(OP_NOTIF, **{"DAP-Status": "Failure", "DAP-Reason": "Unknown Operation"}))


def case_delete_no_relevant(pub):
    pub.publish(OSYS, [("DAP-OpType", "DELETE"), ("DAP-OpTFs", TOPIC)])
    return wait_for(lambda: pub.got(OP_NOTIF, **{"DAP-Status": "Failure",
                                                 "DAP-Reason": "No relevant subscribers found"}))


def case_history_subscriber_offline(pub):
    subs = subscribers_with_data(pub, ["subA"])
    subs[0].disconnect()
    time.sleep(0.2)
    pub.publish(OSYS, [("DAP-OpType", "HISTORY"), ("DAP-OpTFs", TOPIC)])
    return wait_for(lambda: pub.got(OP_NOTIF, **{"DAP-Status": "Failure", "DAP-Reason": "Subscriber not connected"}))


def case_history_many_offline(pub):
    # Enough long ids to make DAP-UnreachedClients longer than 256 bytes.
    ids = ["offline-subscriber-with-a-long-client-id-%02d" % i for i in range(12)]
    subs = subscribers_with_data(pub, ids)
    for s in subs:
        s.disconnect()
    time.sleep(0.3)
    pub.publish(OSYS, [("DAP-OpType", "HISTORY"), ("DAP-OpTFs", TOPIC)])
    if not wait_for(lambda: pub.got(OP_NOTIF, **{"DAP-Status": "Failure"})):
        return False
    unreached = pub.got(OP_NOTIF, **{"DAP-Status": "Failure"})[0][2].get("DAP-UnreachedClients", "")
    return all(i in unreached.split() for i in ids)


def case_history_success(pub):
    subscribers_with_data(pub, ["subA"])
    pub.publish(OSYS, [("DAP-OpType", "HISTORY"), ("DAP-OpTFs", TOPIC)])
    return wait_for(lambda: pub.got(OP_NOTIF, **{"DAP-Status": "Success"}))


def case_failure_to_response_topic(_):
    pub = requester(response_topic="op_resp/pub1")
    pub.publish(OSYS, [("DAP-OpType", "BOGUS")], response_topic="op_resp/pub1")
    return wait_for(lambda: pub.got("op_resp/pub1", **{"DAP-Status": "Failure"}))


def case_control_publish(_):
    # Takes an early return in handle__publish after the request struct is allocated.
    c = mqtt.Client(CallbackAPIVersion.VERSION2, client_id="control-pub", protocol=mqtt.MQTTv311)
    c.connect(HOST, PORT)
    c.loop_start()
    c.publish("$CONTROL/broker/v1", payload=b"{}", qos=1).wait_for_publish(5)
    time.sleep(0.2)
    c.loop_stop()
    c.disconnect()
    return True


CASES = [
    ("unrecognised DAP-OpType gets a Failure", case_unknown_op, True),
    ("DELETE with no relevant subscribers gets a Failure", case_delete_no_relevant, True),
    ("HISTORY with an offline subscriber gets a Failure", case_history_subscriber_offline, True),
    ("HISTORY with many offline subscribers lists every one", case_history_many_offline, True),
    ("HISTORY with all subscribers online gets a Success", case_history_success, True),
    ("Failure goes to the request's ResponseTopic", case_failure_to_response_topic, False),
    ("PUBLISH to $CONTROL frees the request", case_control_publish, False),
]


def main():
    for what, body, needs_requester in CASES:
        broker = Broker()
        try:
            pub = requester() if needs_requester else None
            ok = body(pub)
            alive = broker.alive()
        finally:
            for c in clients:
                c.c.loop_stop()
            clients.clear()
            rc = broker.stop()
        check(ok and alive and rc == 0, "%s (alive=%s, exit code %s)" % (what, alive, rc))
        if not (alive and rc == 0):
            print("    " + broker.read_log()[-2000:].replace("\n", "\n    "))

    print()
    if failures:
        print("OP RESPONSE PATHS TEST FAILED: %d check(s)" % len(failures))
        sys.exit(1)
    print("OP RESPONSE PATHS TEST PASSED")


if __name__ == "__main__":
    main()
