#!/usr/bin/env python3
"""Operation Failure/Success responses must not crash or leak the broker.

Covers an unknown DAP-OpType, a DELETE with no relevant subscribers, HISTORY
with offline and online subscribers, AUDIT, requests held for offline
subscribers, and how relevance is decided. Each case runs against a fresh broker,
which must exit cleanly (under make WITH_ASAN=yes a leak fails that check).

Usage: python3 test/dap/op_response_paths_test.py [port]
"""
import json
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
        self.c.on_message = lambda c, u, m: self.msgs.append((m.topic, m.payload, user_props(m), m.qos))
        clients.append(self)

    def connect(self, clean_start=False):
        props = Properties(PacketTypes.CONNECT)
        props.SessionExpiryInterval = 3600
        self.c.connect(HOST, PORT, clean_start=clean_start, properties=props)
        self.c.loop_start()
        return self

    def disconnect(self):
        self.c.disconnect()
        self.c.loop_stop()

    def subscribe(self, topic, sp):
        props = Properties(PacketTypes.SUBSCRIBE)
        props.UserProperty = [("DAP-SP", sp)]
        self.c.subscribe(topic, qos=1, properties=props)

    def publish(self, topic, pairs, payload=b"", qos=1, response_topic=None, retain=False):
        props = Properties(PacketTypes.PUBLISH)
        props.UserProperty = [("DAP-Allow", "1")] + pairs
        if response_topic:
            props.ResponseTopic = response_topic
        self.c.publish(topic, payload=payload, qos=qos, retain=retain, properties=props).wait_for_publish(5)

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
    return wait_for(lambda: pub.got(OP_NOTIF, **{"DAP-Status": "Pending"}))


def case_history_many_offline(pub):
    # Enough long ids to make DAP-UnreachedClients longer than 256 bytes.
    ids = ["offline-subscriber-with-a-long-client-id-%02d" % i for i in range(12)]
    subs = subscribers_with_data(pub, ids)
    for sub in subs:
        sub.disconnect()
    time.sleep(0.2)
    deadline = int(time.time()) + 3
    pub.publish(OSYS, [("DAP-OpType", "HISTORY"), ("DAP-OpTFs", TOPIC), ("DAP-Deadline", str(deadline))])
    if not wait_for(lambda: pub.got(OP_NOTIF, **{"DAP-Status": "Pending", "DAP-Deadline": str(deadline)})):
        return False
    # Unreached subscribers are reported when the requested deadline passes.
    expired = {"DAP-Status": "Failure", "DAP-Reason": "Operation deadline expired"}
    if not wait_for(lambda: pub.got(OP_NOTIF, **expired), timeout=10):
        return False
    unreached = pub.got(OP_NOTIF, **expired)[0][2].get("DAP-UnreachedClients", "")
    return sorted(unreached.split()) == sorted(ids)


def case_history_success(pub):
    sub = subscribers_with_data(pub, ["subA"])[0]
    pub.publish(OSYS, [("DAP-OpType", "HISTORY"), ("DAP-OpTFs", TOPIC)])
    if not wait_for(lambda: sub.got(f"{OP_REQ}/subA") and pub.got(OP_NOTIF, **{"DAP-Status": "Pending"})):
        return False
    op_id = sub.got(f"{OP_REQ}/subA")[0][2].get("DAP-OpId")
    sub.publish(OSYS, [("DAP-Status", "Success"), ("DAP-OpId", op_id)], payload=b"history")
    return wait_for(lambda: pub.got(OP_NOTIF, **{"DAP-Status": "Success", "DAP-ClientID": "subA"})
                    and pub.got(OP_NOTIF, **{"DAP-Status": "Success", "DAP-Reason": "All subscribers responded"}))


def case_audit_lists_subscribers(pub):
    subscribers_with_data(pub, ["subA", "subB"])
    pub.publish(OSYS, [("DAP-OpType", "AUDIT"), ("DAP-OpTFs", TOPIC)])
    if not wait_for(lambda: pub.got(OP_NOTIF, **{"DAP-Status": "Success"})):
        return False
    return sorted(pub.got(OP_NOTIF, **{"DAP-Status": "Success"})[0][1].split(b",")) == [b"subA", b"subB"]


def case_audit_no_relevant(pub):
    subscribers_with_data(pub, ["subA"])
    pub.publish(OSYS, [("DAP-OpType", "AUDIT"), ("DAP-OpTFs", "other/topic")])
    return wait_for(lambda: pub.got(OP_NOTIF, **{"DAP-Status": "Failure",
                                                 "DAP-Reason": "No relevant subscribers found"}))


def case_relevance_uses_delivery_time_sp(pub):
    # subA receives data under one purpose, then changes its SP to another.
    pub.publish("$MP_REG", [("DAP-MP", f"{MP}|operations/forecast:{TOPIC}")], qos=0)
    time.sleep(0.2)
    sub = subscribers_with_data(pub, ["subA"])[0]
    sub.subscribe(TOPIC, "operations/forecast")
    time.sleep(0.2)
    pub.publish(OSYS, [("DAP-OpType", "DELETE"), ("DAP-OpTFs", TOPIC), ("DAP-OpPFs", MP)])
    old_purpose = wait_for(lambda: sub.got(f"{OP_REQ}/subA"))
    pub.publish(OSYS, [("DAP-OpType", "DELETE"), ("DAP-OpTFs", TOPIC), ("DAP-OpPFs", "operations/forecast")])
    unused_purpose = wait_for(lambda: pub.got(OP_NOTIF, **{"DAP-Status": "Failure",
                                                          "DAP-Reason": "No relevant subscribers found"}))
    return old_purpose and unused_purpose


def case_relevance_topic_wildcard(pub):
    sub = subscribers_with_data(pub, ["subA"])[0]
    pub.publish(OSYS, [("DAP-OpType", "DELETE"), ("DAP-OpTFs", TOPIC.split("/")[0] + "/#")])
    return wait_for(lambda: sub.got(f"{OP_REQ}/subA"))


def offline_request_case(pub, resume_session):
    sub = subscribers_with_data(pub, ["subA"])[0]
    sub.disconnect()
    time.sleep(0.2)
    pub.publish(OSYS, [("DAP-OpType", "DELETE"), ("DAP-OpTFs", TOPIC)])
    if not wait_for(lambda: pub.got(OP_NOTIF, **{"DAP-Status": "Pending"})):
        return False
    sub = Client("subA").connect(clean_start=not resume_session)
    if not resume_session:
        # A new session subscribes to its request topic again, as PSMark does.
        sub.subscribe(f"{OP_REQ}/subA", OP_PURPOSE)
    if not wait_for(lambda: sub.got(f"{OP_REQ}/subA", **{"DAP-OpType": "DELETE"})):
        return False
    op_id = sub.got(f"{OP_REQ}/subA")[0][2].get("DAP-OpId")
    sub.publish(OSYS, [("DAP-Status", "Success"), ("DAP-OpId", op_id)])
    return wait_for(lambda: pub.got(OP_NOTIF, **{"DAP-Status": "Success", "DAP-Reason": "All subscribers responded"}))


def case_request_held_for_inbox_without_op_purpose(pub):
    # An inbox whose SP the DAP_OP purpose does not permit cannot receive the request,
    # so it is held until the subscriber subscribes with one it does.
    sub = subscribers_with_data(pub, ["subA"])[0]
    sub.subscribe(f"{OP_REQ}/subA", f"{OP_PURPOSE}|analytics")
    time.sleep(0.2)
    pub.publish(OSYS, [("DAP-OpType", "DELETE"), ("DAP-OpTFs", TOPIC)])
    if not wait_for(lambda: pub.got(OP_NOTIF, **{"DAP-Status": "Pending"})):
        return False
    time.sleep(0.3)
    if sub.got(f"{OP_REQ}/subA"):
        return False
    sub.subscribe(f"{OP_REQ}/subA", OP_PURPOSE)
    return wait_for(lambda: sub.got(f"{OP_REQ}/subA", **{"DAP-OpType": "DELETE"}))


def case_offline_request_on_resubscribe(pub):
    return offline_request_case(pub, resume_session=False)


def case_offline_request_on_session_resume(pub):
    return offline_request_case(pub, resume_session=True)


def case_client_defined_operation(pub):
    # O:<name> operations reuse the subscriber workflow (paper 6.2).
    sub = subscribers_with_data(pub, ["subA"])[0]
    pub.publish(OSYS, [("DAP-OpType", "O:CONSENT-WITHDRAW"), ("DAP-OpTFs", TOPIC)], payload=b"why")
    if not wait_for(lambda: sub.got(f"{OP_REQ}/subA", **{"DAP-OpType": "O:CONSENT-WITHDRAW"})
                    and pub.got(OP_NOTIF, **{"DAP-Status": "Pending"})):
        return False
    request = sub.got(f"{OP_REQ}/subA")[0]
    sub.publish(OSYS, [("DAP-Status", "Success"), ("DAP-OpId", request[2].get("DAP-OpId"))])
    return request[1] == b"why" and wait_for(
        lambda: pub.got(OP_NOTIF, **{"DAP-Status": "Success", "DAP-Reason": "All subscribers responded"}))


def case_client_defined_operation_needs_a_name(pub):
    subscribers_with_data(pub, ["subA"])
    pub.publish(OSYS, [("DAP-OpType", "O:"), ("DAP-OpTFs", TOPIC)])
    return wait_for(lambda: pub.got(OP_NOTIF, **{"DAP-Status": "Failure", "DAP-Reason": "Unknown Operation"}))


def case_status_request(pub):
    # The requester can ask for every relevant subscriber's status at any time (paper 6.3).
    subs = {s.id: s for s in subscribers_with_data(pub, ["subA", "subB", "subC"])}
    pub.publish(OSYS, [("DAP-OpType", "DELETE"), ("DAP-OpTFs", TOPIC)])
    if not wait_for(lambda: all(s.got(f"{OP_REQ}/{i}") for i, s in subs.items())):
        return False
    op_id = subs["subA"].got(f"{OP_REQ}/subA")[0][2].get("DAP-OpId")
    subs["subA"].publish(OSYS, [("DAP-Status", "Success"), ("DAP-OpId", op_id)])
    subs["subB"].publish(OSYS, [("DAP-Status", "Failure"), ("DAP-OpId", op_id), ("DAP-Reason", "retention obligation")])
    subs["subC"].publish(OSYS, [("DAP-Status", "Pending"), ("DAP-OpId", op_id)])
    time.sleep(0.3)

    def status():
        before = len(pub.got(OP_NOTIF, **{"DAP-OpType": "STATUS"}))
        pub.publish(OSYS, [("DAP-OpType", "STATUS"), ("DAP-OpId", op_id)])
        if not wait_for(lambda: len(pub.got(OP_NOTIF, **{"DAP-OpType": "STATUS"})) > before):
            return None
        reply = pub.got(OP_NOTIF, **{"DAP-OpType": "STATUS"})[-1]
        if reply[2].get("DAP-Status") != "Success" or reply[2].get("DAP-OpId") != op_id:
            return None
        return {s["id"]: s for s in json.loads(reply[1])["subscribers"]}

    summary = status()
    if not summary or summary["subA"]["status"] != "Success" or summary["subB"] != {
            "id": "subB", "status": "Failure", "reason": "retention obligation"}:
        return False
    if summary["subC"]["status"] != "Pending" or not 0 < summary["subC"]["remaining"] <= 30:
        return False

    # Only the requester may ask.
    subs["subA"].subscribe(f"{OP_NOTIF}/subA", OP_PURPOSE)
    time.sleep(0.2)
    subs["subA"].publish(OSYS, [("DAP-OpType", "STATUS"), ("DAP-OpId", op_id)])
    if not wait_for(lambda: subs["subA"].got(OP_NOTIF, **{"DAP-Status": "Failure", "DAP-Reason": "Unknown operation"})):
        return False

    # A settled operation can still be asked about until its deadline.
    subs["subC"].publish(OSYS, [("DAP-Status", "Success"), ("DAP-OpId", op_id)])
    if not wait_for(lambda: pub.got(OP_NOTIF, **{"DAP-Reason": "All subscribers responded"})):
        return False
    summary = status()
    return bool(summary) and summary["subC"]["status"] == "Success"


def case_delete_scopes_retained(pub):
    # A DELETE removes the requester's retained messages it covers, and only those.
    pub.publish("$MP_REG", [("DAP-MP", f"{MP}:sensors/other")], qos=0)
    time.sleep(0.2)
    subscribers_with_data(pub, ["subA"])
    pub.publish(TOPIC, [], payload=b"kept-a", retain=True)
    pub.publish("sensors/other", [], payload=b"kept-b", retain=True)
    pub.publish(OSYS, [("DAP-OpType", "DELETE"), ("DAP-OpTFs", TOPIC)])
    if not wait_for(lambda: pub.got(OP_NOTIF, **{"DAP-Status": "Pending"})):
        return False
    time.sleep(0.3)
    late = Client("subLate").connect()
    late.subscribe(TOPIC, MP)
    late.subscribe("sensors/other", MP)
    time.sleep(0.5)
    return [m[1] for m in late.got("sensors/")] == [b"kept-b"]


def case_deadline_in_the_past(pub):
    subscribers_with_data(pub, ["subA"])
    pub.publish(OSYS, [("DAP-OpType", "DELETE"), ("DAP-OpTFs", TOPIC), ("DAP-Deadline", str(int(time.time()) - 5))])
    return wait_for(lambda: pub.got(OP_NOTIF, **{"DAP-Status": "Failure", "DAP-Reason": "Deadline has passed"}))


def case_operation_traffic_is_qos1(pub):
    # Requests and notifications are sent at QoS 1 to inboxes subscribed at QoS 1 (paper 6.2).
    sub = subscribers_with_data(pub, ["subA"])[0]
    pub.publish(OSYS, [("DAP-OpType", "DELETE"), ("DAP-OpTFs", TOPIC)], qos=0)
    if not wait_for(lambda: sub.got(f"{OP_REQ}/subA") and pub.got(OP_NOTIF, **{"DAP-Status": "Pending"})):
        return False
    op_id = sub.got(f"{OP_REQ}/subA")[0][2].get("DAP-OpId")
    sub.publish(OSYS, [("DAP-Status", "Success"), ("DAP-OpId", op_id)], qos=0)
    if not wait_for(lambda: pub.got(OP_NOTIF, **{"DAP-Reason": "All subscribers responded"})):
        return False
    return all(m[3] == 1 for m in sub.got(OP_REQ) + pub.got(OP_NOTIF))


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
    ("HISTORY with an offline subscriber gets a Pending ack", case_history_subscriber_offline, True),
    ("HISTORY with many offline subscribers lists every one at the requested deadline", case_history_many_offline, True),
    ("a deadline that has already passed gets a Failure", case_deadline_in_the_past, True),
    ("a DELETE removes only the retained messages it covers", case_delete_scopes_retained, True),
    ("an O: operation runs the subscriber workflow", case_client_defined_operation, True),
    ("the requester can ask for an operation's status", case_status_request, True),
    ("an O: operation without a name is unknown", case_client_defined_operation_needs_a_name, True),
    ("HISTORY gets a Success once its subscriber responds", case_history_success, True),
    ("AUDIT returns the relevant subscriber ids", case_audit_lists_subscribers, True),
    ("an offline subscriber gets the request when it subscribes again", case_offline_request_on_resubscribe, True),
    ("an offline subscriber gets the request when its session resumes", case_offline_request_on_session_resume, True),
    ("a request waits for an inbox whose SP admits it", case_request_held_for_inbox_without_op_purpose, True),
    ("requests and notifications use QoS 1", case_operation_traffic_is_qos1, True),
    ("AUDIT with no relevant subscribers gets a Failure", case_audit_no_relevant, True),
    ("relevance uses the SP in force at delivery", case_relevance_uses_delivery_time_sp, True),
    ("relevance matches DAP-OpTFs as MQTT topic filters", case_relevance_topic_wildcard, True),
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
