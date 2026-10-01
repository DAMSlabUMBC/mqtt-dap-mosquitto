#!/usr/bin/env python3
"""DAP pending operations, flows, subscription SPs and held requests survive a broker
crash + restart (persist-sqlite), and operations are reclaimed once their deadline passes.

MPs are not persisted, so publishers re-register them after the restart.

Usage: python3 test/dap/op_persistence_test.py [broker] [plugin.so] [port]
"""
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time

import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
BROKER = sys.argv[1] if len(sys.argv) > 1 else os.path.join(ROOT, "src", "mosquitto")
PLUGIN = sys.argv[2] if len(sys.argv) > 2 else os.path.join(ROOT, "plugins", "persist-sqlite", "mosquitto_persist_sqlite.so")
PORT = int(sys.argv[3]) if len(sys.argv) > 3 else 18831
HOST = "127.0.0.1"

OSYS = "$OP_SYS"
OP_REQ = "OP_REQ"
OP_NOTIF = "OP_NOTIF"
OP_PURPOSE = "DAP_OP"
MP = "quality/assurance"

failures = []


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
    def __init__(self, workdir):
        self.workdir = workdir
        self.conf = os.path.join(workdir, "mosquitto.conf")
        with open(self.conf, "w") as f:
            f.write(f"listener {PORT} {HOST}\n"
                    "allow_anonymous true\n"
                    "use_metadata_operation_support true\n"
                    f"persistence_location {workdir}\n"
                    f"plugin {PLUGIN}\n"
                    "plugin_opt_flush_period 1\n")
        self.proc = None
        self.log_path = None

    def start(self, tag):
        self.log_path = os.path.join(self.workdir, f"broker-{tag}.log")
        self.log = open(self.log_path, "w")
        self.proc = subprocess.Popen([BROKER, "-c", self.conf, "-v"], stdout=self.log, stderr=subprocess.STDOUT)
        ok = wait_for(lambda: "running as user" in self.read_log() or "Opening ipv4 listen socket" in self.read_log())
        time.sleep(0.2)
        if not ok or self.proc.poll() is not None:
            print(self.read_log())
            raise SystemExit("broker failed to start")

    def read_log(self):
        with open(self.log_path) as f:
            return f.read()

    def kill(self):
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait()
        self.log.close()

    def stop(self):
        self.proc.send_signal(signal.SIGTERM)
        rc = self.proc.wait(timeout=10)
        self.log.close()
        return rc


def user_props(msg):
    props = getattr(msg.properties, "UserProperty", None) or []
    return dict(props)


class Client:

    def __init__(self, client_id):
        self.id = client_id
        self.msgs = []
        self.c = mqtt.Client(CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt.MQTTv5)
        self.c.on_message = lambda c, u, m: self.msgs.append((m.topic, m.payload, user_props(m)))

    def connect(self):
        props = Properties(PacketTypes.CONNECT)
        props.SessionExpiryInterval = 3600
        self.c.connect(HOST, PORT, clean_start=False, properties=props)
        self.c.loop_start()
        return self

    def disconnect(self):
        self.c.disconnect()
        self.c.loop_stop()

    def subscribe(self, topic, sp, qos=1):
        props = Properties(PacketTypes.SUBSCRIBE)
        if sp is not None:
            props.UserProperty = [("DAP-SP", sp)]
        self.c.subscribe(topic, qos=qos, properties=props)

    def publish(self, topic, pairs, payload=b"", qos=1, retain=False):
        props = Properties(PacketTypes.PUBLISH)
        props.UserProperty = [("DAP-Allow", "1")] + pairs
        self.c.publish(topic, payload=payload, qos=qos, retain=retain, properties=props).wait_for_publish(5)

    def got(self, topic_prefix, **match):
        return [m for m in self.msgs
                if m[0].startswith(topic_prefix) and all(m[2].get(k) == v for k, v in match.items())]


def data(client, topic, payload):
    client.publish(topic, [], payload=payload)


def held_request_case():
    """A request held for an offline subscriber survives a restart (paper 6.3)."""
    workdir = tempfile.mkdtemp(prefix="dap-persist-held-")
    broker = Broker(workdir)
    broker.start("held-1")
    subW = Client("subW").connect()
    subW.subscribe("sensors/temp", MP)
    subW.subscribe(f"{OP_REQ}/subW", OP_PURPOSE)
    pub2 = Client("pub2").connect()
    pub2.subscribe(f"{OP_NOTIF}/pub2", OP_PURPOSE)
    time.sleep(0.3)
    pub2.publish("$MP_REG", [("DAP-MP", f"{MP}:sensors/temp")], qos=0)
    time.sleep(0.2)
    data(pub2, "sensors/temp", b"temp")
    check(wait_for(lambda: subW.got("sensors/temp")), "subW receives data before going offline")
    subW.disconnect()
    time.sleep(0.2)
    pub2.publish(OSYS, [("DAP-OpType", "HISTORY"), ("DAP-OpTFs", "sensors/temp")])
    check(wait_for(lambda: pub2.got(OP_NOTIF, **{"DAP-Status": "Pending"})), "the HISTORY is accepted")
    time.sleep(2.5)  # wait for the plugin flush
    pub2.c.loop_stop()
    broker.kill()

    broker.start("held-2")
    check("Restored 1 DAP requests (0 failed)" in broker.read_log(), "broker restores the held request")
    subW = Client("subW").connect()
    check(wait_for(lambda: subW.got(OP_REQ, **{"DAP-OpType": "HISTORY"})),
          "the held request is delivered when subW's session resumes after the restart")
    subW.disconnect()
    rc = broker.stop()
    check(rc == 0, f"broker exits cleanly (rc={rc})")


def main():
    workdir = tempfile.mkdtemp(prefix="dap-persist-")
    broker = Broker(workdir)

    # ---- Before the restart -------------------------------------------------
    broker.start("1")

    subX = Client("subX").connect()
    subY = Client("subY").connect()
    subZ = Client("subZ").connect()
    pub1 = Client("pub1").connect()
    for s in (subX, subY, subZ):
        s.subscribe("sensors/temp", MP)
        s.subscribe("sensors/humidity", MP)
        s.subscribe(f"{OP_REQ}/{s.id}", OP_PURPOSE)
    pub1.subscribe(f"{OP_NOTIF}/pub1", OP_PURPOSE)
    pub1.subscribe(OSYS, None)  # an operation topic needs no SP; its row has none
    time.sleep(0.3)
    subZ.disconnect()  # offline, so messages queue for it

    pub1.publish("$MP_REG", [("DAP-MP", f"{MP}:sensors/temp")], qos=0)
    pub1.publish("$MP_REG", [("DAP-MP", f"{MP}:sensors/humidity")], qos=0)
    pub1.publish("$MP_REG", [("DAP-MP", f"{MP}:sensors/retained")], qos=0)
    time.sleep(0.2)
    pub1.publish("sensors/retained", [], payload=b"kept", retain=True)
    data(pub1, "sensors/temp", b"temp-before-delete")
    data(pub1, "sensors/humidity", b"humidity-before-delete")
    check(wait_for(lambda: len(subX.got("sensors/")) == 2 and len(subY.got("sensors/")) == 2),
          "online subscribers receive the data before the operation")

    pub1.publish(OSYS, [("DAP-OpType", "DELETE"), ("DAP-OpTFs", "sensors/temp")])
    check(wait_for(lambda: pub1.got(OP_NOTIF, **{"DAP-Status": "Pending"})), "requester gets a Pending ack")
    pending = pub1.got(OP_NOTIF, **{"DAP-Status": "Pending"})
    op_id = pending[0][2].get("DAP-OpId") if pending else None
    check(op_id is not None, f"Pending ack carries an op id ({op_id})")
    check(wait_for(lambda: subX.got(OP_REQ) and subY.got(OP_REQ)), "relevant subscribers get the request")

    subX.publish(OSYS, [("DAP-Status", "Success"), ("DAP-OpId", op_id)])
    check(wait_for(lambda: pub1.got(OP_NOTIF, **{"DAP-Status": "Success", "DAP-ClientID": "subX"})),
          "subX's response is relayed to the requester")

    time.sleep(2.5)  # wait for the plugin flush
    for c in (subX, subY, pub1):
        c.c.loop_stop()
    broker.kill()

    db = sqlite3.connect(os.path.join(workdir, "mosquitto.sqlite3"))
    ops = db.execute("SELECT op_id, publisher_id, op_type, topic_filters FROM dap_ops").fetchall()
    check(ops == [(int(op_id), "pub1", 0, "sensors/temp")], f"dap_ops holds the DELETE: {ops}")
    tracked = db.execute("SELECT op_id, publisher_id, settled FROM dap_tracked_ops").fetchall()
    check(tracked == [(int(op_id), "pub1", 0)], f"dap_tracked_ops holds the unsettled op: {tracked}")
    subs = sorted(db.execute("SELECT sub_id, responded FROM dap_tracked_op_subs").fetchall())
    check(subs == [("subX", 1), ("subY", 0)], f"dap_tracked_op_subs records who responded: {subs}")
    db.close()

    # ---- After the restart --------------------------------------------------
    broker.start("2")
    log = broker.read_log()
    check("Restored 1 DAP pending operations (0 failed)" in log, "broker restores the pending operation")
    check("Restored 1 DAP tracked operations (0 failed)" in log, "broker restores the tracked operation")

    subZ = Client("subZ").connect()
    check(wait_for(lambda: subZ.got("sensors/humidity")), "queued message not covered by the op is still delivered")
    time.sleep(0.5)
    check(not subZ.got("sensors/temp"), "queued message covered by the restored DELETE is dropped")

    # Flows survive the restart (paper 6.1): relevance needs no new delivery.
    pub1 = Client("pub1").connect()
    pub1.subscribe(f"{OP_NOTIF}/pub1", OP_PURPOSE)
    time.sleep(0.3)
    pub1.publish(OSYS, [("DAP-OpType", "AUDIT"), ("DAP-OpTFs", "sensors/temp"), ("DAP-OpPFs", MP)])
    check(wait_for(lambda: pub1.got(OP_NOTIF, **{"DAP-OpType": "AUDIT", "DAP-Status": "Success"})),
          "an AUDIT after the restart gets an answer")
    audit = pub1.got(OP_NOTIF, **{"DAP-OpType": "AUDIT", "DAP-Status": "Success"})
    check(bool(audit) and sorted(audit[0][1].split(b",")) == [b"subX", b"subY"],
          f"it lists the subscribers that received data before the restart: {audit[0][1] if audit else None}")

    # A retained message keeps its MP across the restart.
    subR = Client("subR").connect()
    subR.subscribe("sensors/retained", MP)
    check(wait_for(lambda: subR.got("sensors/retained")),
          "a retained message from before the restart reaches a subscription its MP permits")
    subR.disconnect()

    # SPs survive with the session; MPs are re-registered by their publishers.
    pub1.publish("$MP_REG", [("DAP-MP", f"{MP}:sensors/humidity")], qos=0)
    subY = Client("subY").connect()
    time.sleep(0.3)
    data(pub1, "sensors/humidity", b"humidity-for-restored-sessions")
    check(wait_for(lambda: subY.got("sensors/humidity")),
          "a restored session receives new data without subscribing again")
    subY.publish(OSYS, [("DAP-Status", "Success"), ("DAP-OpId", op_id)])
    check(wait_for(lambda: pub1.got(OP_NOTIF, **{"DAP-Status": "Success", "DAP-ClientID": "subY"})),
          "subY's response after the restart is relayed to the requester")
    check(wait_for(lambda: pub1.got(OP_NOTIF, **{"DAP-Status": "Success", "DAP-OpId": op_id,
                                                  "DAP-Reason": "All subscribers responded"})),
          "the operation settles using the response restored from before the restart")

    # New op ids continue after restored ones. Delivery records aren't persisted,
    # so create one first.
    subX = Client("subX").connect()
    subX.subscribe("sensors/humidity", MP)
    time.sleep(0.3)
    data(pub1, "sensors/humidity", b"humidity-after-restart")
    wait_for(lambda: subX.got("sensors/humidity"))
    pub1.publish(OSYS, [("DAP-OpType", "RESTRICT"), ("DAP-OpTFs", "sensors/humidity")])
    check(wait_for(lambda: len(pub1.got(OP_NOTIF, **{"DAP-Status": "Pending"})) >= 1),
          "a new operation is accepted after the restart")
    new_pending = pub1.got(OP_NOTIF, **{"DAP-Status": "Pending"})
    new_id = new_pending[-1][2].get("DAP-OpId") if new_pending else None
    check(new_id is not None and int(new_id) > int(op_id),
          f"the new operation gets a fresh op id ({new_id} > {op_id})")

    # An operation is reclaimed once its deadline passes; late responses still reach the requester.
    pub1.publish(OSYS, [("DAP-OpType", "DELETE"), ("DAP-OpTFs", "sensors/humidity"),
                        ("DAP-Deadline", str(int(time.time()) + 2))])
    check(wait_for(lambda: len(pub1.got(OP_NOTIF, **{"DAP-Status": "Pending"})) >= 2),
          "a DELETE with a short deadline is accepted")
    short_id = pub1.got(OP_NOTIF, **{"DAP-Status": "Pending"})[-1][2].get("DAP-OpId")
    check(wait_for(lambda: pub1.got(OP_NOTIF, **{"DAP-OpId": short_id, "DAP-Reason": "Operation deadline expired"}),
                   timeout=10), "the DELETE expires at its deadline")
    time.sleep(2.5)  # wait for the plugin flush
    subX.publish(OSYS, [("DAP-Status", "Success"), ("DAP-OpId", short_id)])
    check(wait_for(lambda: pub1.got(OP_NOTIF, **{"DAP-Status": "Success", "DAP-OpId": short_id,
                                                  "DAP-ClientID": "subX"})),
          "a response after the deadline is still relayed to the requester")

    for c in (subX, subY, subZ, pub1):
        c.disconnect()
    time.sleep(0.2)
    rc = broker.stop()
    check(rc == 0, f"broker exits cleanly (rc={rc})")

    db = sqlite3.connect(os.path.join(workdir, "mosquitto.sqlite3"))
    ops = sorted(r[0] for r in db.execute("SELECT op_id FROM dap_ops").fetchall())
    check(int(short_id) not in ops and int(new_id) in ops,
          f"the expired DELETE is reclaimed from disk, the live RESTRICT is kept: {ops}")
    settled = db.execute("SELECT settled FROM dap_tracked_ops WHERE op_id=?", (int(op_id),)).fetchone()
    check(settled == (1,), f"the settled operation is marked settled on disk: {settled}")
    left = db.execute("SELECT COUNT(*) FROM dap_tracked_op_subs WHERE op_id=?", (int(op_id),)).fetchone()
    check(left == (0,), "its per-subscriber rows are cleared")
    db.close()

    held_request_case()

    if failures:
        print(f"\nOP PERSISTENCE TEST FAILED ({len(failures)} checks); broker logs in {workdir}")
        return 1
    print("\nOP PERSISTENCE TEST PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
