#!/usr/bin/env python3
"""A broker configured with recognized purposes rejects MPs and SPs that describe
any other purpose (paper 4.3).

Usage: python3 test/dap/recognized_purposes_test.py [port]
"""
import os
import signal
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
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 18836
HOST = "127.0.0.1"

failures = []


def check(cond, what):
    print(("ok   - " if cond else "FAIL - ") + what)
    if not cond:
        failures.append(what)


def props(packet_type, pairs):
    p = Properties(packet_type)
    p.UserProperty = pairs
    return p


class Client:
    def __init__(self, client_id):
        self.received = []
        self.acks = {}
        self.granted = {}
        self.disconnected = False
        self.c = mqtt.Client(CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt.MQTTv5)
        self.c.on_message = lambda c, u, m: self.received.append((m.topic, m.payload))
        self.c.on_publish = lambda c, u, mid, rc, p: self.acks.__setitem__(mid, rc.value)
        self.c.on_subscribe = lambda c, u, mid, rcs, p: self.granted.__setitem__(mid, [r.value for r in rcs])
        self.c.on_disconnect = lambda c, u, f, rc, p: setattr(self, "disconnected", True)
        self.c.connect(HOST, PORT)
        self.c.loop_start()
        time.sleep(0.2)

    def publish(self, topic, pairs, payload=b""):
        info = self.c.publish(topic, payload=payload, qos=1,
                              properties=props(PacketTypes.PUBLISH, [("DAP-Allow", "1")] + pairs))
        info.wait_for_publish(5)
        time.sleep(0.2)
        return self.acks.get(info.mid)

    def subscribe(self, topic, sp):
        _, mid = self.c.subscribe(topic, qos=1, properties=props(PacketTypes.SUBSCRIBE, [("DAP-SP", sp)]))
        time.sleep(0.3)
        return self.granted.get(mid)

    def stop(self):
        self.c.loop_stop()
        self.c.disconnect()


def write_conf(conf, purposes):
    with open(conf, "w") as f:
        f.write(f"listener {PORT} {HOST}\n"
                "allow_anonymous true\n")
        for p in purposes:
            f.write(f"dap_recognized_purposes {p}\n")


def main():
    workdir = tempfile.mkdtemp(prefix="dap-recognized-")
    conf = os.path.join(workdir, "mosquitto.conf")
    write_conf(conf, ["quality/assurance", "maintenance/{predictive,routine}"])
    log = open(os.path.join(workdir, "broker.log"), "w")
    broker = subprocess.Popen([BROKER, "-c", conf], stdout=log, stderr=subprocess.STDOUT)
    time.sleep(0.5)
    try:
        sub = Client("subR")
        pub = Client("pubR")

        check(sub.subscribe("t/#", "quality/assurance") == [1],
              "a subscription whose SP is recognized is granted")
        check(sub.subscribe("t/y", "maintenance/{predictive,routine}") == [1],
              "an SP that describes several recognized purposes is granted")
        check(sub.subscribe("t/x", "marketing") == [0x87],
              "a subscription whose SP describes an unrecognized purpose is refused")
        check(sub.subscribe("OP_REQ/subR", "DAP_OP") == [1],
              "the operation purpose is always recognized")

        check(pub.publish("$MP_REG", [("DAP-MP", "quality/assurance:t/a")]) == 0,
              "an MP of recognized purposes is registered")
        check(pub.publish("$MP_REG", [("DAP-MP", "maintenance/routine:t/b"),
                                      ("DAP-MP", "quality/assurance|marketing:t/c")]) == 0x87,
              "a registration naming an unrecognized purpose is refused")
        check(pub.publish("t/a", [], b"a") == 0, "data under a recognized MP is accepted")
        check(pub.publish("t/b", [], b"b") == 0x87, "nothing in the refused registration was registered")
        time.sleep(0.3)
        check(sub.received == [("t/a", b"a")], "only data under the recognized MP is delivered (got %s)"
              % sub.received)
        check(not sub.disconnected and not pub.disconnected, "both clients stay connected")

        write_conf(conf, ["marketing"])
        broker.send_signal(signal.SIGHUP)
        time.sleep(0.5)
        check(sub.subscribe("t/x", "marketing") == [1], "a purpose added by a reload is recognized")
        check(sub.subscribe("t/q", "quality/assurance") == [0x87], "a purpose removed by a reload is not")
        sub.stop()
        pub.stop()
    finally:
        broker.terminate()
        rc = broker.wait(timeout=10)
        log.close()
    check(rc == 0, "broker exits cleanly")

    print()
    if failures:
        print("RECOGNIZED PURPOSES TEST FAILED: %d check(s)" % len(failures))
        return 1
    print("RECOGNIZED PURPOSES TEST PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
