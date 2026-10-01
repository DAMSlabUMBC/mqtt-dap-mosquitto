#!/usr/bin/env python3
"""End-to-end purpose matching (paper section 4).

A subscription receives a message when its topic filter matches and every purpose
its SP describes is permitted by the message's MP, or the MP is "*". SPs are given
bare or as <SP>:<topic_filter>, which binds them to one subscription of the packet.

Usage: the broker must already be running on HOST:PORT with allow_anonymous.
  python3 purpose_matching_test.py [host] [port]
"""
import sys
import time

import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes

HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 18830

MPS = {
    "pm/temp": "quality/assurance|operations/forecast",
    "pm/vib": "maintenance/{predictive,routine}",
    "pm/partner": "partner/{.,logistics}",
    "pm/open": "*",
}

# client id -> (topic filter, DAP-SP values, topics it must receive)
CASES = {
    "subA": ("pm/temp", ["quality/assurance"], ["pm/temp"]),
    "subB": ("pm/temp", ["operations/forecast"], ["pm/temp"]),
    "subC": ("pm/temp", ["vendor/maintenance"], []),
    "subD": ("pm/temp", ["operations/forecast|quality/assurance"], ["pm/temp"]),
    "subE": ("pm/temp", ["quality/assurance", "operations/forecast"], ["pm/temp"]),
    "subF": ("pm/temp", ["quality/assurance|vendor/maintenance"], []),
    "subG": ("pm/temp", ["*"], []),
    "subH": ("pm/vib", ["maintenance/predictive"], ["pm/vib"]),
    "subI": ("pm/vib", ["maintenance"], []),
    "subJ": ("pm/partner", ["partner"], ["pm/partner"]),
    "subK": ("pm/partner", ["partner/billing"], []),
    "subL": ("pm/open", ["anything/at/all"], ["pm/open"]),
    "subM": ("pm/open", [""], []),
    "subN": ("pm/#", ["quality/assurance"], ["pm/open", "pm/temp"]),
    "subO": ("pm/temp", ["quality/assurance:pm/temp"], ["pm/temp"]),
    "subP": ("pm/temp", ["vendor/maintenance:pm/vib", "quality/assurance"], ["pm/temp"]),
    "subQ": ("pm/temp", ["quality/assurance:pm/temp", "vendor/maintenance"], []),
}

received = {}
disconnected = {}
failures = []


def check(ok, msg):
    print(("ok   - " if ok else "FAIL - ") + msg)
    if not ok:
        failures.append(msg)


def client(cid):
    c = mqtt.Client(CallbackAPIVersion.VERSION2, client_id=cid, protocol=mqtt.MQTTv5)
    received[cid] = []
    disconnected[cid] = False
    c.on_message = lambda cl, u, m: received[cid].append(m.topic)
    c.on_disconnect = lambda cl, u, f, rc, p: disconnected.__setitem__(cid, True)
    c.connect(HOST, PORT)
    c.loop_start()
    return c


def props(packet_type, pairs):
    p = Properties(packet_type)
    p.UserProperty = pairs
    return p


def main():
    subs = {}
    for cid, (topic_filter, sps, _) in CASES.items():
        subs[cid] = client(cid)
    time.sleep(0.3)
    for cid, (topic_filter, sps, _) in CASES.items():
        subs[cid].subscribe(topic_filter, qos=0,
                            properties=props(PacketTypes.SUBSCRIBE, [("DAP-SP", sp) for sp in sps]))

    # An SP bound only to another topic filter leaves this subscription undeclared.
    unbound = client("subZ")
    time.sleep(0.2)
    unbound.subscribe("pm/temp", qos=0,
                      properties=props(PacketTypes.SUBSCRIBE, [("DAP-SP", "quality/assurance:pm/vib")]))
    time.sleep(0.4)

    pub = client("pub1")
    time.sleep(0.2)
    for topic, mp in MPS.items():
        pub.publish("$MP_REG", payload="", qos=0,
                    properties=props(PacketTypes.PUBLISH, [("DAP-Allow", "1"), ("DAP-MP", f"{mp}:{topic}")]))
    time.sleep(0.4)
    for topic in MPS:
        pub.publish(topic, payload=b"x", qos=0, properties=props(PacketTypes.PUBLISH, [("DAP-Allow", "1")]))
    time.sleep(0.6)
    rejected = disconnected["subZ"]

    for c in list(subs.values()) + [unbound, pub]:
        c.loop_stop()
        c.disconnect()

    for cid, (topic_filter, sps, expected) in CASES.items():
        got = sorted(received[cid])
        check(got == sorted(expected), "%s on %s with SP %s received %s (expected %s)"
              % (cid, topic_filter, sps, got, sorted(expected)))
    check(rejected and received["subZ"] == [],
          "a subscription whose only SP is bound to another topic filter is rejected")

    print()
    if failures:
        print("PURPOSE MATCHING TEST FAILED: %d check(s)" % len(failures))
        return 1
    print("PURPOSE MATCHING TEST PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
