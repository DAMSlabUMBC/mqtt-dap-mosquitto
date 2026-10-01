#!/usr/bin/env python3
"""Malformed v5 PUBLISH/SUBSCRIBE packets must not crash or leak in the DAP code.

Each case sends one packet to a fresh broker, which must survive it and exit
cleanly (under make WITH_ASAN=yes a leak fails that check).

Usage: python3 test/dap/user_property_robustness_test.py [port]
"""
import os
import socket
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import mosq_test  # noqa: E402
import mqtt5_props  # noqa: E402

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 18832

CONSENT = "DAP-Allow"
MP_KEY = "DAP-MP"
SP_KEY = "DAP-SP"
MP_REG_TOPIC = "$MP_REG"

failures = []


def check(cond, what):
    print(("ok   - " if cond else "FAIL - ") + what)
    if not cond:
        failures.append(what)


def user_props(*pairs):
    # gen_publish/gen_subscribe add the property-length prefix themselves.
    return b"".join(mqtt5_props.gen_string_pair_prop(mqtt5_props.USER_PROPERTY, k, v) for k, v in pairs)


def connect(client_id):
    connect_packet = mosq_test.gen_connect(client_id, proto_ver=5)
    connack_packet = mosq_test.gen_connack(rc=0, proto_ver=5)
    return mosq_test.do_client_connect(connect_packet, connack_packet, port=PORT, timeout=5)


def sync(sock):
    """Round-trip a PINGREQ so everything sent before it has been processed."""
    mosq_test.do_send_receive(sock, mosq_test.gen_pingreq(), mosq_test.gen_pingresp(), "pingresp")


def broker_alive():
    try:
        sock = connect("liveness-probe")
        sock.close()
        return True
    except (OSError, mosq_test.TestError):
        return False


def send_and_drop(client_id, packet):
    """Send one packet, give the broker time to act on it, then disconnect."""
    try:
        sock = connect(client_id)
        sock.send(packet)
        time.sleep(0.2)
        sock.close()
    except (OSError, mosq_test.TestError):
        pass


def publish_packet(props, topic="robust/data"):
    return mosq_test.gen_publish(topic, qos=1, mid=1, payload=b"x", proto_ver=5, properties=props)


def subscribe_packet(props, topic="robust/data"):
    return mosq_test.gen_subscribe(1, topic, 0, proto_ver=5, properties=props)


def positive_control():
    """A registered MP still reaches a subscriber whose SP matches it."""
    sub = connect("robust-ctrl-sub")
    mosq_test.do_send_receive(
        sub,
        mosq_test.gen_subscribe(1, "robust/ctrl", 0, proto_ver=5, properties=user_props((SP_KEY, "qa"))),
        mosq_test.gen_suback(1, 0, proto_ver=5),
        "control suback")

    pub = connect("robust-ctrl-pub")
    pub.send(mosq_test.gen_publish(MP_REG_TOPIC, qos=0, payload=b"", proto_ver=5,
                                   properties=user_props((CONSENT, "1"), (MP_KEY, "qa:robust/ctrl"))))
    sync(pub)
    pub.send(mosq_test.gen_publish("robust/ctrl", qos=0, payload=b"hello", proto_ver=5,
                                   properties=user_props((CONSENT, "1"))))
    sync(pub)

    sub.settimeout(5)
    try:
        data = sub.recv(1024)
    except socket.timeout:
        data = b""
    pub.close()
    sub.close()
    return len(data) > 0 and data[0] & 0xF0 == 0x30 and data.endswith(b"hello")


CASES = [
    # (description, packet). Consent parsing:
    ("PUBLISH with an empty-key user property", publish_packet(user_props(("", "p"), (CONSENT, "1")))),
    ("PUBLISH with an empty-value user property", publish_packet(user_props(("k", ""), (CONSENT, "1")))),
    ("PUBLISH with an empty key and value", publish_packet(user_props(("", ""), (CONSENT, "1")))),
    ("PUBLISH with an empty DAP-Allow value", publish_packet(user_props((CONSENT, "")))),
    # MP registration parsing:
    ("PUBLISH with an empty DAP-MP value on " + MP_REG_TOPIC,
     publish_packet(user_props((CONSENT, "1"), (MP_KEY, "")), topic=MP_REG_TOPIC)),
    ("PUBLISH with an empty-key user property on " + MP_REG_TOPIC,
     publish_packet(user_props((CONSENT, "1"), ("", "p"), (MP_KEY, "qa:robust/data")), topic=MP_REG_TOPIC)),
    # SP parsing:
    ("SUBSCRIBE with an empty-key user property", subscribe_packet(user_props(("", "p"), (SP_KEY, "qa")))),
    ("SUBSCRIBE with an empty DAP-SP value", subscribe_packet(user_props((SP_KEY, "")))),
    ("SUBSCRIBE with an empty key and value", subscribe_packet(user_props(("", ""), (SP_KEY, "qa")))),
    ("SUBSCRIBE with a DAP-SP and an invalid topic filter",
     subscribe_packet(user_props((SP_KEY, "qa")), topic="robust/#/data")),
    ("PUBLISH with a topic alias above the maximum",
     publish_packet(user_props((CONSENT, "1")) + mqtt5_props.gen_uint16_prop(mqtt5_props.TOPIC_ALIAS, 11))),
    ("PUBLISH with an empty topic and an unknown topic alias",
     publish_packet(user_props((CONSENT, "1")) + mqtt5_props.gen_uint16_prop(mqtt5_props.TOPIC_ALIAS, 3), topic="")),
    ("SUBSCRIBE with more DAP-SP purposes than allowed",
     subscribe_packet(user_props(*[(SP_KEY, "p%03d" % i) for i in range(101)]))),
]


def run_with_broker(what, body):
    """Run body() against a fresh broker; the broker must survive and exit cleanly."""
    broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=PORT)
    try:
        ok = body()
        alive = broker_alive()
    finally:
        rc, stde = mosq_test.terminate_broker(broker)
    check(ok and alive and rc == 0 and broker.returncode == 0,
          "%s (alive=%s, exit code %s)" % (what, alive, broker.returncode))
    if not (alive and broker.returncode == 0):
        print("    " + stde.decode("utf-8", errors="replace")[-1500:].replace("\n", "\n    "))


def main():
    run_with_broker("control: matching SP receives the publish", positive_control)
    for what, packet in CASES:
        run_with_broker("broker survives " + what,
                        lambda packet=packet: send_and_drop("robust-client", packet) or True)
    run_with_broker("control: matching SP still receives after the cases", positive_control)

    print()
    if failures:
        print("USER PROPERTY ROBUSTNESS TEST FAILED: %d check(s)" % len(failures))
        sys.exit(1)
    print("USER PROPERTY ROBUSTNESS TEST PASSED")


if __name__ == "__main__":
    main()
