#!/usr/bin/env bash
# Run the DAP integration tests against the broker in $BUILD_ROOT (the repository
# root for a make build, the build directory for CMake). Needs paho-mqtt 2.x.

set -u

R="$(cd "$(dirname "$0")/../.." && pwd)"
export BUILD_ROOT="${BUILD_ROOT:-$R}"
BROKER="$BUILD_ROOT/src/mosquitto"
PLUGIN="$BUILD_ROOT/plugins/persist-sqlite/mosquitto_persist_sqlite.so"
T="$R/test/dap"
PORT=18830
OUT="$(mktemp -d)"

# macOS has no timeout(1) unless coreutils is installed.
TIMEOUT="$(command -v timeout || command -v gtimeout || true)"
[ -n "$TIMEOUT" ] && TIMEOUT="$TIMEOUT 600"

pass=0
fail=0
failed=()

run() {
	local name=$1
	shift
	if $TIMEOUT python3 "$T/$name.py" "$@" > "$OUT/$name.log" 2>&1; then
		echo "PASS $name"
		pass=$((pass + 1))
	else
		echo "FAIL $name"
		sed 's/^/    /' "$OUT/$name.log" | tail -30
		fail=$((fail + 1)); failed+=("$name")
	fi
}

# These two talk to a broker that is already running.
printf "listener %d 127.0.0.1\nallow_anonymous true\n" "$PORT" > "$OUT/shared.conf"
"$BROKER" -c "$OUT/shared.conf" > "$OUT/shared-broker.log" 2>&1 &
broker_pid=$!
sleep 1
run multi_filter_routing_test 127.0.0.1 "$PORT"
run empty_topic_crash_repro 127.0.0.1 "$PORT"
kill "$broker_pid"
if wait "$broker_pid"; then
	echo "PASS shared broker exits cleanly"
	pass=$((pass + 1))
else
	echo "FAIL shared broker exits cleanly"
	sed 's/^/    /' "$OUT/shared-broker.log" | tail -30
	fail=$((fail + 1)); failed+=("shared broker exit")
fi

# These start their own brokers.
run op_persistence_test "$BROKER" "$PLUGIN" $((PORT + 1))
run user_property_robustness_test $((PORT + 2))
run op_response_paths_test $((PORT + 3))

echo
echo "DAP integration tests: $pass passed, $fail failed"
if [ "$fail" -ne 0 ]; then
	printf '  %s\n' "${failed[@]}"
	exit 1
fi
