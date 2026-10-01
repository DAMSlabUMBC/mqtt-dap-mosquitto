#!/usr/bin/env bash
# Build and run the standalone DAP unit tests (test/unit/lib) under ASan/UBSan.

set -u

R="$(cd "$(dirname "$0")/../.." && pwd)"
T="$R/test/unit/lib"
D="$R/lib/dap"
[ -d "$D" ] || D="$R/lib"  # pre-2.1.2 layout
OUT="${DAP_TEST_OUT:-$(mktemp -d)}"
CC="${CC:-cc}"

INC=(-I"$R" -I"$R/lib" -I"$D" -I"$R/include" -I"$R/libcommon" -I"$R/src" -I"$R/common" -I"$R/deps")
# cJSON headers outside the default search path (e.g. Homebrew on macOS).
if command -v pkg-config > /dev/null && pkg-config --exists libcjson; then
	# shellcheck disable=SC2207
	INC+=($(pkg-config --cflags libcjson))
fi
MEM="$R/libcommon/memory_common.c"
TOPIC="$R/libcommon/topic_common.c"
PROPS="$R/libcommon/property_common.c $R/libcommon/utf8_common.c $R/libcommon/mqtt_common.c $TOPIC"
SAN=(-fsanitize=address,undefined -fno-omit-frame-pointer -g -O0)

# name | sources (relative to lib/dap unless absolute) | extra flags
TESTS=(
	"dap_deadline_tracker_test|dap_deadline_tracker.c $MEM|"
	"dap_holding_list_test|dap_holding_list.c $MEM|"
	"dap_op_requester_test|dap_op_requester.c $MEM|"
	"dap_op_request_test|dap_op_request.c dap_pending_ops.c purpose_filters.c $TOPIC $MEM|"
	"dap_pending_ops_test|dap_pending_ops.c purpose_filters.c $TOPIC $MEM|"
	"dap_request_store_test|dap_request_store.c $PROPS $MEM|"
	"dap_send_verify_test|dap_send_verify.c|"
	"dap_stamp_test|dap_stamp.c dap_subscription_queues.c mp_registry.c $MEM|"
	"dap_subscription_queues_test|dap_subscription_queues.c $MEM|"
	"dap_timestamp_test|dap_timestamp.c|"
	"dap_topics_test|dap_topics.c|"
	"purpose_filter_copy_test|purpose_filters.c $MEM|-include string.h"
	"purpose_filter_match_test|purpose_filters.c $MEM|-include string.h"
	"purpose_version_test|mp_registry.c $MEM|"
	"dr_relevance_test|dr_registry.c purpose_filters.c $TOPIC $MEM|"
)

pass=0
fail=0
failed=()
for entry in "${TESTS[@]}"; do
	IFS='|' read -r name srcs extra <<< "$entry"
	files=()
	for s in $srcs; do
		case "$s" in
			/*) files+=("$s") ;;
			*) files+=("$D/$s") ;;
		esac
	done
	# shellcheck disable=SC2086
	if ! "$CC" "${INC[@]}" "${SAN[@]}" $extra "$T/$name.c" "${files[@]}" -o "$OUT/$name" > "$OUT/$name.build.log" 2>&1; then
		echo "FAIL (build) $name"
		sed 's/^/    /' "$OUT/$name.build.log" | head -20
		fail=$((fail + 1)); failed+=("$name")
		continue
	fi
	if (cd "$T" && "$OUT/$name") > "$OUT/$name.run.log" 2>&1; then
		echo "PASS $name ($(grep -c '^ok' "$OUT/$name.run.log") checks)"
		pass=$((pass + 1))
	else
		echo "FAIL (run) $name"
		sed 's/^/    /' "$OUT/$name.run.log" | tail -20
		fail=$((fail + 1)); failed+=("$name")
	fi
done

echo
echo "DAP unit tests: $pass passed, $fail failed"
if [ "$fail" -ne 0 ]; then
	printf '  %s\n' "${failed[@]}"
	exit 1
fi
