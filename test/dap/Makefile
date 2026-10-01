R=../..

.PHONY: all test unit integration

all :

test : unit integration

# Standalone lib/dap unit tests, built and run under ASan/UBSan.
unit :
	./run_unit_tests.sh

# End-to-end tests against the broker in $(BUILD_ROOT) (default: this checkout).
integration :
	./run_integration_tests.sh
