/* dap_topics_test.c - standalone unit test for the DAP operation-system topic
 * classifier. Compile (CUnit is not installed locally):
 *
 *   cc -std=c11 -Werror -fsanitize=address,undefined -g \
 *       -I include -I lib \
 *       -o /tmp/dap_topics_test \
 *       test/unit/lib/dap_topics_test.c lib/dap_topics.c
 *   /tmp/dap_topics_test
 */
#include <assert.h>
#include <stdio.h>

#include "dap_topics.h"
#include "mosquitto/defs.h"

static void test_purpose_management_topics_are_system(void)
{
    assert(dap_is_op_system_topic("$DAP/MP_reg/sensors/temp") == true);
    assert(dap_is_op_system_topic("$DAP/SP_reg/sensors/temp") == true);
    assert(dap_is_op_system_topic("$DAP/purpose_management") == true);
    printf("ok - $DAP/* purpose-management topics are system topics\n");
}

static void test_osys_is_system(void)
{
    assert(dap_is_op_system_topic(MOSQ_DAP_TOPIC_OSYS) == true);
    assert(dap_is_op_system_topic(MOSQ_DAP_TOPIC_OSYS "/anything") == true);
    printf("ok - " MOSQ_DAP_TOPIC_OSYS " status bus is a system topic\n");
}

static void test_operation_request_topics_bare_and_keyed(void)
{
    assert(dap_is_op_system_topic(MOSQ_DAP_TOPIC_ORS) == true);
    assert(dap_is_op_system_topic(MOSQ_DAP_TOPIC_ORS "/sub1") == true);
    assert(dap_is_op_system_topic(MOSQ_DAP_TOPIC_ONP) == true);
    assert(dap_is_op_system_topic(MOSQ_DAP_TOPIC_ONP "/pub1") == true);
    printf("ok - " MOSQ_DAP_TOPIC_ORS "/" MOSQ_DAP_TOPIC_ONP " bare and keyed are system topics\n");
}

static void test_data_topics_are_not_system(void)
{
    assert(dap_is_op_system_topic("sensors/temp") == false);
    assert(dap_is_op_system_topic("op_resp/pub1") == false);
    /* Topics that merely share a prefix with a base must not be exempted. */
    assert(dap_is_op_system_topic(MOSQ_DAP_TOPIC_ORS "x") == false);
    assert(dap_is_op_system_topic(MOSQ_DAP_TOPIC_ORS "A/x") == false);
    assert(dap_is_op_system_topic(MOSQ_DAP_TOPIC_ONP "yx") == false);
    assert(dap_is_op_system_topic(MOSQ_DAP_TOPIC_OSYS "ish") == false);
    printf("ok - data topics and prefix lookalikes are not system topics\n");
}

static void test_null_is_not_system(void)
{
    assert(dap_is_op_system_topic(NULL) == false);
    printf("ok - NULL is not a system topic\n");
}

int main(void)
{
    test_purpose_management_topics_are_system();
    test_osys_is_system();
    test_operation_request_topics_bare_and_keyed();
    test_data_topics_are_not_system();
    test_null_is_not_system();
    printf("\nAll dap_topics tests passed.\n");
    return 0;
}
