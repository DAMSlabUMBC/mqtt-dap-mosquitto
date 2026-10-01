/* Integration check for Case 2 phase 1: an incoming DAP operation request inserts a
 * pending operation into the broker-wide map (lib/dap_op_request.c).
 *
 * This is the unit-level stand-in for "a publish carrying DAP-OpType:Erasure /
 * Restriction inserts a pending op": it drives the exact decode + insert path the
 * broker calls from handle_publish.c's operation block, using the real pending-op
 * map, and verifies the op is then visible via dap_pending_ops_match /
 * dap_pending_ops_lookup_operations_for_publisher. The full broker is not exercised
 * here. Build and run on its own:
 *
 *   cc -I../../.. -I../../../lib -I../../../lib/dap -I../../../include -I../../../libcommon \
 *      -I../../../src -I../../../common -I../../../deps -I/opt/homebrew/include \
 *      dap_op_request_test.c ../../../lib/dap/dap_op_request.c ../../../lib/dap/dap_pending_ops.c \
 *      ../../../libcommon/memory_common.c -o dap_op_request_test && ./dap_op_request_test

 */

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "dap_op_request.h"
#include "dap_pending_ops.h"
#include "mosquitto/defs.h"
#include "purpose_filters.h"

/* Match a message queued for a subscription whose SP is the collection sp_filters. */
static enum dap_op_action match(struct dap_pending_ops *map, const char *pub, const char *topic,
                                const char *sp_filters, const char *sub, uint64_t order, uint64_t *op_id)
{
    char **sp = NULL;
    uint32_t n = 0;
    enum dap_op_action a;

    assert(purpose_set_expand(sp_filters, &sp, &n) == 0);
    a = dap_pending_ops_match(map, pub, topic, sp, n, sub, order, op_id, NULL);
    purpose_set_free(sp, n);
    return a;
}

/* Wrap the args into the dap__op_property the API now takes. */
static int request_insert(struct dap_pending_ops *map, const char *pub_id, const char *op_type,
                          const char *topic_filters, const char *purpose_filters,
                          const char *client_filters, uint64_t order, uint64_t *op_id_out)
{
    struct dap__op_property props;
    memset(&props, 0, sizeof(props));
    props.op_id = (char *)op_type;
    props.op_topic_filters = (char *)topic_filters;
    props.op_purpose_filters = (char *)purpose_filters;
    props.op_client_filters = (char *)client_filters;
    return dap_op_request_insert(map, pub_id, &props, order, op_id_out);
}

/* How many ops are currently tracked for a publisher. */
static size_t count_ops(struct dap_pending_ops *map, const char *pub)
{
    size_t n = 0;
    for(struct dap_pending_op *o = dap_pending_ops_lookup_operations_for_publisher(map, pub);
        o; o = o->next){
        n++;
    }
    return n;
}

/* An Erasure request inserts a DELETE op the map then resolves to DROP. */
static void test_erasure_inserts_delete(void)
{
    struct dap_pending_ops map;
    dap_pending_ops_init(&map);

    uint64_t op_id = 0;
    assert(request_insert(&map, "pub1", MOSQ_DAP_OP_DELETE,
                                 "sensors/a", "*", "*", 1000, &op_id) == 0);
    assert(op_id != 0);
    assert(count_ops(&map, "pub1") == 1);

    uint64_t matched = 0;
    enum dap_op_action a = match(&map, "pub1", "sensors/a", "research", "subX", 500, &matched);
    assert(a == DAP_OP_ACTION_DROP);
    assert(matched == op_id);

    dap_pending_ops_destroy(&map);
    printf("ok - Erasure request inserts a DELETE op (matches as DROP)\n");
}

/* A Restriction request inserts a RESTRICT op the map then resolves to RESTRICT. */
static void test_restriction_inserts_restrict(void)
{
    struct dap_pending_ops map;
    dap_pending_ops_init(&map);

    uint64_t op_id = 0;
    assert(request_insert(&map, "pub1", MOSQ_DAP_OP_RESTRICT,
                                 "sensors/a", "*", "*", 1000, &op_id) == 0);
    assert(op_id != 0);

    uint64_t matched = 0;
    enum dap_op_action a = match(&map, "pub1", "sensors/a", "research", "subX", 500, &matched);
    assert(a == DAP_OP_ACTION_RESTRICT);
    assert(matched == op_id);

    dap_pending_ops_destroy(&map);
    printf("ok - Restriction request inserts a RESTRICT op (matches as RESTRICT)\n");
}

/* The three filter strings are parsed as comma-separated lists: any element matches,
 * a value outside every list does not. */
static void test_filters_parsed_as_lists(void)
{
    struct dap_pending_ops map;
    dap_pending_ops_init(&map);

    assert(request_insert(&map, "pub1", MOSQ_DAP_OP_DELETE,
                                 "t/a,t/b,t/c", "*", "sub1,sub2", 1000, NULL) == 0);

    /* A middle topic element + a listed subscriber -> applies. */
    assert(match(&map, "pub1", "t/b", "p", "sub2", 500, NULL)
           == DAP_OP_ACTION_DROP);
    /* Topic outside the list -> no match. */
    assert(match(&map, "pub1", "t/z", "p", "sub2", 500, NULL)
           == DAP_OP_ACTION_NONE);
    /* Subscriber outside the list -> no match. */
    assert(match(&map, "pub1", "t/a", "p", "sub9", 500, NULL)
           == DAP_OP_ACTION_NONE);

    dap_pending_ops_destroy(&map);
    printf("ok - DAP-OpTFs/OpPFs/OpClients parsed as comma-separated lists\n");
}

/* NULL filter strings mean "any". */
static void test_null_filters_match_any(void)
{
    struct dap_pending_ops map;
    dap_pending_ops_init(&map);

    assert(request_insert(&map, "pub1", MOSQ_DAP_OP_DELETE,
                                 NULL, NULL, NULL, 1000, NULL) == 0);
    assert(match(&map, "pub1", "anything", "anyhow", "anyone", 500, NULL)
           == DAP_OP_ACTION_DROP);

    dap_pending_ops_destroy(&map);
    printf("ok - NULL filter strings match any topic/purpose/subscriber\n");
}

/* Operations that are not erasure/restriction are left for the existing right
 * handlers and never inserted - including other C2/C3 rights like Access. */
static void test_non_pending_operations_not_inserted(void)
{
    struct dap_pending_ops map;
    dap_pending_ops_init(&map);

    assert(request_insert(&map, "pub1", MOSQ_DAP_OP_AUDIT,
                                 "*", "*", "*", 1000, NULL) != 0);
    assert(request_insert(&map, "pub1", MOSQ_DAP_OP_HISTORY,
                                 "*", "*", "*", 1000, NULL) != 0);
    assert(count_ops(&map, "pub1") == 0);

    dap_pending_ops_destroy(&map);
    printf("ok - non-erasure/restriction operations are not inserted\n");
}

/* Bad arguments are rejected without inserting. */
static void test_bad_args(void)
{
    struct dap_pending_ops map;
    dap_pending_ops_init(&map);

    assert(request_insert(NULL, "pub1", MOSQ_DAP_OP_DELETE,
                                 "*", "*", "*", 1000, NULL) != 0);
    assert(request_insert(&map, NULL, MOSQ_DAP_OP_DELETE,
                                 "*", "*", "*", 1000, NULL) != 0);
    assert(request_insert(&map, "pub1", NULL,
                                 "*", "*", "*", 1000, NULL) != 0);
    assert(count_ops(&map, "pub1") == 0);

    dap_pending_ops_destroy(&map);
    printf("ok - bad arguments rejected without inserting\n");
}

int main(void)
{
    test_erasure_inserts_delete();
    test_restriction_inserts_restrict();
    test_filters_parsed_as_lists();
    test_null_filters_match_any();
    test_non_pending_operations_not_inserted();
    test_bad_args();
    printf("\nAll dap_op_request tests passed.\n");
    return 0;
}
