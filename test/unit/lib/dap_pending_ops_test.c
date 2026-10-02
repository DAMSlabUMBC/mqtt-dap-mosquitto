/* Standalone isolation test for the MQTT-DAP pending-operation map
 * (lib/dap_pending_ops.c).
 *
 * Covers op-id assignment, the enqueue-time gate, publisher scoping, filter
 * matching (MQTT topic filters, purpose-set intersection with the subscription's
 * SP), RESTRICT most-recent-wins and its revoked purposes, DELETE-supersedes-RESTRICT
 * and removal.
 * Build and run on its own (CUnit is not required here):
 *
 *   cc -I../../.. -I../../../lib -I../../../include -I../../../libcommon \
 *      -I../../../src -I../../../common -I../../../deps -I/opt/homebrew/include \
 *      dap_pending_ops_test.c ../../../lib/dap_pending_ops.c \
 *      ../../../libcommon/memory_common.c -o dap_pending_ops_test && ./dap_pending_ops_test
 */

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "dap_pending_ops.h"
#include "purpose_filters.h"

/* Match a message queued for a subscription whose SP is the collection sp_filters
 * (NULL for an unknown SP). */
static enum dap_op_action match(struct dap_pending_ops *map, const char *pub, const char *topic,
                                const char *sp_filters, const char *sub, uint64_t order,
                                uint64_t *op_id, const char **revoked)
{
    char **sp = NULL;
    uint32_t n = 0;
    enum dap_op_action a;

    if(sp_filters){
        assert(purpose_set_expand(sp_filters, &sp, &n) == 0);
    }
    a = dap_pending_ops_match(map, pub, topic, sp_filters ? sp : NULL, n, sub, order, op_id, revoked);
    purpose_set_free(sp, n);
    return a;
}

/* Count the operations currently held for a publisher. */
static int op_count(struct dap_pending_ops *map, const char *pub_id)
{
    int n = 0;
    struct dap_pending_op *op = dap_pending_ops_lookup_operations_for_publisher(map, pub_id);
    while(op){
        n++;
        op = op->next;
    }
    return n;
}

static void test_empty_map(void)
{
    struct dap_pending_ops map;
    uint64_t op_id = 99;

    assert(dap_pending_ops_init(&map) == 0);
    assert(dap_pending_ops_lookup_operations_for_publisher(&map, "pub1") == NULL);
    assert(match(&map, "pub1", "t/a", "p", "sub1", 10, &op_id, NULL) == DAP_OP_ACTION_NONE);
    assert(op_id == 0); /* NONE clears the out-param */
    dap_pending_ops_destroy(&map);
    printf("ok - empty map yields NONE and no operations\n");
}

static void test_insert_assigns_increasing_ids(void)
{
    struct dap_pending_ops map;
    uint64_t id1 = 0, id2 = 0, id3 = 0;

    dap_pending_ops_init(&map);
    assert(dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_RESTRICT, 100, 0, "*", "*", "*", &id1) == 0);
    assert(dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_DELETE, 100, 0, "*", "*", "*", &id2) == 0);
    assert(dap_pending_ops_insert_operation(&map, "pub2", DAP_OP_RESTRICT, 100, 0, "*", "*", "*", &id3) == 0);

    /* Ids are broker-assigned, start at 1 and never repeat across publishers. */
    assert(id1 == 1 && id2 == 2 && id3 == 3);
    assert(op_count(&map, "pub1") == 2);
    assert(op_count(&map, "pub2") == 1);

    dap_pending_ops_destroy(&map);
    printf("ok - insert assigns unique increasing op ids from 1\n");
}

static void test_enqueue_time_gate(void)
{
    struct dap_pending_ops map;
    dap_pending_ops_init(&map);

    /* DELETE invoked at t=100, matching anything. */
    dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_DELETE, 100, 0, "*", "*", "*", NULL);

    /* A message received before the op is affected... */
    assert(match(&map, "pub1", "t/a", "p", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_DROP);
    /* ...as is one at exactly the op's order... */
    assert(match(&map, "pub1", "t/a", "p", "sub1", 100, NULL, NULL) == DAP_OP_ACTION_DROP);
    /* ...but a message received after the op is not. */
    assert(match(&map, "pub1", "t/a", "p", "sub1", 150, NULL, NULL) == DAP_OP_ACTION_NONE);

    dap_pending_ops_destroy(&map);
    printf("ok - operations apply only to messages received at or before them\n");
}

static void test_only_requesting_publisher(void)
{
    struct dap_pending_ops map;
    dap_pending_ops_init(&map);

    dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_DELETE, 100, 0, "*", "*", "*", NULL);

    assert(match(&map, "pub1", "t/a", "p", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_DROP);
    /* A different publisher's messages are untouched. */
    assert(match(&map, "pub2", "t/a", "p", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_NONE);

    dap_pending_ops_destroy(&map);
    printf("ok - operations apply only to the requesting publisher's messages\n");
}

static void test_filters_must_match(void)
{
    struct dap_pending_ops map;
    dap_pending_ops_init(&map);

    /* A specific topic/purpose/subscriber operation. */
    dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_DELETE, 100, 0,
                                     "sensors/temp", "billing", "sub1", NULL);

    /* Exact match on all three applies. */
    assert(match(&map, "pub1", "sensors/temp", "billing", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_DROP);
    /* A mismatch on any single field means the op does not apply. */
    assert(match(&map, "pub1", "sensors/humidity", "billing", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_NONE);
    assert(match(&map, "pub1", "sensors/temp", "research", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_NONE);
    assert(match(&map, "pub1", "sensors/temp", "billing", "sub2", 50, NULL, NULL) == DAP_OP_ACTION_NONE);

    dap_pending_ops_destroy(&map);
    printf("ok - non-wildcard filters must match the message\n");
}

static void test_topic_filter_list(void)
{
    struct dap_pending_ops map;
    dap_pending_ops_init(&map);

    /* A comma-separated list of topic filters - any one of them may match. */
    dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_DELETE, 100, 0,
                                     "sensors/temp,sensors/humidity", "*", "*", NULL);

    /* A message on either listed topic is matched... */
    assert(match(&map, "pub1", "sensors/temp", "p", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_DROP);
    assert(match(&map, "pub1", "sensors/humidity", "p", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_DROP);
    /* ...but a topic absent from the list is not. */
    assert(match(&map, "pub1", "sensors/pressure", "p", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_NONE);

    dap_pending_ops_destroy(&map);
    printf("ok - an operation applies when any topic in its filter list matches\n");
}

static void test_wildcard_within_list(void)
{
    struct dap_pending_ops map;
    dap_pending_ops_init(&map);

    /* "*" sitting anywhere in a list still means "any". */
    dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_RESTRICT, 100, 0,
                                     "sensors/temp,*", "*", "*", NULL);

    assert(match(&map, "pub1", "completely/unrelated", "p", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_RESTRICT);

    dap_pending_ops_destroy(&map);
    printf("ok - a \"*\" anywhere in a filter list matches any value\n");
}

static void test_lists_across_all_filters(void)
{
    struct dap_pending_ops map;
    dap_pending_ops_init(&map);

    /* Lists on every dimension; matching is any-within-a-list but all-lists-must-match. */
    dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_DELETE, 100, 0,
                                     "*", "billing,research", "sub1,sub2", NULL);

    /* Purpose and subscriber both hit a listed element. */
    assert(match(&map, "pub1", "t/a", "research", "sub2", 50, NULL, NULL) == DAP_OP_ACTION_DROP);
    /* Purpose matches but the subscriber is outside its list. */
    assert(match(&map, "pub1", "t/a", "research", "sub3", 50, NULL, NULL) == DAP_OP_ACTION_NONE);
    /* Subscriber matches but the purpose is outside its list. */
    assert(match(&map, "pub1", "t/a", "ads", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_NONE);

    dap_pending_ops_destroy(&map);
    printf("ok - every filter list must match, each by any of its own elements\n");
}

static void test_topic_filters_are_mqtt_filters(void)
{
    struct dap_pending_ops map;
    dap_pending_ops_init(&map);

    dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_DELETE, 100, 0,
                                     "sensors/+/temp,factory/#", "*", "*", NULL);

    assert(match(&map, "pub1", "sensors/a/temp", "p", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_DROP);
    assert(match(&map, "pub1", "factory/line/1", "p", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_DROP);
    assert(match(&map, "pub1", "factory", "p", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_DROP);
    assert(match(&map, "pub1", "sensors/a/humidity", "p", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_NONE);
    assert(match(&map, "pub1", "sensors/temp", "p", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_NONE);

    dap_pending_ops_destroy(&map);
    printf("ok - DAP-OpTFs are matched as MQTT topic filters\n");
}

static void test_purpose_filters_intersect_sp(void)
{
    struct dap_pending_ops map;
    dap_pending_ops_init(&map);

    /* A ','-list of '|'-joined filters, with braces, describes one purpose set. */
    dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_DELETE, 100, 0,
                                     "*", "maintenance/{predictive,routine}|quality,vendor", "*", NULL);

    /* The subscription's SP shares a purpose with the operation's. */
    assert(match(&map, "pub1", "t/a", "maintenance/routine", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_DROP);
    assert(match(&map, "pub1", "t/a", "ads|vendor", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_DROP);
    assert(match(&map, "pub1", "t/a", "quality", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_DROP);
    /* No shared purpose: a parent or sibling does not count. */
    assert(match(&map, "pub1", "t/a", "maintenance", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_NONE);
    assert(match(&map, "pub1", "t/a", "maintenance/calibration", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_NONE);
    /* An unknown SP (a message restored without its subscription) is in scope. */
    assert(match(&map, "pub1", "t/a", NULL, "sub1", 50, NULL, NULL) == DAP_OP_ACTION_DROP);

    dap_pending_ops_destroy(&map);
    printf("ok - DAP-OpPFs apply when they share a purpose with the subscription's SP\n");
}

static void test_restrict_reports_revoked_purposes(void)
{
    struct dap_pending_ops map;
    const char *revoked = NULL;
    dap_pending_ops_init(&map);

    dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_RESTRICT, 100, 0,
                                     "t/a", "operations/forecast,ads", "*", NULL);
    dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_RESTRICT, 100, 0, "t/b", NULL, "*", NULL);

    assert(match(&map, "pub1", "t/a", "ads", "sub1", 50, NULL, &revoked) == DAP_OP_ACTION_RESTRICT);
    assert(!strcmp(revoked, "ads|operations/forecast"));
    /* A RESTRICT without DAP-OpPFs revokes every purpose. */
    revoked = NULL;
    assert(match(&map, "pub1", "t/b", "ads", "sub1", 50, NULL, &revoked) == DAP_OP_ACTION_RESTRICT);
    assert(!strcmp(revoked, "*"));
    /* Only a RESTRICT reports revoked purposes. */
    assert(match(&map, "pub1", "t/c", "ads", "sub1", 50, NULL, &revoked) == DAP_OP_ACTION_NONE);
    assert(revoked == NULL);

    dap_pending_ops_destroy(&map);
    printf("ok - a RESTRICT reports the purposes it revokes\n");
}

static void test_restrict_most_recent_wins(void)
{
    struct dap_pending_ops map;
    uint64_t newer_id = 0, chosen = 0;

    dap_pending_ops_init(&map);

    /* Two RESTRICTs that both apply to the same message; the newer one wins. */
    dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_RESTRICT, 100, 0, "sensors/temp", "*", "*", NULL);
    dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_RESTRICT, 200, 0, "*", "*", "*", &newer_id);

    assert(match(&map, "pub1", "sensors/temp", "p", "sub1", 50, &chosen, NULL) == DAP_OP_ACTION_RESTRICT);
    assert(chosen == newer_id);

    dap_pending_ops_destroy(&map);
    printf("ok - among matching RESTRICTs the most recent timestamp wins\n");
}

static void test_delete_supersedes_restrict(void)
{
    struct dap_pending_ops map;
    uint64_t chosen = 7;

    dap_pending_ops_init(&map);

    /* A RESTRICT with a larger timestamp than the DELETE - DELETE still wins. */
    dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_RESTRICT, 300, 0, "*", "*", "*", NULL);
    dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_DELETE, 100, 0, "*", "*", "*", NULL);

    assert(match(&map, "pub1", "t/a", "p", "sub1", 50, &chosen, NULL) == DAP_OP_ACTION_DROP);

    dap_pending_ops_destroy(&map);
    printf("ok - DELETE supersedes RESTRICT regardless of timestamps\n");
}

static void test_remove_operation(void)
{
    struct dap_pending_ops map;
    uint64_t del_id = 0, res_id = 0;

    dap_pending_ops_init(&map);
    dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_DELETE, 100, 0, "*", "*", "*", &del_id);
    dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_RESTRICT, 200, 0, "*", "*", "*", &res_id);

    /* Removing the DELETE leaves the RESTRICT, which then decides the message. */
    assert(dap_pending_ops_remove_operation_by_id(&map, del_id) == 0);
    assert(op_count(&map, "pub1") == 1);
    assert(match(&map, "pub1", "t/a", "p", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_RESTRICT);

    /* With the newer op gone, older ops still decide older messages. */
    uint64_t old_id = 0;
    dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_DELETE, 100, 0, "*", "*", "*", &old_id);
    uint64_t new_id = 0;
    dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_DELETE, 900, 0, "*", "*", "*", &new_id);
    assert(match(&map, "pub1", "t/a", "p", "sub1", 500, NULL, NULL) == DAP_OP_ACTION_DROP);
    assert(dap_pending_ops_remove_operation_by_id(&map, new_id) == 0);
    assert(match(&map, "pub1", "t/a", "p", "sub1", 500, NULL, NULL) == DAP_OP_ACTION_NONE);
    assert(match(&map, "pub1", "t/a", "p", "sub1", 150, NULL, NULL) == DAP_OP_ACTION_RESTRICT);
    assert(match(&map, "pub1", "t/a", "p", "sub1", 50, NULL, NULL) == DAP_OP_ACTION_DROP);
    assert(dap_pending_ops_remove_operation_by_id(&map, old_id) == 0);

    /* Removing an unknown id is reported as a failure. */
    assert(dap_pending_ops_remove_operation_by_id(&map, 9999) != 0);

    /* Removing the last op drops the publisher entry entirely. */
    assert(dap_pending_ops_remove_operation_by_id(&map, res_id) == 0);
    assert(dap_pending_ops_lookup_operations_for_publisher(&map, "pub1") == NULL);

    dap_pending_ops_destroy(&map);
    printf("ok - remove_operation_by_id unlinks ops and cleans up empty publishers\n");
}

static void test_allocate_op_id_shares_counter(void)
{
    struct dap_pending_ops map;
    uint64_t inserted = 0;

    dap_pending_ops_init(&map);

    /* allocate_op_id hands out ids from the same monotonic counter as insert, so
     * operations that do not enter the map (HISTORY/UPDATE) never collide with
     * DELETE/RESTRICT op ids. */
    assert(dap_pending_ops_allocate_op_id(&map) == 1);
    assert(dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_DELETE, 100, 0, "*", "*", "*", &inserted) == 0);
    assert(inserted == 2);
    assert(dap_pending_ops_allocate_op_id(&map) == 3);
    assert(dap_pending_ops_allocate_op_id(NULL) == 0);

    dap_pending_ops_destroy(&map);
    printf("ok - allocate_op_id shares the insert counter\n");
}

static void test_restore_keeps_op_id_and_matches(void)
{
    struct dap_pending_ops map;
    uint64_t matched = 0;

    dap_pending_ops_init(&map);
    assert(dap_pending_ops_restore_operation(&map, 7, "pub1", DAP_OP_RESTRICT, 1000, 0,
                                             "t/a,t/b", "p", "*") == 0);
    assert(op_count(&map, "pub1") == 1);
    assert(match(&map, "pub1", "t/b", "p", "sub1", 900, &matched, NULL) == DAP_OP_ACTION_RESTRICT);
    assert(matched == 7);
    assert(match(&map, "pub1", "t/b", "p", "sub1", 1001, NULL, NULL) == DAP_OP_ACTION_NONE);

    dap_pending_ops_destroy(&map);
    printf("ok - restore keeps the op id, filters and timestamp\n");
}

static void test_restore_advances_id_counter(void)
{
    struct dap_pending_ops map;
    uint64_t id = 0;

    dap_pending_ops_init(&map);
    assert(dap_pending_ops_restore_operation(&map, 5, "pub1", DAP_OP_DELETE, 100, 0, "*", "*", "*") == 0);
    assert(dap_pending_ops_restore_operation(&map, 3, "pub2", DAP_OP_DELETE, 100, 0, "*", "*", "*") == 0);
    assert(dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_RESTRICT, 100, 0, "*", "*", "*", &id) == 0);
    assert(id == 6);
    assert(dap_pending_ops_allocate_op_id(&map) == 7);

    dap_pending_ops_destroy(&map);
    printf("ok - restore advances the op id counter past restored ids\n");
}

static void test_restore_rejects_bad_or_duplicate_ids(void)
{
    struct dap_pending_ops map;

    dap_pending_ops_init(&map);
    assert(dap_pending_ops_restore_operation(&map, 0, "pub1", DAP_OP_DELETE, 100, 0, "*", "*", "*") != 0);
    assert(dap_pending_ops_restore_operation(&map, 4, NULL, DAP_OP_DELETE, 100, 0, "*", "*", "*") != 0);
    assert(dap_pending_ops_restore_operation(&map, 4, "pub1", DAP_OP_DELETE, 100, 0, "*", "*", "*") == 0);
    assert(dap_pending_ops_restore_operation(&map, 4, "pub2", DAP_OP_RESTRICT, 100, 0, "*", "*", "*") != 0);
    assert(op_count(&map, "pub1") == 1);
    assert(op_count(&map, "pub2") == 0);

    dap_pending_ops_destroy(&map);
    printf("ok - restore rejects a zero or already-present op id\n");
}

static void test_reserve_op_id(void)
{
    struct dap_pending_ops map;

    dap_pending_ops_init(&map);
    dap_pending_ops_reserve_op_id(&map, 9);
    assert(dap_pending_ops_allocate_op_id(&map) == 10);
    /* Never moves backwards. */
    dap_pending_ops_reserve_op_id(&map, 2);
    assert(dap_pending_ops_allocate_op_id(&map) == 11);

    dap_pending_ops_destroy(&map);
    printf("ok - reserve_op_id keeps new ids above a restored id\n");
}

static void removed_count(uint64_t op_id, void *arg)
{
    (void)op_id;
    (*(int *)arg)++;
}

static void test_cover_stored(void)
{
    struct dap_pending_ops map;
    uint64_t id = 0;

    dap_pending_ops_init(&map);
    dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_RESTRICT, 100, 0, "t/#", "qa", "subX", &id);

    /* DAP-OpClients is ignored; the MP must share a purpose with DAP-OpPFs. */
    assert(dap_pending_ops_cover_stored(&map, "pub1", "t/a", "qa|qb", 50));
    assert(dap_pending_ops_cover_stored(&map, "pub1", "t/a", "*", 50));
    assert(dap_pending_ops_cover_stored(&map, "pub1", "t/a", NULL, 50));
    assert(!dap_pending_ops_cover_stored(&map, "pub1", "t/a", "qb", 50));
    assert(!dap_pending_ops_cover_stored(&map, "pub1", "u/a", "qa", 50));
    assert(!dap_pending_ops_cover_stored(&map, "pub1", "t/a", "qa", 150));
    assert(!dap_pending_ops_cover_stored(&map, "pub2", "t/a", "qa", 50));

    dap_pending_ops_destroy(&map);
    printf("ok - an operation covers stored messages by topic, MP and receipt order\n");
}

static void test_remove_expired(void)
{
    struct dap_pending_ops map;
    uint64_t early = 0, late = 0, none = 0;
    int removed = 0;

    dap_pending_ops_init(&map);
    dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_DELETE, 100, 50, "*", "*", "*", &early);
    dap_pending_ops_insert_operation(&map, "pub1", DAP_OP_DELETE, 900, 80, "*", "*", "*", &late);
    dap_pending_ops_insert_operation(&map, "pub2", DAP_OP_RESTRICT, 100, 0, "*", "*", "*", &none);

    /* Nothing has expired yet. */
    assert(!dap_pending_ops_any_expired(&map, 49));
    assert(dap_pending_ops_any_expired(&map, 50));
    dap_pending_ops_remove_expired(&map, 49, removed_count, &removed);
    assert(removed == 0 && op_count(&map, "pub1") == 2);

    /* The op whose deadline passed is reclaimed; the newest remaining op now bounds pub1. */
    dap_pending_ops_remove_expired(&map, 60, removed_count, &removed);
    assert(removed == 1 && op_count(&map, "pub1") == 1);
    assert(match(&map, "pub1", "t/a", "p", "sub1", 500, NULL, NULL) == DAP_OP_ACTION_DROP);
    assert(match(&map, "pub1", "t/a", "p", "sub1", 950, NULL, NULL) == DAP_OP_ACTION_NONE);

    /* The last op of a publisher takes its entry with it; a deadline of 0 never expires. */
    dap_pending_ops_remove_expired(&map, 1000, removed_count, &removed);
    assert(removed == 2);
    assert(dap_pending_ops_lookup_operations_for_publisher(&map, "pub1") == NULL);
    assert(op_count(&map, "pub2") == 1);
    assert(!dap_pending_ops_any_expired(&map, 5000));

    dap_pending_ops_destroy(&map);
    printf("ok - operations are reclaimed once their deadline passes\n");
}

int main(void)
{
    test_empty_map();
    test_insert_assigns_increasing_ids();
    test_allocate_op_id_shares_counter();
    test_enqueue_time_gate();
    test_only_requesting_publisher();
    test_filters_must_match();
    test_topic_filter_list();
    test_wildcard_within_list();
    test_lists_across_all_filters();
    test_topic_filters_are_mqtt_filters();
    test_purpose_filters_intersect_sp();
    test_restrict_reports_revoked_purposes();
    test_restrict_most_recent_wins();
    test_delete_supersedes_restrict();
    test_remove_operation();
    test_restore_keeps_op_id_and_matches();
    test_restore_advances_id_counter();
    test_restore_rejects_bad_or_duplicate_ids();
    test_reserve_op_id();
    test_cover_stored();
    test_remove_expired();
    printf("\nAll dap_pending_ops tests passed.\n");
    return 0;
}
