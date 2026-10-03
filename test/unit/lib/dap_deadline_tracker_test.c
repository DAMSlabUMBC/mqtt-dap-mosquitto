/* Standalone isolation test for the operation-deadline tracker
 * (lib/dap_deadline_tracker.c).
 *
 * Covers registration, marking responses, deadline-expiry detection, removal of
 * expired ops, late responses for untracked ops, and operation independence.
 * Build and run on its own (CUnit is not required here):
 *
 *   cc -I../../.. -I../../../lib -I../../../include -I../../../libcommon \
 *      -I../../../src -I../../../common -I../../../deps -I/opt/homebrew/include \
 *      dap_deadline_tracker_test.c ../../../lib/dap_deadline_tracker.c \
 *      ../../../libcommon/memory_common.c -o dap_deadline_tracker_test \
 *      && ./dap_deadline_tracker_test
 */

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "dap_deadline_tracker.h"

/* Find an expired op by id in a check_expired result list, or NULL. */
static struct dap_expired_op *find_expired(struct dap_expired_op *list, uint64_t op_id)
{
    for(struct dap_expired_op *e = list; e; e = e->next){
        if(e->op_id == op_id) return e;
    }
    return NULL;
}

/* True if an expired op lists sub_id among its unresponded subscribers. */
static bool has_unresponded(struct dap_expired_op *e, const char *sub_id)
{
    for(size_t i = 0; i < e->num_unresponded; i++){
        if(!strcmp(e->unresponded_subs[i], sub_id)) return true;
    }
    return false;
}

/* Count the ops in a check_expired result list. */
static size_t expired_len(struct dap_expired_op *list)
{
    size_t n = 0;
    for(struct dap_expired_op *e = list; e; e = e->next) n++;
    return n;
}

static void test_register_makes_op_tracked(void)
{
    struct dap_deadline_tracker t;
    assert(dap_deadline_tracker_init(&t) == 0);

    assert(dap_deadline_tracker_is_tracked(&t, 1) == false);

    const char *subs[] = {"sub/a", "sub/b"};
    assert(dap_deadline_tracker_register_pending_operation(&t, 1, "pub/x", subs, 2, 100) == 0);
    assert(dap_deadline_tracker_is_tracked(&t, 1) == true);

    dap_deadline_tracker_destroy(&t);
    printf("ok - register starts tracking an operation\n");
}

static void test_register_rejects_duplicate_and_bad_args(void)
{
    struct dap_deadline_tracker t;
    dap_deadline_tracker_init(&t);

    const char *subs[] = {"sub/a"};
    assert(dap_deadline_tracker_register_pending_operation(&t, 7, "pub/x", subs, 1, 100) == 0);

    /* Re-registering the same op_id is rejected and leaves the original intact. */
    assert(dap_deadline_tracker_register_pending_operation(&t, 7, "pub/y", subs, 1, 200) != 0);
    assert(dap_deadline_tracker_is_tracked(&t, 7) == true);

    /* Bad arguments are rejected. */
    assert(dap_deadline_tracker_register_pending_operation(NULL, 8, "pub/x", subs, 1, 100) != 0);
    assert(dap_deadline_tracker_register_pending_operation(&t, 8, NULL, subs, 1, 100) != 0);
    assert(dap_deadline_tracker_is_tracked(&t, 8) == false);

    dap_deadline_tracker_destroy(&t);
    printf("ok - register rejects duplicate op ids and bad arguments\n");
}

static void test_mark_response_and_untracked(void)
{
    struct dap_deadline_tracker t;
    dap_deadline_tracker_init(&t);

    const char *subs[] = {"sub/a", "sub/b"};
    dap_deadline_tracker_register_pending_operation(&t, 1, "pub/x", subs, 2, 100);

    /* Marking an expected subscriber on a tracked op succeeds. */
    assert(dap_deadline_tracker_mark_subscriber_responded(&t, 1, "sub/a") == 0);

    /* Marking a subscriber that was never expected is rejected. */
    assert(dap_deadline_tracker_mark_subscriber_responded(&t, 1, "sub/z") != 0);

    /* A late response for an untracked op is rejected here; the caller forwards it
     * separately. is_tracked is the source of truth. */
    assert(dap_deadline_tracker_is_tracked(&t, 999) == false);
    assert(dap_deadline_tracker_mark_subscriber_responded(&t, 999, "sub/a") != 0);

    dap_deadline_tracker_destroy(&t);
    printf("ok - responses recorded, unexpected and untracked marks rejected\n");
}

static void test_no_expiry_before_deadline(void)
{
    struct dap_deadline_tracker t;
    dap_deadline_tracker_init(&t);

    const char *subs[] = {"sub/a"};
    dap_deadline_tracker_register_pending_operation(&t, 1, "pub/x", subs, 1, 100);

    /* Before the deadline nothing expires and the op stays tracked. */
    struct dap_expired_op *expired = dap_deadline_tracker_check_expired(&t, 99);
    assert(expired == NULL);
    assert(dap_deadline_tracker_is_tracked(&t, 1) == true);

    dap_deadline_tracker_destroy(&t);
    printf("ok - nothing expires before the deadline\n");
}

static void test_expiry_returns_unresponded_and_removes(void)
{
    struct dap_deadline_tracker t;
    dap_deadline_tracker_init(&t);

    const char *subs[] = {"sub/a", "sub/b", "sub/c"};
    dap_deadline_tracker_register_pending_operation(&t, 1, "pub/x", subs, 3, 100);
    /* sub/b responds in time; sub/a and sub/c do not. */
    assert(dap_deadline_tracker_mark_subscriber_responded(&t, 1, "sub/b") == 0);

    /* At the deadline the op expires and reports only the unresponded subscribers. */
    struct dap_expired_op *expired = dap_deadline_tracker_check_expired(&t, 100);
    assert(expired_len(expired) == 1);
    struct dap_expired_op *e = find_expired(expired, 1);
    assert(e != NULL);
    assert(!strcmp(e->publisher_id, "pub/x"));
    assert(e->num_unresponded == 2);
    assert(has_unresponded(e, "sub/a"));
    assert(has_unresponded(e, "sub/c"));
    assert(!has_unresponded(e, "sub/b"));
    dap_deadline_tracker_free_expired(expired);

    /* Expired ops are removed from tracking. */
    assert(dap_deadline_tracker_is_tracked(&t, 1) == false);

    dap_deadline_tracker_destroy(&t);
    printf("ok - expiry returns unresponded subscribers and removes the op\n");
}

static void test_fully_responded_op_expires_with_empty_list(void)
{
    struct dap_deadline_tracker t;
    dap_deadline_tracker_init(&t);

    const char *subs[] = {"sub/a"};
    dap_deadline_tracker_register_pending_operation(&t, 1, "pub/x", subs, 1, 100);
    dap_deadline_tracker_mark_subscriber_responded(&t, 1, "sub/a");

    /* A fully-responded op is still swept once past its deadline, with no offenders. */
    struct dap_expired_op *expired = dap_deadline_tracker_check_expired(&t, 101);
    assert(expired_len(expired) == 1);
    assert(expired->num_unresponded == 0);
    dap_deadline_tracker_free_expired(expired);
    assert(dap_deadline_tracker_is_tracked(&t, 1) == false);

    dap_deadline_tracker_destroy(&t);
    printf("ok - fully-responded op expires with an empty unresponded list\n");
}

static void test_operations_independent(void)
{
    struct dap_deadline_tracker t;
    dap_deadline_tracker_init(&t);

    const char *subs_a[] = {"sub/a"};
    const char *subs_b[] = {"sub/b"};
    dap_deadline_tracker_register_pending_operation(&t, 1, "pub/x", subs_a, 1, 100);
    dap_deadline_tracker_register_pending_operation(&t, 2, "pub/y", subs_b, 1, 200);

    /* Only the op whose deadline has passed is swept; the other stays tracked. */
    struct dap_expired_op *expired = dap_deadline_tracker_check_expired(&t, 150);
    assert(expired_len(expired) == 1);
    struct dap_expired_op *e = find_expired(expired, 1);
    assert(e != NULL);
    assert(!strcmp(e->publisher_id, "pub/x"));
    assert(has_unresponded(e, "sub/a"));
    dap_deadline_tracker_free_expired(expired);

    assert(dap_deadline_tracker_is_tracked(&t, 1) == false);
    assert(dap_deadline_tracker_is_tracked(&t, 2) == true);

    /* Later the second op expires on its own deadline. */
    struct dap_expired_op *expired2 = dap_deadline_tracker_check_expired(&t, 200);
    assert(expired_len(expired2) == 1);
    assert(find_expired(expired2, 2) != NULL);
    dap_deadline_tracker_free_expired(expired2);
    assert(dap_deadline_tracker_is_tracked(&t, 2) == false);

    dap_deadline_tracker_destroy(&t);
    printf("ok - operations expire independently on their own deadlines\n");
}

static void test_destroy_cleans_everything(void)
{
    struct dap_deadline_tracker t;
    dap_deadline_tracker_init(&t);

    const char *subs[] = {"sub/a", "sub/b"};
    dap_deadline_tracker_register_pending_operation(&t, 1, "pub/x", subs, 2, 100);
    dap_deadline_tracker_register_pending_operation(&t, 2, "pub/y", subs, 2, 200);
    dap_deadline_tracker_mark_subscriber_responded(&t, 1, "sub/a");

    dap_deadline_tracker_destroy(&t);

    /* Destroy leaves the tracker empty and reusable. */
    assert(dap_deadline_tracker_is_tracked(&t, 1) == false);
    assert(dap_deadline_tracker_is_tracked(&t, 2) == false);
    const char *subs2[] = {"sub/c"};
    assert(dap_deadline_tracker_register_pending_operation(&t, 3, "pub/z", subs2, 1, 300) == 0);
    assert(dap_deadline_tracker_is_tracked(&t, 3) == true);
    dap_deadline_tracker_destroy(&t);

    printf("ok - destroy frees all operations and stays reusable\n");
}

static void test_all_responded_predicate(void)
{
    struct dap_deadline_tracker t;
    dap_deadline_tracker_init(&t);

    const char *subs[] = {"sub/a", "sub/b"};
    dap_deadline_tracker_register_pending_operation(&t, 1, "pub/x", subs, 2, 100);

    /* False while any expected subscriber is still unresponded. */
    assert(dap_deadline_tracker_all_responded(&t, 1) == false);
    dap_deadline_tracker_mark_subscriber_responded(&t, 1, "sub/a");
    assert(dap_deadline_tracker_all_responded(&t, 1) == false);
    dap_deadline_tracker_mark_subscriber_responded(&t, 1, "sub/b");
    assert(dap_deadline_tracker_all_responded(&t, 1) == true);

    /* An untracked op is not "all responded". */
    assert(dap_deadline_tracker_all_responded(&t, 99) == false);

    dap_deadline_tracker_destroy(&t);
    printf("ok - all_responded is true only once every expected subscriber responded\n");
}

static void test_remove_stops_tracking(void)
{
    struct dap_deadline_tracker t;
    dap_deadline_tracker_init(&t);

    const char *subs[] = {"sub/a"};
    dap_deadline_tracker_register_pending_operation(&t, 1, "pub/x", subs, 1, 100);
    assert(dap_deadline_tracker_is_tracked(&t, 1) == true);

    /* Removing a tracked op succeeds and stops tracking it. */
    assert(dap_deadline_tracker_remove(&t, 1) == 0);
    assert(dap_deadline_tracker_is_tracked(&t, 1) == false);

    /* It no longer expires (the sweep must not double-report it). */
    assert(dap_deadline_tracker_check_expired(&t, 1000) == NULL);

    /* Removing an untracked op is a no-op failure. */
    assert(dap_deadline_tracker_remove(&t, 1) != 0);
    assert(dap_deadline_tracker_remove(NULL, 1) != 0);

    dap_deadline_tracker_destroy(&t);
    printf("ok - remove stops tracking so the deadline sweep cannot re-report it\n");
}

static const struct dap_expected_sub *expected(const struct dap_tracked_op *op, const char *sub_id)
{
    for(const struct dap_expected_sub *sub = op->expected; sub; sub = sub->next){
        if(!strcmp(sub->sub_id, sub_id)) return sub;
    }
    return NULL;
}

/* Each subscriber's latest status and reason are kept for status requests. */
static void test_record_status(void)
{
    struct dap_deadline_tracker t;
    const char *subs[] = {"subA", "subB", "subC"};

    dap_deadline_tracker_init(&t);
    assert(dap_deadline_tracker_register_pending_operation(&t, 7, "pub1", subs, 3, 100) == 0);
    assert(dap_deadline_tracker_lookup(&t, 8) == NULL);

    assert(dap_deadline_tracker_record_status(&t, 7, "subA", "Pending", NULL) == 0);
    assert(dap_deadline_tracker_record_status(&t, 7, "subB", "Failure", "retention obligation") == 0);
    assert(dap_deadline_tracker_record_status(&t, 7, "subX", "Success", NULL) != 0);

    const struct dap_tracked_op *op = dap_deadline_tracker_lookup(&t, 7);
    assert(op != NULL && op->deadline == 100 && !strcmp(op->publisher_id, "pub1"));
    assert(!expected(op, "subA")->responded && !strcmp(expected(op, "subA")->status, "Pending"));
    assert(expected(op, "subB")->responded && !strcmp(expected(op, "subB")->status, "Failure")
           && !strcmp(expected(op, "subB")->reason, "retention obligation"));
    assert(!expected(op, "subC")->responded && expected(op, "subC")->status == NULL);
    assert(!dap_deadline_tracker_all_responded(&t, 7));

    /* A later status replaces the earlier one; Pending is not a response. */
    assert(dap_deadline_tracker_record_status(&t, 7, "subA", "Success", NULL) == 0);
    assert(dap_deadline_tracker_record_status(&t, 7, "subC", "Success", NULL) == 0);
    assert(expected(op, "subA")->responded && !strcmp(expected(op, "subA")->status, "Success"));
    assert(dap_deadline_tracker_all_responded(&t, 7));

    dap_deadline_tracker_destroy(&t);
    printf("ok - the latest status and reason of each subscriber are kept\n");
}

/* A settled op stays queryable until its deadline, and says so when it expires. */
static void test_settled_op_expires_quietly(void)
{
    struct dap_deadline_tracker t;
    const char *subs[] = {"subA"};

    dap_deadline_tracker_init(&t);
    dap_deadline_tracker_register_pending_operation(&t, 1, "pub1", subs, 1, 100);
    dap_deadline_tracker_register_pending_operation(&t, 2, "pub1", subs, 1, 100);
    dap_deadline_tracker_record_status(&t, 1, "subA", "Success", NULL);
    assert(dap_deadline_tracker_settle(&t, 1) == 0);
    assert(dap_deadline_tracker_settle(&t, 9) != 0);
    assert(dap_deadline_tracker_lookup(&t, 1)->settled);

    struct dap_expired_op *e = dap_deadline_tracker_check_expired(&t, 100);
    int settled = 0, unsettled = 0;
    for(struct dap_expired_op *x = e; x; x = x->next){
        if(x->settled) settled++; else unsettled++;
    }
    assert(settled == 1 && unsettled == 1);
    dap_deadline_tracker_free_expired(e);
    assert(dap_deadline_tracker_lookup(&t, 1) == NULL);

    dap_deadline_tracker_destroy(&t);
    printf("ok - a settled operation is kept until its deadline and expires as settled\n");
}

int main(void)
{
    test_register_makes_op_tracked();
    test_register_rejects_duplicate_and_bad_args();
    test_mark_response_and_untracked();
    test_no_expiry_before_deadline();
    test_expiry_returns_unresponded_and_removes();
    test_fully_responded_op_expires_with_empty_list();
    test_operations_independent();
    test_destroy_cleans_everything();
    test_all_responded_predicate();
    test_remove_stops_tracking();
    test_record_status();
    test_settled_op_expires_quietly();
    printf("\nAll dap_deadline_tracker tests passed.\n");
    return 0;
}
