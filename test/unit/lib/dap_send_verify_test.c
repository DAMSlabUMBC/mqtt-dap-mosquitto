/* Unit test for the §6 Case 3 send-time verdict predicate (lib/dap_send_verify.c).
 *
 * dap_verify_for_send is a pure function: it inspects a stamped message and the
 * caller-supplied current MP/SP versions plus the pending-op action/id that
 * dap_pending_ops_match decided, and returns a verdict. No registries, queues or
 * maps are touched, so the test builds a dap_stamped_msg on the stack and calls
 * the predicate directly. Build and run on its own (add the sanitizers and
 * -Werror to match CI):
 *
 *   cc -I../../.. -I../../../lib -I../../../include -I../../../libcommon \
 *      -I../../../src -I../../../common -I../../../deps -I/opt/homebrew/include \
 *      -Wall -Wextra -Werror -fsanitize=address,undefined \
 *      dap_send_verify_test.c ../../../lib/dap_send_verify.c \
 *      -o dap_send_verify_test && ./dap_send_verify_test
 */

#include <assert.h>
#include <stdio.h>

#include "dap_send_verify.h"
#include "dap_subscription_queues.h"
#include "dap_pending_ops.h"

/* Distinct non-NULL stand-in for a stored message; its contents are never read. */
static char msg_slot[8];
#define MSG ((struct mosquitto__base_msg *)&msg_slot[0])

/* Build a stamped message on the stack with the given versions. */
static struct dap_stamped_msg make_stamp(uint32_t mp, uint32_t sp)
{
    struct dap_stamped_msg m;
    m.base_msg = MSG;
    m.mp_version = mp;
    m.sp_version = sp;
    m.enqueue_time = 0;
    m.prev = NULL;
    m.next = NULL;
    return m;
}

/* A retained message arrives with no stamp at all; it always passes. */
static void test_null_stamp_passes(void)
{
    assert(dap_verify_for_send(NULL, 5, 7, DAP_OP_ACTION_NONE, false, "test", "test_client") == DAP_SEND_PASS);
    printf("ok - a NULL stamp (retained message) passes\n");
}

/* Versions match and no op applies: deliver as stamped. */
static void test_all_match_passes(void)
{
    struct dap_stamped_msg m = make_stamp(3, 4);
    assert(dap_verify_for_send(&m, 3, 4, DAP_OP_ACTION_NONE, false, "test", "test_client") == DAP_SEND_PASS);
    printf("ok - matching versions with no pending op passes\n");
}

/* The MP version moved on since stamping: re-verify. */
static void test_mp_mismatch_fails(void)
{
    struct dap_stamped_msg m = make_stamp(3, 4);
    assert(dap_verify_for_send(&m, 9, 4, DAP_OP_ACTION_NONE, false, "test", "test_client") == DAP_SEND_FAIL_MP_VER);
    printf("ok - a stale MP version returns FAIL_MP\n");
}

/* The SP version moved on since stamping (MP still matches): re-verify. */
static void test_sp_mismatch_fails(void)
{
    struct dap_stamped_msg m = make_stamp(3, 4);
    assert(dap_verify_for_send(&m, 3, 9, DAP_OP_ACTION_NONE, false, "test", "test_client") == DAP_SEND_FAIL_SP_VER);
    printf("ok - a stale SP version returns FAIL_SP\n");
}

/* A RESTRICT applies: re-verify against the purposes it revoked. */
static void test_restrict_reverifies(void)
{
    struct dap_stamped_msg m = make_stamp(3, 4);
    assert(dap_verify_for_send(&m, 3, 4, DAP_OP_ACTION_RESTRICT, false, "test", "test_client") == DAP_SEND_FAIL_RESTRICT);
    printf("ok - a pending RESTRICT returns FAIL_RESTRICT\n");
}

/* A DELETE applies: drop the message without delivering it. */
static void test_delete_drops(void)
{
    struct dap_stamped_msg m = make_stamp(3, 4);
    assert(dap_verify_for_send(&m, 3, 4, DAP_OP_ACTION_DROP, false, "test", "test_client") == DAP_SEND_DROP_DELETE);
    printf("ok - a pending DELETE returns DROP_DELETE\n");
}

/* A DELETE outranks a stale version: it drops rather than asking to re-verify. */
static void test_delete_precedes_version_mismatch(void)
{
    struct dap_stamped_msg m = make_stamp(3, 4);
    assert(dap_verify_for_send(&m, 9, 9, DAP_OP_ACTION_DROP, false, "test", "test_client") == DAP_SEND_DROP_DELETE);
    printf("ok - a DELETE outranks a stale MP/SP version\n");
}

/* Operations are checked for topic. */
static void test_op_topic(void)
{
    struct dap_stamped_msg m = make_stamp(3, 4);
    assert(dap_verify_for_send(&m, 3, 4, DAP_OP_ACTION_NONE, true, "OP_NOTIF/test_client", "test_client") == DAP_SEND_PASS);
    assert(dap_verify_for_send(&m, 3, 4, DAP_OP_ACTION_NONE, true, "OP_NOTIF/test_client2", "test_client") == DAP_SEND_DROP_INVALID_OP_TOPIC);
    printf("ok - operations are correctly checked for topic\n");
}

/* ---- dap_send_decide: the send-path disposition mapping ---- */

/* No stamp (retained / will / non-DAP) is always delivered; the rest is ignored. */
static void test_decide_no_stamp_delivers(void)
{
    /* Even with a holding client and a FAIL verdict, no stamp means just deliver. */
    assert(dap_send_decide(false, true, 7, 7, DAP_SEND_FAIL_MP_VER, true) == DAP_DISP_DELIVER);
    printf("ok - decide: no stamp always delivers\n");
}

/* Not holding: a clean PASS delivers. */
static void test_decide_pass_delivers(void)
{
    assert(dap_send_decide(true, false, 0, 11, DAP_SEND_PASS, true) == DAP_DISP_DELIVER);
    printf("ok - decide: PASS while not holding delivers\n");
}

/* Not holding: a DELETE drops. */
static void test_decide_delete_drops(void)
{
    assert(dap_send_decide(true, false, 0, 11, DAP_SEND_DROP_DELETE, true) == DAP_DISP_DROP);
    printf("ok - decide: DELETE while not holding drops\n");
}

/* Not holding: every version/op failure bumps the message back for re-verification if full check
                is not to be performed. */
static void test_decide_failures_bump(void)
{
    assert(dap_send_decide(true, false, 0, 11, DAP_SEND_FAIL_MP_VER, false) == DAP_DISP_BUMP);
    assert(dap_send_decide(true, false, 0, 11, DAP_SEND_FAIL_SP_VER, false) == DAP_DISP_BUMP);
    assert(dap_send_decide(true, false, 0, 11, DAP_SEND_FAIL_RESTRICT, false) == DAP_DISP_BUMP);
    printf("ok - decide: FAIL_MP/SP/OP_MISSING while not holding bump\n");
}

/* Not holding: every version/op failure notifies for r re-verification if full check
                is expected to be performed. */
static void test_decide_failures_recalculate(void)
{
    assert(dap_send_decide(true, false, 0, 11, DAP_SEND_FAIL_MP_VER, true) == DAP_DISP_RECALCULATE);
    assert(dap_send_decide(true, false, 0, 11, DAP_SEND_FAIL_SP_VER, true) == DAP_DISP_RECALCULATE);
    assert(dap_send_decide(true, false, 0, 11, DAP_SEND_FAIL_RESTRICT, true) == DAP_DISP_RECALCULATE);
    printf("ok - decide: FAIL_MP/SP/OP_MISSING while not holding bump\n");
}

/* Holding, and this is the re-verify candidate (this_id == pending_id): a PASS (the
 * usual post-re-stamp outcome) delivers and ends the hold. */
static void test_decide_candidate_pass_delivers(void)
{
    assert(dap_send_decide(true, true, 11, 11, DAP_SEND_PASS, true) == DAP_DISP_DELIVER);
    printf("ok - decide: holding candidate that passes delivers\n");
}

/* Holding candidate: a DELETE that arrived during the hold drops it. */
static void test_decide_candidate_delete_drops(void)
{
    assert(dap_send_decide(true, true, 11, 11, DAP_SEND_DROP_DELETE, true) == DAP_DISP_DROP);
    printf("ok - decide: holding candidate hit by a DELETE drops\n");
}

/* Holding candidate whose purpose the subscription's current SP no longer admits drops. */
static void test_decide_candidate_purpose_drops(void)
{
    assert(dap_send_decide(true, true, 11, 11, DAP_SEND_DROP_INVALID_OP_TOPIC, true) == DAP_DISP_DROP);
    assert(dap_send_decide(true, false, 0, 11, DAP_SEND_DROP_INVALID_OP_TOPIC, true) == DAP_DISP_DROP);
    printf("ok - decide: a purpose the current SP no longer admits drops\n");
}

/* Holding, but this is some other message (this_id != pending_id): skip it this pass
 * so the candidate cannot be overtaken unless it is a drop. */
static void test_decide_non_candidate_skips_if_not_drop(void)
{
    assert(dap_send_decide(true, true, 11, 99, DAP_SEND_PASS, true) == DAP_DISP_SKIP);
    /* Ids that share their low 16 bits (two QoS 0 messages both carry mid 0) are distinct. */
    assert(dap_send_decide(true, true, 0x10000, 0x20000, DAP_SEND_PASS, true) == DAP_DISP_SKIP);
    printf("ok - decide: holding a different message skips this one\n");
}

/* Holding, but this is some other message (this_id != pending_id): allow drops. */
static void test_decide_non_candidate_drops_if_drop(void)
{
    assert(dap_send_decide(true, true, 11, 99, DAP_SEND_DROP_DELETE, true) == DAP_DISP_DROP);
    /* Ids that share their low 16 bits (two QoS 0 messages both carry mid 0) are distinct. */
}

int main(void)
{
    test_null_stamp_passes();
    test_all_match_passes();
    test_mp_mismatch_fails();
    test_sp_mismatch_fails();
    test_restrict_reverifies();
    test_delete_drops();
    test_delete_precedes_version_mismatch();
    test_op_topic();
    test_decide_no_stamp_delivers();
    test_decide_pass_delivers();
    test_decide_delete_drops();
    test_decide_failures_bump();
    test_decide_failures_recalculate();
    test_decide_candidate_pass_delivers();
    test_decide_candidate_delete_drops();
    test_decide_candidate_purpose_drops();
    test_decide_non_candidate_skips_if_not_drop();
    test_decide_non_candidate_drops_if_drop();
    printf("\nAll dap_send_verify tests passed.\n");
    return 0;
}
