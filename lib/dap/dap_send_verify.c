/* dap_send_verify.c */
#include "dap_send_verify.h"

#include <stdbool.h>

#include "dap_subscription_queues.h" /* struct dap_stamped_msg */

enum dap_send_verdict dap_verify_for_send(const struct dap_stamped_msg *stamped,
                                          uint32_t current_mp_version,
                                          uint32_t current_sp_version,
                                          enum dap_op_action current_op_action)
{
    /* A retained message that was never stamped flows through untouched. */
    if (stamped == NULL) {
        return DAP_SEND_PASS;
    }

    /* A DELETE drops the message outright, regardless of version freshness. */
    if (current_op_action == DAP_OP_ACTION_DROP) {
        return DAP_SEND_DROP_DELETE;
    }

    /* A stale version means policy changed since stamping: re-verify (MP first). */
    if (stamped->mp_version != current_mp_version) {
        return DAP_SEND_FAIL_MP;
    }
    if (stamped->sp_version != current_sp_version) {
        return DAP_SEND_FAIL_SP;
    }

    /* A RESTRICT re-verifies the message against the purposes it revoked. */
    if (current_op_action == DAP_OP_ACTION_RESTRICT) {
        return DAP_SEND_FAIL_RESTRICT;
    }

    return DAP_SEND_PASS;
}

enum dap_send_disposition dap_send_decide(bool has_stamp,
                                          bool is_holding,
                                          uint64_t pending_id,
                                          uint64_t this_id,
                                          enum dap_send_verdict verdict)
{
    /* No stamp: retained / will / non-DAP message, deliver untouched. */
    if (!has_stamp) {
        return DAP_DISP_DELIVER;
    }

    if (is_holding) {
        /* A different message must wait so the candidate keeps its place. */
        if (this_id != pending_id) {
            return DAP_DISP_SKIP;
        }
        /* The re-verify candidate terminates this pass: a DELETE, or a current MP
         * that no longer permits the current SP, drops it, otherwise it delivers. It
         * must never bump again, which would spin. */
        return (verdict == DAP_SEND_DROP_DELETE || verdict == DAP_SEND_DROP_PURPOSE)
                ? DAP_DISP_DROP : DAP_DISP_DELIVER;
    }

    /* Not holding: act on the verdict directly. */
    switch (verdict) {
        case DAP_SEND_PASS:
            return DAP_DISP_DELIVER;
        case DAP_SEND_DROP_DELETE:
        case DAP_SEND_DROP_PURPOSE:
            return DAP_DISP_DROP;
        case DAP_SEND_FAIL_MP:
        case DAP_SEND_FAIL_SP:
        case DAP_SEND_FAIL_RESTRICT:
            return DAP_DISP_BUMP;
    }
    return DAP_DISP_DELIVER; /* unreachable; keeps the compiler happy */
}
