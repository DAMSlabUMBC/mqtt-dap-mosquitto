/* dap_send_verify.h */
#ifndef DAP_SEND_VERIFY_H
#define DAP_SEND_VERIFY_H

#include <stdint.h>
#include <stdbool.h>

#include "dap_pending_ops.h" /* enum dap_op_action */

#ifdef __cplusplus
extern "C" {
#endif

/* Defined in dap_subscription_queues.h; only a pointer is needed here. */
struct dap_stamped_msg;

/*
 * The send-time verdict for one stamped message at the front of a subscription's
 * topic queue, just before it goes on the wire.
 */
enum dap_send_verdict {
    DAP_SEND_PASS = 0,        /* deliver the message as stamped */
    DAP_SEND_FAIL_MP,         /* MP version changed since stamping - re-verify */
    DAP_SEND_FAIL_SP,         /* SP version changed since stamping - re-verify */
    DAP_SEND_FAIL_OP_MISSING, /* a RESTRICT now applies that the stamp does not cover - re-verify */
    DAP_SEND_DROP_DELETE,     /* a DELETE now applies - drop without delivering */
    DAP_SEND_DROP_PURPOSE,    /* re-verified: the current SP no longer admits the purpose - drop */
};

/*
 * Decide what to do with a stamped message at send time. The versions it was
 * stamped with are compared against the current MP and SP versions and against
 * the pending operation, if any, that applies right now. The caller computes
 * current_op_action / current_op_id via dap_pending_ops_match; this function does
 * no lookups and has no side effects.
 *
 * Precedence: a pending DELETE (DAP_OP_ACTION_DROP) drops the message regardless
 * of version; otherwise a stale MP or SP version forces re-verification (MP before
 * SP); finally a pending RESTRICT passes only when its op id is already stamped,
 * else re-verification re-stamps it under the current op. DAP_OP_ACTION_NONE with
 * matching versions passes even if the message carries older applied op ids.
 *
 * A NULL stamp (e.g. a retained message that was never stamped) always passes.
 */
enum dap_send_verdict dap_verify_for_send(const struct dap_stamped_msg *stamped,
                                          uint32_t current_mp_version,
                                          uint32_t current_sp_version,
                                          enum dap_op_action current_op_action,
                                          uint64_t current_op_id);

/*
 * What the send-path hook should do with the message it is about to write, once
 * the verdict above is known and the client's hold state is consulted.
 */
enum dap_send_disposition {
    DAP_DISP_DELIVER = 0, /* send the message normally (the caller removes any stamp first) */
    DAP_DISP_DROP,        /* a DELETE applies - drop without sending */
    DAP_DISP_BUMP,        /* version/op changed - bump back for re-verification, start holding */
    DAP_DISP_SKIP,        /* the client is holding a different message - leave this one queued, skip this pass */
};

/*
 * Map the send-path state to a disposition. Pure - no side effects.
 *
 *   has_stamp   - whether a matching DAP stamp was found for this message; when
 *                 false (retained / will / non-DAP) the message is always DELIVERed
 *                 and the remaining arguments are ignored.
 *   is_holding  - whether this message's client is currently re-verifying a message.
 *   pending_id  - the cmsg_id of that re-verify candidate (meaningful only when holding).
 *   this_id     - the cmsg_id of the message being written now.
 *   verdict     - dap_verify_for_send's result for this message. For the holding
 *                 candidate the caller passes the re-verification result (PASS, or a
 *                 DROP_* when a DELETE applies or the current SP no longer admits the
 *                 message's purpose), so the candidate terminates in DELIVER or DROP.
 *
 * Not holding: PASS->DELIVER, DROP_*->DROP, any FAIL_*->BUMP. Holding and this is
 * the candidate (this_id == pending_id): DROP_*->DROP, otherwise DELIVER (never BUMP
 * again - that would spin). Holding and this is some other message: SKIP.
 */
enum dap_send_disposition dap_send_decide(bool has_stamp,
                                          bool is_holding,
                                          uint64_t pending_id,
                                          uint64_t this_id,
                                          enum dap_send_verdict verdict);

#ifdef __cplusplus
}
#endif

#endif /* DAP_SEND_VERIFY_H */
