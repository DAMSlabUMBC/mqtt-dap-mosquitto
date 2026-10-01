/* dap_stamp.c */

#include "dap_stamp.h"
#include "mp_registry.h"
#include "dap_subscription_queues.h"
#include "dap_pending_ops.h"

int dap_stamp_and_enqueue(struct dap_subscription_queues *queues,
                          struct dap_pending_ops *pending_ops,
                          const char *publisher_id,
                          const char *subscriber_id,
                          const char *topic,
                          uint64_t cmsg_id,
                          uint32_t sp_version,
                          const char *purpose,
                          struct mosquitto__base_msg *msg,
                          time_t enqueue_time,
                          enum dap_op_action *action_out)
{
    if(!queues) return 1;

    /* MP version is keyed by (publisher, topic); an unregistered topic looks up as 0. */
    uint32_t mp_version = 0;
    struct mp_entry *stored = mp__lookup(publisher_id, topic);
    if(stored)
    {
        mp_version = stored->version;
    }

    /* Consult the pending-op map for this publisher. A NULL map yields NONE. */
    uint64_t op_id = 0;
    enum dap_op_action action = dap_pending_ops_match(pending_ops, publisher_id, topic,
                                                      purpose, subscriber_id, enqueue_time,
                                                      &op_id);
    if(action_out) *action_out = action;

    /* A RESTRICT stamps the message with the single deciding op id; NONE and DROP
     * leave the applied-op list empty. The send-path gate re-checks pending ops and
     * drops on DROP from there. */
    if(action == DAP_OP_ACTION_RESTRICT){
        return dap_subscription_queues_enqueue(queues, topic, msg, cmsg_id, mp_version, sp_version,
                                               &op_id, 1, enqueue_time);
    }
    return dap_subscription_queues_enqueue(queues, topic, msg, cmsg_id, mp_version, sp_version,
                                           NULL, 0, enqueue_time);
}
