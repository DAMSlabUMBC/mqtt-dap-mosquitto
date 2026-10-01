/* dap_stamp.c */

#include "dap_stamp.h"
#include "mp_registry.h"
#include "dap_subscription_queues.h"

int dap_stamp_and_enqueue(struct dap_subscription_queues *queues,
                          const char *publisher_id,
                          const char *topic,
                          uint64_t cmsg_id,
                          uint32_t sp_version,
                          struct mosquitto__base_msg *msg,
                          time_t enqueue_time)
{
    if(!queues) return 1;

    /* MP version is keyed by (publisher, topic); an unregistered topic looks up as 0. */
    uint32_t mp_version = 0;
    struct mp_entry *stored = mp__lookup(publisher_id, topic);
    if(stored)
    {
        mp_version = stored->version;
    }

    return dap_subscription_queues_enqueue(queues, topic, msg, cmsg_id, mp_version, sp_version,
                                           enqueue_time);
}
