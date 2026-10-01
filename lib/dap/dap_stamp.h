/* dap_stamp.h */
#ifndef DAP_STAMP_H
#define DAP_STAMP_H

#include <stdint.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Borrowed types, held by pointer. Forward declared. */
struct dap_subscription_queues;
struct mosquitto__base_msg;

/*
 * Stamp a matched data message for one subscription and append it to that
 * subscription's topic queue, alongside the broker's existing per-client queue.
 *
 * The MP version is looked up by (publisher_id, topic). The SP version is supplied
 * by the caller from the subscription leaf. cmsg_id identifies the client message
 * that was queued for this subscription, so the send-path gate can find its stamp.
 * Pending operations are applied by that gate.
 *
 * msg is borrowed. Returns 0 when the message was enqueued, non-zero on a bad
 * argument or allocation failure.
 */
int dap_stamp_and_enqueue(struct dap_subscription_queues *queues,
                          const char *publisher_id,
                          const char *topic,
                          uint64_t cmsg_id,
                          uint32_t sp_version,
                          struct mosquitto__base_msg *msg,
                          time_t enqueue_time);

#ifdef __cplusplus
}
#endif

#endif /* DAP_STAMP_H */
