/* dap_subscription_queues.h */
#ifndef DAP_SUBSCRIPTION_QUEUES_H
#define DAP_SUBSCRIPTION_QUEUES_H

#include <stddef.h>
#include <stdint.h>
#include <time.h>

#include "uthash.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Stored message, held by pointer and never inspected here. Forward declared. */
struct mosquitto__base_msg;

/*
 * A data message stamped at enqueue time so the send-time check can tell whether
 * anything changed before delivery. The queue owns this wrapper, not the
 * underlying message.
 */
struct dap_stamped_msg {
    struct mosquitto__base_msg *base_msg; /* stored message, borrowed not owned */
    uint64_t cmsg_id;                    /* cmsg_id of the client message it stamps */
    uint32_t mp_version;                 /* MP version for the publisher/topic pair */
    uint32_t sp_version;                 /* SP version for this subscription/topic pair */
    time_t enqueue_time;                 /* when the message was enqueued */
    struct dap_stamped_msg *prev;        /* utlist DL links */
    struct dap_stamped_msg *next;
};

/* One topic's FIFO queue within a subscription, keyed by topic in the uthash map. */
struct dap_topic_queue {
    char *topic;                  /* hash key */
    struct dap_stamped_msg *head; /* utlist DL list, front == head */
    size_t count;
    UT_hash_handle hh;
};

/* One subscription's set of per-topic queues. */
struct dap_subscription_queues {
    struct dap_topic_queue *topics; /* uthash head: topic -> queue */
    size_t total_count;             /* messages across all topic queues */
};

/* Initialize an empty set of queues. Returns 0 on success, non-zero if q is NULL. */
int dap_subscription_queues_init(struct dap_subscription_queues *q);

/*
 * Stamp a message and append it to the FIFO queue for topic (created on first
 * use). The base_msg pointer is stored as-is. Returns 0 on success, non-zero on a
 * bad argument or allocation failure.
 */
int dap_subscription_queues_enqueue(struct dap_subscription_queues *q,
                                    const char *topic,
                                    struct mosquitto__base_msg *base_msg,
                                    uint64_t cmsg_id,
                                    uint32_t mp_version,
                                    uint32_t sp_version,
                                    time_t enqueue_time);

/* Return the front message of a topic queue without removing it, or NULL if empty. */
struct dap_stamped_msg *dap_subscription_queues_peek_front(struct dap_subscription_queues *q,
                                                           const char *topic);

/*
 * Remove and return the front message of a topic queue, or NULL if empty. The
 * caller takes ownership and must either push it back or free it with
 * dap_stamped_msg_free.
 */
struct dap_stamped_msg *dap_subscription_queues_dequeue_front(struct dap_subscription_queues *q,
                                                              const char *topic);

/*
 * Find the stamp for a client message (by base_msg and cmsg_id) anywhere in a topic
 * queue, or NULL if it has none.
 */
struct dap_stamped_msg *dap_subscription_queues_find(struct dap_subscription_queues *q,
                                                     const char *topic,
                                                     const struct mosquitto__base_msg *base_msg,
                                                     uint64_t cmsg_id);

/* Unlink a stamp found in a topic queue. The caller takes ownership. */
void dap_subscription_queues_remove(struct dap_subscription_queues *q,
                                    const char *topic,
                                    struct dap_stamped_msg *msg);

/*
 * Re-insert a message at the front of a topic queue (created on first use),
 * handing ownership back to the queue. Used by the holding-list mechanism.
 * Returns 0 on success, non-zero on a bad argument or allocation failure.
 */
int dap_subscription_queues_push_front(struct dap_subscription_queues *q,
                                       const char *topic,
                                       struct dap_stamped_msg *msg);

/* Number of messages queued for a topic (0 if the topic has no queue). */
size_t dap_subscription_queues_topic_size(struct dap_subscription_queues *q, const char *topic);

/* Total number of messages across all of this subscription's topic queues. */
size_t dap_subscription_queues_total_size(struct dap_subscription_queues *q);

/* Free a stamped message (its op-id array and the wrapper), but not the base_msg. */
void dap_stamped_msg_free(struct dap_stamped_msg *msg);

/* Free every topic queue and stamped message, leaving q empty and reusable. */
void dap_subscription_queues_destroy(struct dap_subscription_queues *q);

#ifdef __cplusplus
}
#endif

#endif /* DAP_SUBSCRIPTION_QUEUES_H */
