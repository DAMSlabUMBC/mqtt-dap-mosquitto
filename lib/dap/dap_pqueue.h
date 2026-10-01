#ifndef DAP_PQUEUE_H
#define DAP_PQUEUE_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Two-level priority queue for incoming DAP packets. High-priority packets
 * drain before low-priority ones; within a level the order is FIFO. Each level
 * is a doubly-linked list built with the utlist DL_* macros.
 *
 * Packets are stored by pointer and never inspected. The queue owns the link
 * nodes, not the packets.
 */

/* Incoming packet type. Forward declared so only the pointer is needed. */
struct mosquitto__packet_in;

/* Priority a packet is queued at. */
enum dap_pq_priority {
    DAP_PQ_PRIORITY_LOW = 0,
    DAP_PQ_PRIORITY_HIGH = 1,
};

/* One queued packet. The prev/next links are named for the utlist DL_* macros. */
struct dap_pq_node {
    struct mosquitto__packet_in *packet;
    struct dap_pq_node *prev;
    struct dap_pq_node *next;
};

/* Caller-allocated queue. The two levels are kept as separate lists. */
struct dap_pqueue {
    struct dap_pq_node *high_head;
    struct dap_pq_node *low_head;
    size_t high_count;
    size_t low_count;
};

/* Initialise a queue to the empty state. Returns 0 on success, non-zero if q is NULL. */
int dap_pqueue__init(struct dap_pqueue *q);

/*
 * Append a packet to the FIFO list for the given priority. The pointer is stored
 * as-is; the queue does not copy or take ownership of the packet.
 * Returns 0 on success, non-zero on a bad argument or allocation failure.
 */
int dap_pqueue__enqueue(struct dap_pqueue *q, struct mosquitto__packet_in *packet, enum dap_pq_priority priority);

/*
 * Remove and return the next packet, draining the high-priority list first.
 * Returns NULL when the queue is empty.
 */
struct mosquitto__packet_in *dap_pqueue__dequeue(struct dap_pqueue *q);

/* Return the packet dequeue would yield next, without removing it. NULL if empty. */
struct mosquitto__packet_in *dap_pqueue__peek(struct dap_pqueue *q);

/* Return the total number of packets queued across both levels. */
size_t dap_pqueue__size(struct dap_pqueue *q);

/*
 * Free every remaining node and reset the queue to empty. The queued packets are
 * left untouched, since the queue never owned them.
 */
void dap_pqueue__destroy(struct dap_pqueue *q);

#ifdef __cplusplus
}
#endif

#endif /* DAP_PQUEUE_H */
