/* dap_holding_list.h */
#ifndef DAP_HOLDING_LIST_H
#define DAP_HOLDING_LIST_H

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

#include "uthash.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Stamped message owned by the subscription queues; held here by pointer. Forward declared. */
struct dap_stamped_msg;

/*
 * One message parked in a subscription's hold list while another message from the
 * same subscription is being re-verified. The wrapper is owned by the holding
 * list; the stamped message it points at is borrowed (it is going back into a
 * topic queue once the hold is flushed).
 */
struct dap_held_msg {
    struct dap_stamped_msg *msg; /* borrowed, not owned */
    struct dap_held_msg *prev;   /* utlist DL links */
    struct dap_held_msg *next;
};

/* The hold state for one subscription, keyed by subscription id in the uthash map. */
struct dap_holding_entry {
    char *sub_id;              /* hash key */
    uint64_t pending_id;       /* cmsg_id of the bumped message this subscription is re-verifying */
    struct dap_held_msg *head; /* utlist DL FIFO, front == head */
    size_t count;
    UT_hash_handle hh;
};

/* Holding lists for every subscription currently re-verifying a bumped message. */
struct dap_holding_list {
    struct dap_holding_entry *subscriptions; /* uthash head: sub_id -> hold state */
};

/* Initialize an empty holding list. Returns 0 on success, non-zero if h is NULL. */
int dap_holding_list_init(struct dap_holding_list *h);

/*
 * Mark a subscription as holding, called when one of its messages is bumped back
 * for re-verification. pending_id is that client message's cmsg_id; the send-path
 * hook uses it to tell the re-verify candidate from the messages it must park.
 * Idempotent: re-starting an already-holding subscription keeps its held messages
 * and original pending_id. Returns 0 on success, non-zero on a bad argument or
 * allocation failure.
 */
int dap_holding_list_start_holding(struct dap_holding_list *h, const char *sub_id,
                                   uint64_t pending_id);

/* True while sub_id is holding (between start_holding and flush). */
bool dap_holding_list_is_holding(struct dap_holding_list *h, const char *sub_id);

/* The pending re-verify cmsg_id recorded for sub_id at start_holding time, or 0
 * when sub_id is not holding. */
uint64_t dap_holding_list_pending_id(struct dap_holding_list *h, const char *sub_id);

/*
 * Append a message to a subscription's hold list, preserving arrival order. The
 * message is borrowed, not copied. The subscription must already be holding.
 * Returns 0 on success, non-zero if the subscription is not holding, on a bad
 * argument, or on allocation failure.
 */
int dap_holding_list_add_held(struct dap_holding_list *h, const char *sub_id,
                              struct dap_stamped_msg *msg);

/*
 * Stop holding sub_id and hand back its held messages as a FIFO linked list (front
 * first), transferring ownership of the wrapper nodes to the caller. The stamped
 * messages they point at are still borrowed. Returns NULL if the subscription was
 * not holding or held nothing; either way the holding state is cleared. Free the
 * returned wrappers with dap_holding_list_free_held.
 */
struct dap_held_msg *dap_holding_list_flush(struct dap_holding_list *h, const char *sub_id);

/* Free a list of held-message wrappers, but not the stamped messages they point at. */
void dap_holding_list_free_held(struct dap_held_msg *list);

/* Free every subscription's hold state and wrappers, leaving h empty and reusable. */
void dap_holding_list_destroy(struct dap_holding_list *h);

#ifdef __cplusplus
}
#endif

#endif /* DAP_HOLDING_LIST_H */
