/* dap_pending_ops.h */
#ifndef DAP_PENDING_OPS_H
#define DAP_PENDING_OPS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>

#include "uthash.h"

#ifdef __cplusplus
extern "C" {
#endif

/* The broker-enforced operation a publisher has invoked. DELETE drops a matching
 * message outright; RESTRICT flags it so delivery can be held back later. */
enum dap_op_type {
    DAP_OP_DELETE = 0,
    DAP_OP_RESTRICT = 1,
};

/* What the match helper decided for a single message. */
enum dap_op_action {
    DAP_OP_ACTION_NONE = 0, /* no pending operation applies */
    DAP_OP_ACTION_DROP,     /* a DELETE applies - do not deliver */
    DAP_OP_ACTION_RESTRICT, /* a RESTRICT applies - op_id_out names which one */
};

/*
 * A single pending operation for one publisher. It applies to a message queued for
 * a subscription when the message's topic matches one of its DAP-OpTFs (MQTT topic
 * filters), the subscription's SP shares a purpose with its DAP-OpPFs, and the
 * subscriber is one of its DAP-OpClients. An absent filter, or "*", matches anything.
 * An operation only applies to messages from the publisher that invoked it.
 */
struct dap_pending_op {
    uint64_t op_id;              /* broker-assigned, unique within the map */
    uint64_t order;              /* broker receipt order of the operation request */
    time_t deadline;             /* reclaimed once this passes; 0 = never */
    enum dap_op_type type;
    char **topic_filters;        /* DAP-OpTFs; any element may match, "*" = any */
    size_t num_topic_filters;
    char *purposes;              /* DAP-OpPFs as a canonical purpose set; NULL = any */
    char **subscriber_filters;   /* DAP-OpClients; any element may match, "*" = any */
    size_t num_subscriber_filters;
    struct dap_pending_op *next; /* next op for the same publisher */
};

/* Per-publisher hash entry. The publisher id keys the map and owns the op list. */
struct dap_pub_entry {
    char *pub_id;               /* hash key */
    struct dap_pending_op *ops; /* head of this publisher's op list */
    uint64_t max_order;         /* the latest op's order: newer messages skip the list */
    UT_hash_handle hh;
};

/* The pending-operation map, keyed by publisher id. */
struct dap_pending_ops {
    struct dap_pub_entry *publishers; /* uthash head */
    uint64_t next_op_id;              /* hands out op ids, starts at 1 */
};

/* Initialize an empty map. Returns 0 on success, non-zero if map is NULL. */
int dap_pending_ops_init(struct dap_pending_ops *map);

/*
 * Insert a pending operation for pub_id. topic_filters and subscriber_filters are
 * comma-separated lists, and purpose_filters a purpose filter collection. Pass "*"
 * or NULL for a filter that matches anything. The strings are copied. The operation
 * is kept until dap_pending_ops_remove_expired finds its deadline passed (0 = never).
 * The assigned operation id is written to *op_id_out when non-NULL. Returns 0 on
 * success, non-zero on a bad argument, an invalid filter or allocation failure.
 */
int dap_pending_ops_insert_operation(struct dap_pending_ops *map,
                                     const char *pub_id,
                                     enum dap_op_type type,
                                     uint64_t order,
                                     time_t deadline,
                                     const char *topic_filter,
                                     const char *purpose_filter,
                                     const char *subscriber_filter,
                                     uint64_t *op_id_out);

/*
 * Hand out the next broker-assigned op id from the same counter insert uses, without
 * storing anything. For subscriber-involving operations that need a unique id and a
 * deadline-tracker entry but no in-flight match entry (HISTORY/UPDATE). Returns 0 if
 * map is NULL.
 */
uint64_t dap_pending_ops_allocate_op_id(struct dap_pending_ops *map);

/* Insert a restored op with its original id. Fails on id 0 or a duplicate id. */
int dap_pending_ops_restore_operation(struct dap_pending_ops *map,
                                      uint64_t op_id,
                                      const char *pub_id,
                                      enum dap_op_type type,
                                      uint64_t order,
                                      time_t deadline,
                                      const char *topic_filter,
                                      const char *purpose_filter,
                                      const char *subscriber_filter);

/* Keep the id counter above op_id. */
void dap_pending_ops_reserve_op_id(struct dap_pending_ops *map, uint64_t op_id);

/* Returns the head of the pending-operation list for pub_id, or NULL if none. */
struct dap_pending_op *dap_pending_ops_lookup_operations_for_publisher(struct dap_pending_ops *map,
                                                                       const char *pub_id);

/*
 * Remove the operation with the given op_id. Returns 0 if it was found and
 * removed, non-zero otherwise. The publisher entry is dropped once its last
 * operation is gone.
 */
int dap_pending_ops_remove_operation_by_id(struct dap_pending_ops *map, uint64_t op_id);

/* True when an operation of pub_id covers a message it stored earlier on topic under
 * MP mp (NULL = none): the topic matches the operation's DAP-OpTFs, and its DAP-OpPFs,
 * if any, share a purpose with mp. DAP-OpClients is not considered, since a retained
 * message has no subscriber yet. */
bool dap_pending_ops_cover_stored(struct dap_pending_ops *map, const char *pub_id,
                                  const char *topic, const char *mp, uint64_t msg_order);

/* True when some operation's deadline is at or before now. */
bool dap_pending_ops_any_expired(struct dap_pending_ops *map, time_t now);

/* Remove every operation whose deadline is at or before now (paper 6.3: operation
 * state is reclaimed once its deadline elapses), calling removed for each. */
void dap_pending_ops_remove_expired(struct dap_pending_ops *map, time_t now,
                                    void (*removed)(uint64_t op_id, void *arg), void *arg);

/* Free every entry and operation, leaving the map empty and reusable. */
void dap_pending_ops_destroy(struct dap_pending_ops *map);

/*
 * Decide what happens to a message from pub_id queued for a subscription with the
 * sorted purpose set sp (NULL when the subscription is unknown, which every purpose
 * filter matches). An operation applies when each of its filters matches and the
 * broker received the message before the operation (msg_order <= op->order).
 * DELETE supersedes RESTRICT: any applicable DELETE gives DAP_OP_ACTION_DROP,
 * otherwise the most recent applicable RESTRICT wins. The deciding op id is written
 * to *op_id_out for DROP and RESTRICT, or 0 for NONE. For RESTRICT, *revoked_out is
 * set to the canonical purposes it revokes ("*" for all), and to NULL otherwise.
 */
enum dap_op_action dap_pending_ops_match(struct dap_pending_ops *map,
                                         const char *pub_id,
                                         const char *topic,
                                         char *const *sp,
                                         uint32_t sp_count,
                                         const char *subscriber_id,
                                         uint64_t msg_order,
                                         uint64_t *op_id_out,
                                         const char **revoked_out);

#ifdef __cplusplus
}
#endif

#endif /* DAP_PENDING_OPS_H */
