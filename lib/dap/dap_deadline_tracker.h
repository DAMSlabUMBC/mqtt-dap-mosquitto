/* dap_deadline_tracker.h */
#ifndef DAP_DEADLINE_TRACKER_H
#define DAP_DEADLINE_TRACKER_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <time.h>

#include "uthash.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * One subscriber the broker expects to acknowledge an operation, with a flag set
 * once its status message arrives. The expected list doubles as the "responded"
 * set: a subscriber is unresponded while responded is false.
 */
struct dap_expected_sub {
    char *sub_id;                  /* owned copy */
    bool responded;
    struct dap_expected_sub *prev; /* utlist DL links */
    struct dap_expected_sub *next;
};

/*
 * One operation awaiting status notifications, keyed by op_id in the uthash map.
 * The deadline is the wall-clock time by which every expected subscriber must
 * have responded; past it the operation is reported as expired.
 */
struct dap_tracked_op {
    uint64_t op_id;                   /* hash key, broker-assigned */
    char *publisher_id;               /* requesting publisher, owned copy */
    struct dap_expected_sub *expected;/* utlist DL list of expected subscribers */
    size_t expected_count;
    time_t deadline;
    UT_hash_handle hh;
};

/* The deadline tracker: every operation currently awaiting status notifications. */
struct dap_deadline_tracker {
    struct dap_tracked_op *operations; /* uthash head: op_id -> tracked op */
};

/*
 * One expired operation handed back by check_expired, carrying the ids of the
 * subscribers that never responded so the caller can build a failure status
 * message for the requesting publisher. unresponded_subs is empty when every
 * expected subscriber responded before the deadline passed.
 */
struct dap_expired_op {
    uint64_t op_id;
    char *publisher_id;          /* owned copy */
    char **unresponded_subs;     /* owned copies of the offending subscriber ids */
    size_t num_unresponded;
    struct dap_expired_op *next; /* singly-linked result list */
};

/* Initialize an empty tracker. Returns 0 on success, non-zero if t is NULL. */
int dap_deadline_tracker_init(struct dap_deadline_tracker *t);

/*
 * Start tracking an operation that is awaiting status notifications. expected_subs
 * is an array of num_expected subscriber ids that must respond before deadline;
 * publisher_id and every subscriber id are copied. Returns 0 on success, non-zero
 * on a bad argument, allocation failure, or if op_id is already tracked.
 */
int dap_deadline_tracker_register_pending_operation(struct dap_deadline_tracker *t,
                                                    uint64_t op_id,
                                                    const char *publisher_id,
                                                    const char *const *expected_subs,
                                                    size_t num_expected,
                                                    time_t deadline);

/*
 * Record that subscriber_id has responded to op_id. Returns 0 if the operation is
 * tracked and the subscriber was expected, non-zero otherwise (including an op_id
 * that is no longer tracked, e.g. a late response the caller handles separately).
 */
int dap_deadline_tracker_mark_subscriber_responded(struct dap_deadline_tracker *t,
                                                    uint64_t op_id,
                                                    const char *subscriber_id);

/* True while op_id is being tracked (between register and expiry). */
bool dap_deadline_tracker_is_tracked(struct dap_deadline_tracker *t, uint64_t op_id);

/*
 * True when op_id is tracked and every expected subscriber has responded. Lets the
 * status path settle an operation as soon as the last response arrives, instead of
 * waiting for the deadline sweep. False for an untracked op.
 */
bool dap_deadline_tracker_all_responded(struct dap_deadline_tracker *t, uint64_t op_id);

/*
 * Stop tracking op_id without going through expiry, freeing it. Used once the status
 * path has settled an operation so the deadline sweep cannot report it again. Returns
 * 0 if it was tracked and removed, non-zero otherwise.
 */
int dap_deadline_tracker_remove(struct dap_deadline_tracker *t, uint64_t op_id);

/*
 * Sweep the tracker for operations whose deadline has been reached (deadline <=
 * now), returning them as a singly-linked list and removing them from tracking.
 * Each returned op carries the ids of its unresponded subscribers (empty when all
 * responded in time). Returns NULL when nothing has expired. Free the returned
 * list with dap_deadline_tracker_free_expired.
 */
struct dap_expired_op *dap_deadline_tracker_check_expired(struct dap_deadline_tracker *t, time_t now);

/* Free a list of expired-op results (the publisher/subscriber id copies and nodes). */
void dap_deadline_tracker_free_expired(struct dap_expired_op *list);

/* Free every tracked operation, leaving the tracker empty and reusable. */
void dap_deadline_tracker_destroy(struct dap_deadline_tracker *t);

#ifdef __cplusplus
}
#endif

#endif /* DAP_DEADLINE_TRACKER_H */
