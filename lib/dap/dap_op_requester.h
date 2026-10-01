/* dap_op_requester.h */
#ifndef DAP_OP_REQUESTER_H
#define DAP_OP_REQUESTER_H

#include <stdint.h>

#include "uthash.h"

#ifdef __cplusplus
extern "C" {
#endif

/* One op-id -> requesting-publisher mapping. */
struct dap_op_requester_entry {
    uint64_t op_id;          /* hash key */
    char *requester_id;      /* owned copy */
    UT_hash_handle hh;
};

/*
 * Maps a broker-assigned op id to the publisher that requested it. Kept for the life
 * of the broker, separate from the deadline tracker, so status notifications - including
 * late ones that arrive after an operation's deadline has passed and it is no longer
 * tracked - can still be forwarded to the original requester's ONP.
 */
struct dap_op_requester {
    struct dap_op_requester_entry *entries; /* uthash head */
};

/* Initialize an empty map. Returns 0 on success, non-zero if m is NULL. */
int dap_op_requester_init(struct dap_op_requester *m);

/*
 * Record that op_id was requested by requester_id (copied). Overwrites any requester
 * already stored for op_id. Returns 0 on success, non-zero on a bad argument or
 * allocation failure.
 */
int dap_op_requester_record(struct dap_op_requester *m, uint64_t op_id, const char *requester_id);

/* Return the requester recorded for op_id (borrowed, valid until destroy), or NULL. */
const char *dap_op_requester_lookup(struct dap_op_requester *m, uint64_t op_id);

/* Free every entry, leaving the map empty and reusable. */
void dap_op_requester_destroy(struct dap_op_requester *m);

#ifdef __cplusplus
}
#endif

#endif /* DAP_OP_REQUESTER_H */
