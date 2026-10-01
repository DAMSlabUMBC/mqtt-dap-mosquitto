/* dap_op_request.h */
#ifndef DAP_OP_REQUEST_H
#define DAP_OP_REQUEST_H

#include <stdint.h>
#include <time.h>

/* dap_pending_ops.h provides the map type and dap_pending_ops_insert_operation. */
#include "dap_pending_ops.h"
#include "property_common.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Decode a DAP operation request and, for an erasure or restriction, insert the
 * matching pending operation into the broker-wide map.
 *
 * operation is the DAP-OpType value; only MOSQ_DAP_OP_DELETE
 * and MOSQ_DAP_OP_RESTRICT are pending operations. Any
 * other value is left to the existing right handlers and nothing is inserted. The
 * filter strings are the DAP-OpTFs / DAP-OpPFs / DAP-OpClients lists (NULL or ""
 * means any) and are forwarded to dap_pending_ops_insert_operation, which copies
 * and parses them.
 *
 * On a successful insert the assigned op id is written to *op_id_out (when
 * non-NULL) and 0 is returned. Returns non-zero, inserting nothing, when the
 * operation is not a pending operation, on a bad argument, or on insert failure.
 */
int dap_op_request_insert(struct dap_pending_ops *map,
                          const char *pub_id,
                          struct dap__op_property *dap_op_properties,
                          time_t timestamp,
                          uint64_t *op_id_out);

#ifdef __cplusplus
}
#endif

#endif /* DAP_OP_REQUEST_H */
