/* dap_op_request.c */

#include <string.h>

#include "dap_op_request.h"
#include "dap_pending_ops.h"
#include "mosquitto/defs.h"

int dap_op_request_insert(struct dap_pending_ops *map,
                          const char *pub_id,
                          struct dap__op_property *dap_op_properties,
                          time_t timestamp,
                          uint64_t *op_id_out)
{
    if(!map || !pub_id || !dap_op_properties->op_id) return 1;

    /* Only erasure and restriction become pending operations; the broker's existing
     * right handlers deal with every other DAP-OpType string. */
    enum dap_op_type type;
    if(!strcmp(dap_op_properties->op_id, MOSQ_DAP_OP_DELETE)){
        type = DAP_OP_DELETE;
    }else if(!strcmp(dap_op_properties->op_id, MOSQ_DAP_OP_RESTRICT)){
        type = DAP_OP_RESTRICT;
    }else{
        return 1;
    }

    /* The filter strings are forwarded as-is; insert_operation copies and parses each
     * comma-separated list (NULL/"" -> "any"). */
    return dap_pending_ops_insert_operation(map, pub_id, type, timestamp,
                                            dap_op_properties->op_topic_filters, dap_op_properties->op_purpose_filters,
                                            dap_op_properties->op_client_filters, op_id_out);
}
