/* dap_op_requester.c */

#include <string.h>

/* mosquitto_internal.h pulls in config.h, which points uthash at the mosquitto
 * allocators. Include it before dap_op_requester.h so uthash.h picks up those macros
 * rather than its own malloc/free defaults. */
#include "mosquitto_internal.h"
#include "util_mosq.h"
#include "dap_op_requester.h"

int dap_op_requester_init(struct dap_op_requester *m)
{
    if(!m) return 1;
    m->entries = NULL;
    return 0;
}

static struct dap_op_requester_entry *dap__find(struct dap_op_requester *m, uint64_t op_id)
{
    struct dap_op_requester_entry *e = NULL;
    HASH_FIND(hh, m->entries, &op_id, sizeof(op_id), e);
    return e;
}

int dap_op_requester_record(struct dap_op_requester *m, uint64_t op_id, const char *requester_id)
{
    if(!m || !requester_id) return 1;

    struct dap_op_requester_entry *e = dap__find(m, op_id);
    if(e){
        /* Overwrite the requester for an op id that is recorded again. */
        char *dup = mosquitto_strdup(requester_id);
        if(!dup) return 1;
        mosquitto_FREE(e->requester_id);
        e->requester_id = dup;
        return 0;
    }

    e = mosquitto_calloc(1, sizeof(*e));
    if(!e) return 1;
    e->op_id = op_id;
    e->requester_id = mosquitto_strdup(requester_id);
    if(!e->requester_id){
        mosquitto_FREE(e);
        return 1;
    }
    HASH_ADD(hh, m->entries, op_id, sizeof(e->op_id), e);
    return 0;
}

const char *dap_op_requester_lookup(struct dap_op_requester *m, uint64_t op_id)
{
    if(!m) return NULL;
    struct dap_op_requester_entry *e = dap__find(m, op_id);
    return e ? e->requester_id : NULL;
}

void dap_op_requester_destroy(struct dap_op_requester *m)
{
    if(!m) return;
    struct dap_op_requester_entry *e, *tmp;
    HASH_ITER(hh, m->entries, e, tmp){
        HASH_DEL(m->entries, e);
        mosquitto_FREE(e->requester_id);
        mosquitto_FREE(e);
    }
    m->entries = NULL;
}
