/* dap_request_store.c */

#include <string.h>

/* mosquitto_internal.h pulls in config.h, which points uthash at the mosquitto
 * allocators. Include it before dap_request_store.h so uthash.h picks up those
 * macros rather than its own malloc/free defaults. */
#include "mosquitto_internal.h"
#include "util_mosq.h"
#include "dap_request_store.h"

void dap_request_store_init(struct dap_request_store *store)
{
    store->inboxes = NULL;
}

static void dap__request_free(struct dap_stored_request *r)
{
    mosquitto_FREE(r->payload);
    mosquitto_property_free_all(&r->properties);
    mosquitto_FREE(r);
}

void dap_request_store_free_list(struct dap_stored_request *list)
{
    while(list){
        struct dap_stored_request *next = list->next;
        dap__request_free(list);
        list = next;
    }
}

static void dap__inbox_free(struct dap_request_store *store, struct dap_request_inbox *inbox)
{
    HASH_DEL(store->inboxes, inbox);
    dap_request_store_free_list(inbox->requests);
    mosquitto_FREE(inbox->sub_id);
    mosquitto_FREE(inbox);
}

int dap_request_store_add(struct dap_request_store *store, const char *sub_id, uint64_t op_id,
                          time_t deadline, uint8_t qos, const void *payload, uint32_t payloadlen,
                          mosquitto_property *properties)
{
    struct dap_request_inbox *inbox = NULL;
    struct dap_stored_request *r, **tail;

    if(!store || !sub_id || (payloadlen && !payload)) return 1;

    r = mosquitto_calloc(1, sizeof(*r));
    if(!r) return 1;
    if(payloadlen){
        r->payload = mosquitto_malloc(payloadlen);
        if(!r->payload){
            mosquitto_FREE(r);
            return 1;
        }
        memcpy(r->payload, payload, payloadlen);
    }
    r->op_id = op_id;
    r->deadline = deadline;
    r->qos = qos;
    r->payloadlen = payloadlen;

    HASH_FIND_STR(store->inboxes, sub_id, inbox);
    if(!inbox){
        inbox = mosquitto_calloc(1, sizeof(*inbox));
        if(inbox) inbox->sub_id = mosquitto_strdup(sub_id);
        if(!inbox || !inbox->sub_id){
            mosquitto_FREE(inbox);
            dap__request_free(r);
            return 1;
        }
        HASH_ADD_KEYPTR(hh, store->inboxes, inbox->sub_id, strlen(inbox->sub_id), inbox);
    }
    for(tail = &inbox->requests; *tail; tail = &(*tail)->next){}
    *tail = r;
    r->properties = properties;
    return 0;
}

bool dap_request_store_has(struct dap_request_store *store, const char *sub_id)
{
    struct dap_request_inbox *inbox = NULL;

    if(!store || !sub_id) return false;
    HASH_FIND_STR(store->inboxes, sub_id, inbox);
    return inbox != NULL;
}

struct dap_stored_request *dap_request_store_take(struct dap_request_store *store, const char *sub_id, time_t now)
{
    struct dap_request_inbox *inbox = NULL;
    struct dap_stored_request *list, *kept = NULL, **tail = &kept;

    if(!store || !sub_id) return NULL;
    HASH_FIND_STR(store->inboxes, sub_id, inbox);
    if(!inbox) return NULL;

    list = inbox->requests;
    inbox->requests = NULL;
    dap__inbox_free(store, inbox);
    while(list){
        struct dap_stored_request *next = list->next;
        list->next = NULL;
        if(list->deadline > now){
            *tail = list;
            tail = &list->next;
        }else{
            dap__request_free(list);
        }
        list = next;
    }
    return kept;
}

/* Drop the requests of an inbox for which drop() is true, and the inbox once empty. */
static void dap__inbox_filter(struct dap_request_store *store, struct dap_request_inbox *inbox,
                              bool (*drop)(const struct dap_stored_request *, const void *), const void *arg)
{
    struct dap_stored_request **link = &inbox->requests;

    while(*link){
        struct dap_stored_request *r = *link;
        if(drop(r, arg)){
            *link = r->next;
            dap__request_free(r);
        }else{
            link = &r->next;
        }
    }
    if(!inbox->requests){
        dap__inbox_free(store, inbox);
    }
}

static bool dap__expired(const struct dap_stored_request *r, const void *now)
{
    return r->deadline <= *(const time_t *)now;
}

static bool dap__of_operation(const struct dap_stored_request *r, const void *op_id)
{
    return r->op_id == *(const uint64_t *)op_id;
}

void dap_request_store_expire(struct dap_request_store *store, time_t now)
{
    struct dap_request_inbox *inbox, *tmp;

    if(!store) return;
    HASH_ITER(hh, store->inboxes, inbox, tmp){
        dap__inbox_filter(store, inbox, dap__expired, &now);
    }
}

void dap_request_store_remove_operation(struct dap_request_store *store, uint64_t op_id)
{
    struct dap_request_inbox *inbox, *tmp;

    if(!store) return;
    HASH_ITER(hh, store->inboxes, inbox, tmp){
        dap__inbox_filter(store, inbox, dap__of_operation, &op_id);
    }
}

void dap_request_store_destroy(struct dap_request_store *store)
{
    struct dap_request_inbox *inbox, *tmp;

    if(!store) return;
    HASH_ITER(hh, store->inboxes, inbox, tmp){
        dap__inbox_free(store, inbox);
    }
}
