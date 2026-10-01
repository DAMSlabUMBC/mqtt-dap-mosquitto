/* dap_holding_list.c */

#include <string.h>

/* mosquitto_internal.h pulls in config.h, which points uthash at the mosquitto
 * allocators. Include it before dap_holding_list.h so uthash.h picks up those
 * macros rather than its own malloc/free defaults. */
#include "mosquitto_internal.h"
#include "util_mosq.h"
#include "utlist.h"
#include "dap_holding_list.h"

int dap_holding_list_init(struct dap_holding_list *h)
{
    if(!h) return 1;
    h->subscriptions = NULL;
    return 0;
}

/* Look up a subscription's hold state, or NULL if it is not holding. */
static struct dap_holding_entry *dap__find_holding(struct dap_holding_list *h, const char *sub_id)
{
    struct dap_holding_entry *e = NULL;
    HASH_FIND_STR(h->subscriptions, sub_id, e);
    return e;
}

int dap_holding_list_start_holding(struct dap_holding_list *h, const char *sub_id,
                                   uint64_t pending_id)
{
    if(!h || !sub_id) return 1;

    /* Already holding: keep the messages already parked for this subscription and
     * its original pending id. */
    if(dap__find_holding(h, sub_id)) return 0;

    struct dap_holding_entry *e = mosquitto_calloc(1, sizeof(*e));
    if(!e) return 1;
    e->sub_id = mosquitto_strdup(sub_id);
    if(!e->sub_id){
        mosquitto_FREE(e);
        return 1;
    }
    e->pending_id = pending_id;
    e->head = NULL;
    e->count = 0;
    HASH_ADD_KEYPTR(hh, h->subscriptions, e->sub_id, strlen(e->sub_id), e);
    return 0;
}

bool dap_holding_list_is_holding(struct dap_holding_list *h, const char *sub_id)
{
    if(!h || !sub_id) return false;
    return dap__find_holding(h, sub_id) != NULL;
}

uint64_t dap_holding_list_pending_id(struct dap_holding_list *h, const char *sub_id)
{
    if(!h || !sub_id) return 0;
    struct dap_holding_entry *e = dap__find_holding(h, sub_id);
    return e ? e->pending_id : 0;
}

int dap_holding_list_add_held(struct dap_holding_list *h, const char *sub_id,
                              struct dap_stamped_msg *msg)
{
    if(!h || !sub_id || !msg) return 1;

    struct dap_holding_entry *e = dap__find_holding(h, sub_id);
    if(!e) return 1; /* nothing to hold against unless start_holding ran first */

    struct dap_held_msg *node = mosquitto_calloc(1, sizeof(*node));
    if(!node) return 1;
    node->msg = msg; /* borrowed */

    DL_APPEND(e->head, node); /* FIFO: newest at the tail */
    e->count++;
    return 0;
}

struct dap_held_msg *dap_holding_list_flush(struct dap_holding_list *h, const char *sub_id)
{
    if(!h || !sub_id) return NULL;

    struct dap_holding_entry *e = dap__find_holding(h, sub_id);
    if(!e) return NULL;

    /* Hand the FIFO list to the caller and clear the holding state. */
    struct dap_held_msg *held = e->head;
    HASH_DEL(h->subscriptions, e);
    mosquitto_FREE(e->sub_id);
    mosquitto_FREE(e);
    return held;
}

void dap_holding_list_free_held(struct dap_held_msg *list)
{
    while(list){
        struct dap_held_msg *next = list->next;
        mosquitto_FREE(list); /* the stamped message is borrowed; leave it be */
        list = next;
    }
}

void dap_holding_list_destroy(struct dap_holding_list *h)
{
    if(!h) return;

    struct dap_holding_entry *e, *tmp;
    HASH_ITER(hh, h->subscriptions, e, tmp){
        HASH_DEL(h->subscriptions, e);
        dap_holding_list_free_held(e->head);
        mosquitto_FREE(e->sub_id);
        mosquitto_FREE(e);
    }
    h->subscriptions = NULL;
}
