/* dap_deadline_tracker.c */

#include <string.h>

/* mosquitto_internal.h pulls in config.h, which points uthash at the mosquitto
 * allocators. Include it before dap_deadline_tracker.h so uthash.h picks up those
 * macros rather than its own malloc/free defaults. */
#include "mosquitto_internal.h"
#include "util_mosq.h"
#include "utlist.h"
#include "dap_deadline_tracker.h"

int dap_deadline_tracker_init(struct dap_deadline_tracker *t)
{
    if(!t) return 1;
    t->operations = NULL;
    return 0;
}

/* Look up a tracked operation by id, or NULL if it is not tracked. */
static struct dap_tracked_op *dap__find_op(struct dap_deadline_tracker *t, uint64_t op_id)
{
    struct dap_tracked_op *op = NULL;
    HASH_FIND(hh, t->operations, &op_id, sizeof(op_id), op);
    return op;
}

/* Free the expected-subscriber list of an operation. */
static void dap__free_expected(struct dap_tracked_op *op)
{
    struct dap_expected_sub *sub, *tmp;
    DL_FOREACH_SAFE(op->expected, sub, tmp){
        DL_DELETE(op->expected, sub);
        mosquitto_FREE(sub->sub_id);
        mosquitto_FREE(sub);
    }
}

/* Free a tracked operation node and everything it owns. */
static void dap__free_op(struct dap_tracked_op *op)
{
    dap__free_expected(op);
    mosquitto_FREE(op->publisher_id);
    mosquitto_FREE(op);
}

int dap_deadline_tracker_register_pending_operation(struct dap_deadline_tracker *t,
                                                    uint64_t op_id,
                                                    const char *publisher_id,
                                                    const char *const *expected_subs,
                                                    size_t num_expected,
                                                    time_t deadline)
{
    if(!t || !publisher_id) return 1;
    if(dap__find_op(t, op_id)) return 1; /* op ids are unique; refuse to clobber */

    struct dap_tracked_op *op = mosquitto_calloc(1, sizeof(*op));
    if(!op) return 1;
    op->op_id    = op_id;
    op->deadline = deadline;
    op->publisher_id = mosquitto_strdup(publisher_id);
    if(!op->publisher_id){
        mosquitto_FREE(op);
        return 1;
    }

    /* Copy the expected subscribers into the op's list, preserving their order. */
    for(size_t i = 0; i < num_expected; i++){
        struct dap_expected_sub *sub = mosquitto_calloc(1, sizeof(*sub));
        if(!sub){
            dap__free_op(op);
            return 1;
        }
        sub->sub_id = mosquitto_strdup(expected_subs[i]);
        if(!sub->sub_id){
            mosquitto_FREE(sub);
            dap__free_op(op);
            return 1;
        }
        sub->responded = false;
        DL_APPEND(op->expected, sub);
        op->expected_count++;
    }

    HASH_ADD(hh, t->operations, op_id, sizeof(op->op_id), op);
    return 0;
}

int dap_deadline_tracker_mark_subscriber_responded(struct dap_deadline_tracker *t,
                                                    uint64_t op_id,
                                                    const char *subscriber_id)
{
    if(!t || !subscriber_id) return 1;

    struct dap_tracked_op *op = dap__find_op(t, op_id);
    if(!op) return 1; /* untracked (e.g. a late response): caller forwards separately */

    struct dap_expected_sub *sub;
    DL_FOREACH(op->expected, sub){
        if(!strcmp(sub->sub_id, subscriber_id)){
            sub->responded = true;
            return 0;
        }
    }
    return 1; /* subscriber was not in the expected set */
}

bool dap_deadline_tracker_is_tracked(struct dap_deadline_tracker *t, uint64_t op_id)
{
    if(!t) return false;
    return dap__find_op(t, op_id) != NULL;
}

bool dap_deadline_tracker_all_responded(struct dap_deadline_tracker *t, uint64_t op_id)
{
    if(!t) return false;
    struct dap_tracked_op *op = dap__find_op(t, op_id);
    if(!op) return false;

    struct dap_expected_sub *sub;
    DL_FOREACH(op->expected, sub){
        if(!sub->responded) return false;
    }
    return true;
}

int dap_deadline_tracker_remove(struct dap_deadline_tracker *t, uint64_t op_id)
{
    if(!t) return 1;
    struct dap_tracked_op *op = dap__find_op(t, op_id);
    if(!op) return 1;
    HASH_DEL(t->operations, op);
    dap__free_op(op);
    return 0;
}

/* Build a result node holding the op's id, publisher, and unresponded subscribers. */
static struct dap_expired_op *dap__build_expired(struct dap_tracked_op *op)
{
    struct dap_expired_op *e = mosquitto_calloc(1, sizeof(*e));
    if(!e) return NULL;
    e->op_id = op->op_id;
    e->publisher_id = mosquitto_strdup(op->publisher_id);
    if(!e->publisher_id){
        mosquitto_FREE(e);
        return NULL;
    }

    /* Count the unresponded subscribers, then copy their ids in list order. */
    size_t n = 0;
    struct dap_expected_sub *sub;
    DL_FOREACH(op->expected, sub){
        if(!sub->responded) n++;
    }
    if(n > 0){
        e->unresponded_subs = mosquitto_calloc(n, sizeof(char*));
        if(!e->unresponded_subs){
            mosquitto_FREE(e->publisher_id);
            mosquitto_FREE(e);
            return NULL;
        }
        size_t i = 0;
        DL_FOREACH(op->expected, sub){
            if(sub->responded) continue;
            e->unresponded_subs[i] = mosquitto_strdup(sub->sub_id);
            if(!e->unresponded_subs[i]){
                e->num_unresponded = i; /* free what we copied so far */
                dap_deadline_tracker_free_expired(e);
                return NULL;
            }
            i++;
        }
        e->num_unresponded = n;
    }
    return e;
}

struct dap_expired_op *dap_deadline_tracker_check_expired(struct dap_deadline_tracker *t, time_t now)
{
    if(!t) return NULL;

    struct dap_expired_op *results = NULL;
    struct dap_tracked_op *op, *tmp;
    HASH_ITER(hh, t->operations, op, tmp){
        if(op->deadline > now) continue; /* deadline not yet reached */

        struct dap_expired_op *e = dap__build_expired(op);
        if(e){
            /* Prepend to the result list. Order across ops is not promised. */
            e->next = results;
            results = e;
        }
        /* Stop tracking the expired op whether or not the result node was built. */
        HASH_DEL(t->operations, op);
        dap__free_op(op);
    }
    return results;
}

void dap_deadline_tracker_free_expired(struct dap_expired_op *list)
{
    while(list){
        struct dap_expired_op *next = list->next;
        for(size_t i = 0; i < list->num_unresponded; i++){
            mosquitto_FREE(list->unresponded_subs[i]);
        }
        mosquitto_FREE(list->unresponded_subs);
        mosquitto_FREE(list->publisher_id);
        mosquitto_FREE(list);
        list = next;
    }
}

void dap_deadline_tracker_destroy(struct dap_deadline_tracker *t)
{
    if(!t) return;

    struct dap_tracked_op *op, *tmp;
    HASH_ITER(hh, t->operations, op, tmp){
        HASH_DEL(t->operations, op);
        dap__free_op(op);
    }
    t->operations = NULL;
}
