/* dap_subscription_queues.c */

#include <string.h>
#include <stdlib.h>

/* mosquitto_internal.h pulls in config.h, which points uthash at the mosquitto
 * allocators. Include it before dap_subscription_queues.h so uthash.h picks up
 * those macros rather than its own malloc/free defaults. */
#include "mosquitto_internal.h"
#include "util_mosq.h"
#include "utlist.h"
#include "dap_subscription_queues.h"

int dap_subscription_queues_init(struct dap_subscription_queues *q)
{
    if(!q) return 1;
    q->topics = NULL;
    q->total_count = 0;
    return 0;
}

/* Look up a topic's queue, or NULL if none exists yet. */
static struct dap_topic_queue *dap__find_topic(struct dap_subscription_queues *q, const char *topic)
{
    struct dap_topic_queue *tq = NULL;
    HASH_FIND_STR(q->topics, topic, tq);
    return tq;
}

/* Look up a topic's queue, creating and linking an empty one if needed. */
static struct dap_topic_queue *dap__find_or_create_topic(struct dap_subscription_queues *q, const char *topic)
{
    struct dap_topic_queue *tq = dap__find_topic(q, topic);
    if(tq) return tq;

    tq = mosquitto_calloc(1, sizeof(*tq));
    if(!tq) return NULL;
    tq->topic = mosquitto_strdup(topic);
    if(!tq->topic){
        mosquitto_FREE(tq);
        return NULL;
    }
    tq->head = NULL;
    tq->count = 0;
    HASH_ADD_KEYPTR(hh, q->topics, tq->topic, strlen(tq->topic), tq);
    return tq;
}

void dap_stamped_msg_free(struct dap_stamped_msg *msg)
{
    if(!msg) return;
    mosquitto_FREE(msg->applied_op_ids);
    mosquitto_FREE(msg);
}

int dap_subscription_queues_enqueue(struct dap_subscription_queues *q,
                                    const char *topic,
                                    struct mosquitto__base_msg *base_msg,
                                    uint64_t cmsg_id,
                                    uint32_t mp_version,
                                    uint32_t sp_version,
                                    const uint64_t *applied_op_ids,
                                    size_t num_applied_op_ids,
                                    time_t enqueue_time)
{
    if(!q || !topic) return 1;

    /* Build the stamped message wrapper. */
    struct dap_stamped_msg *msg = mosquitto_calloc(1, sizeof(*msg));
    if(!msg) return 1;
    msg->base_msg     = base_msg;
    msg->cmsg_id      = cmsg_id;
    msg->mp_version   = mp_version;
    msg->sp_version   = sp_version;
    msg->enqueue_time = enqueue_time;

    /* Copy the applied op-id list so the caller's storage need not outlive us. */
    if(applied_op_ids && num_applied_op_ids > 0){
        msg->applied_op_ids = mosquitto_malloc(sizeof(uint64_t) * num_applied_op_ids);
        if(!msg->applied_op_ids){
            dap_stamped_msg_free(msg);
            return 1;
        }
        memcpy(msg->applied_op_ids, applied_op_ids, sizeof(uint64_t) * num_applied_op_ids);
        msg->num_applied_op_ids = num_applied_op_ids;
    }

    struct dap_topic_queue *tq = dap__find_or_create_topic(q, topic);
    if(!tq){
        dap_stamped_msg_free(msg);
        return 1;
    }

    DL_APPEND(tq->head, msg); /* FIFO: newest at the tail */
    tq->count++;
    q->total_count++;
    return 0;
}

struct dap_stamped_msg *dap_subscription_queues_peek_front(struct dap_subscription_queues *q,
                                                           const char *topic)
{
    if(!q || !topic) return NULL;
    struct dap_topic_queue *tq = dap__find_topic(q, topic);
    return tq ? tq->head : NULL;
}

struct dap_stamped_msg *dap_subscription_queues_dequeue_front(struct dap_subscription_queues *q,
                                                              const char *topic)
{
    if(!q || !topic) return NULL;
    struct dap_topic_queue *tq = dap__find_topic(q, topic);
    if(!tq || !tq->head) return NULL;

    struct dap_stamped_msg *msg = tq->head;
    DL_DELETE(tq->head, msg);
    tq->count--;
    q->total_count--;

    /* Detach so the caller can re-insert it elsewhere. */
    msg->prev = NULL;
    msg->next = NULL;
    return msg;
}

struct dap_stamped_msg *dap_subscription_queues_find(struct dap_subscription_queues *q,
                                                     const char *topic,
                                                     const struct mosquitto__base_msg *base_msg,
                                                     uint64_t cmsg_id)
{
    if(!q || !topic) return NULL;
    struct dap_topic_queue *tq = dap__find_topic(q, topic);
    if(!tq) return NULL;

    struct dap_stamped_msg *msg;
    DL_FOREACH(tq->head, msg){
        if(msg->cmsg_id == cmsg_id && msg->base_msg == base_msg) return msg;
    }
    return NULL;
}

void dap_subscription_queues_remove(struct dap_subscription_queues *q,
                                    const char *topic,
                                    struct dap_stamped_msg *msg)
{
    if(!q || !topic || !msg) return;
    struct dap_topic_queue *tq = dap__find_topic(q, topic);
    if(!tq) return;

    DL_DELETE(tq->head, msg);
    tq->count--;
    q->total_count--;
    msg->prev = NULL;
    msg->next = NULL;
}

int dap_subscription_queues_push_front(struct dap_subscription_queues *q,
                                       const char *topic,
                                       struct dap_stamped_msg *msg)
{
    if(!q || !topic || !msg) return 1;
    struct dap_topic_queue *tq = dap__find_or_create_topic(q, topic);
    if(!tq) return 1;

    DL_PREPEND(tq->head, msg);
    tq->count++;
    q->total_count++;
    return 0;
}

size_t dap_subscription_queues_topic_size(struct dap_subscription_queues *q, const char *topic)
{
    if(!q || !topic) return 0;
    struct dap_topic_queue *tq = dap__find_topic(q, topic);
    return tq ? tq->count : 0;
}

size_t dap_subscription_queues_total_size(struct dap_subscription_queues *q)
{
    if(!q) return 0;
    return q->total_count;
}

void dap_subscription_queues_destroy(struct dap_subscription_queues *q)
{
    if(!q) return;

    struct dap_topic_queue *tq, *tmp;
    HASH_ITER(hh, q->topics, tq, tmp){
        struct dap_stamped_msg *msg = tq->head;
        while(msg){
            struct dap_stamped_msg *next = msg->next;
            dap_stamped_msg_free(msg);
            msg = next;
        }
        HASH_DEL(q->topics, tq);
        mosquitto_FREE(tq->topic);
        mosquitto_FREE(tq);
    }
    q->topics = NULL;
    q->total_count = 0;
}
