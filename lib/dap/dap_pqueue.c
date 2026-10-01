#include <stdlib.h>

#include "dap_pqueue.h"
#include "utlist.h"

int dap_pqueue__init(struct dap_pqueue *q)
{
    if(!q) return 1;
    q->high_head = NULL;
    q->low_head = NULL;
    q->high_count = 0;
    q->low_count = 0;
    return 0;
}

int dap_pqueue__enqueue(struct dap_pqueue *q, struct mosquitto__packet_in *packet, enum dap_pq_priority priority)
{
    struct dap_pq_node *node;

    if(!q || !packet) return 1;

    node = calloc(1, sizeof(*node));
    if(!node) return 1;
    node->packet = packet;

    /* Append to the tail of the matching level so each level stays FIFO. */
    if(priority == DAP_PQ_PRIORITY_HIGH){
        DL_APPEND(q->high_head, node);
        q->high_count++;
    }else{
        DL_APPEND(q->low_head, node);
        q->low_count++;
    }
    return 0;
}

struct mosquitto__packet_in *dap_pqueue__dequeue(struct dap_pqueue *q)
{
    struct dap_pq_node *node;
    struct mosquitto__packet_in *packet;

    if(!q) return NULL;

    /* Drain the high-priority list first, falling back to low. */
    if(q->high_head){
        node = q->high_head;
        DL_DELETE(q->high_head, node);
        q->high_count--;
    }else if(q->low_head){
        node = q->low_head;
        DL_DELETE(q->low_head, node);
        q->low_count--;
    }else{
        return NULL;
    }

    packet = node->packet;
    free(node);
    return packet;
}

struct mosquitto__packet_in *dap_pqueue__peek(struct dap_pqueue *q)
{
    if(!q) return NULL;
    /* Same high-before-low order as dequeue, but the node stays in place. */
    if(q->high_head) return q->high_head->packet;
    if(q->low_head) return q->low_head->packet;
    return NULL;
}

size_t dap_pqueue__size(struct dap_pqueue *q)
{
    if(!q) return 0;
    return q->high_count + q->low_count;
}

void dap_pqueue__destroy(struct dap_pqueue *q)
{
    struct dap_pq_node *node;

    if(!q) return;

    /* Free the link nodes only - the packets they point at are not ours to free. */
    while(q->high_head){
        node = q->high_head;
        DL_DELETE(q->high_head, node);
        free(node);
    }
    while(q->low_head){
        node = q->low_head;
        DL_DELETE(q->low_head, node);
        free(node);
    }
    q->high_count = 0;
    q->low_count = 0;
}
