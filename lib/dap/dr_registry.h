/* dr_registry.h */
#ifndef DR_REGISTRY_H
#define DR_REGISTRY_H

#include <time.h>

#include "config.h"
#include "property_common.h"
#include "uthash.h"

/* A flow: deliveries from one publisher on one topic to one subscriber under one SP. */
struct dr_sublist {
    char *sub_id;
    char *sp;          /* the subscription's SP at delivery, as a '|'-joined sorted purpose set */
    time_t first_time; /* receipt time of the first and the last message delivered on the flow */
    time_t last_time;
    struct dr_sublist *next;
};

/* The flows of one (publisher, topic) pair. */
struct dr_entry {
    char *pub_id;
    char *topic;
    char *key;         /* pub_id, a NUL, then topic: the hash key */
    struct dr_sublist *sub_list;
    struct dr_entry *next;
    UT_hash_handle hh;
};

struct dr_retained_entry {
    char *pub_id;
    char *topic;
    struct dr_retained_entry *next;
};

extern struct dr_entry *dr_head; 
extern struct dr_retained_entry *dr_retained_head; 

void dr_registry_init(void);
void dr_registry_cleanup(void);

/* Record a delivery from pub_id on topic to sub_id, whose subscription has the sorted
 * purpose set sp, of a message received at recv_time (paper 6.1). *changed is set to
 * the flow when it is new or its receipt times moved, else NULL. Returns 0, or
 * MOSQ_ERR_NOMEM. */
int dr__record_flow(const char *pub_id, const char *topic, const char *sub_id,
                    char *const *sp, uint32_t sp_count, time_t recv_time,
                    const struct dr_sublist **changed);

/* Restore a persisted flow; sp is its '|'-joined purpose set. Returns 0, or
 * MOSQ_ERR_NOMEM. */
int dr__restore_flow(const char *pub_id, const char *topic, const char *sub_id,
                     const char *sp, time_t first_time, time_t last_time);

void dr__record_retained_publisher(const char *pub_id, const char *topic);

/* Freed after use. */
void dr__free_sublist(struct dr_sublist *list);

/*
 * Returns the subscribers relevant to an operation invoked by pub_id (paper 6.1):
 * those with a flow from pub_id where, in addition,
 *  - the topic matches an MQTT topic filter in op_topic_filters,
 *  - the SP recorded at delivery shares a purpose with op_purpose_filters,
 *  - the subscriber id is in op_client_filters, and
 *  - the flow's receipt times overlap the [after, before] bounds (DAP-OpAfter /
 *    DAP-OpBefore): last_time >= after when after != 0, and first_time <= before
 *    when before != 0. A bound of 0 means "unbounded" on that side.
 * Topic and client filters are comma-separated lists, and purpose filters a purpose
 * filter collection; NULL, "" or a list containing "*" skips that condition.
 *
 * Each relevant subscriber appears once; the caller frees the list with
 * dr__free_sublist.
 */
struct dr_sublist *dr__find_relevant_subscribers(const char *pub_id, struct dap__op_property *dap_op_properties);

#endif
