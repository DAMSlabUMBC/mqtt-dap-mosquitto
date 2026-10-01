/* dr_registry.h */
#ifndef DR_REGISTRY_H
#define DR_REGISTRY_H

#include <time.h>
#include "property_common.h"

struct dr_sublist {
    char *sub_id;
    char *sp;   /* subscriber's SP (DAP-SP) recorded at receipt time, or NULL */
    time_t recv_time; /* message receipt time for this flow (stored->dap_recv_time); 0 if unknown */
    struct dr_sublist *next;
};

struct dr_entry {
    char *pub_id;
    char *topic;
    struct dr_sublist *sub_list;
    struct dr_entry *next;
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

/* Add a subscriber to pub_id and topic, recording the message receipt time
 * (stored->dap_recv_time; 0 if unknown). */
void dr__record_recipient(const char *pub_id, const char *topic, const char *sub_id, time_t recv_time);

/* As dr__record_recipient, but also stores the subscriber's SP (DAP-SP) at receipt
 * time. A NULL sp leaves any SP already recorded for this flow untouched. */
void dr__record_recipient_with_sp(const char *pub_id, const char *topic, const char *sub_id, const char *sp, time_t recv_time);

void dr__record_retained_publisher(const char *pub_id, const char *topic);


/* Freed after use. */
struct dr_sublist *dr__get_recipients(const char *pub_id, const char *topic);
void dr__free_sublist(struct dr_sublist *list);

/*
 * Returns the subscribers relevant to an operation invoked by pub_id: those that
 * previously received data from pub_id where, in addition,
 *  - the receipt topic matches a topic filter (op_topic_filters),
 *  - the SP at receipt time shares a purpose with the purpose filters
 *    (op_purpose_filters),
 *  - the subscriber id is in the client filters (op_client_filters), and
 *  - the receipt time falls within the [after, before] bounds (DAP-OpAfter /
 *    DAP-OpBefore): recv_time >= after when after != 0, and recv_time <= before
 *    when before != 0. A bound of 0 means "unbounded" on that side.
 * Each filter argument is a comma-separated list; NULL, "" or a list containing
 * "*" means "not provided / any", which skips that condition. List matching is
 * "any element matches", mirroring dap_pending_ops.
 *
 * Each relevant subscriber appears once; the caller frees the list with
 * dr__free_sublist.
 *
 * Limitation: topic and purpose elements are compared exactly (or via "*"). MQTT
 * topic wildcards (+/#) and hierarchical purpose subsumption are not yet honoured.
 */
struct dr_sublist *dr__find_relevant_subscribers(const char *pub_id, struct dap__op_property *dap_op_properties);

#endif
