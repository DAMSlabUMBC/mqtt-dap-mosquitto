#ifndef DAP_PERSIST_H
#define DAP_PERSIST_H

#include <stddef.h>
#include <stdint.h>
#include <time.h>

struct dr_sublist;

/* Persist DAP pending ops through a persistence plugin (e.g. persist-sqlite). */

/* DELETE/RESTRICT op added to the pending-op map. */
void dap_persist__op_add(uint64_t op_id, const char *publisher_id, int op_type, time_t timestamp,
		time_t deadline, const char *topic_filters, const char *purpose_filters, const char *client_filters);

/* DELETE/RESTRICT op reclaimed from the pending-op map. */
void dap_persist__op_delete(uint64_t op_id);

/* Operation request held for a subscriber. */
void dap_persist__request_add(const char *subscriber_id, uint64_t op_id, time_t deadline,
		const void *payload, uint32_t payloadlen, const struct mqtt5__property *properties);

/* Requests held for subscriber_id delivered or, if that is NULL, the expired ones. */
void dap_persist__request_delete(const char *subscriber_id, time_t deadline);

/* Flow added, or its receipt times moved. */
void dap_persist__flow_add(const char *publisher_id, const char *topic, const struct dr_sublist *flow);

/* Op registered with the deadline tracker. */
void dap_persist__tracked_op_add(uint64_t op_id, const char *publisher_id,
		const char *const *expected_subs, size_t num_expected, time_t deadline);

/* Subscriber responded to an op with status and reason (may be NULL). */
void dap_persist__tracked_op_response(uint64_t op_id, const char *subscriber_id, const char *status, const char *reason);

/* Op settled, kept until its deadline, or (settled false) past its deadline. */
void dap_persist__tracked_op_delete(uint64_t op_id, bool settled);

/* DAP-Timestamp from a message's properties, or 0. */
time_t dap_persist__recv_time(const struct mqtt5__property *properties);

#endif
