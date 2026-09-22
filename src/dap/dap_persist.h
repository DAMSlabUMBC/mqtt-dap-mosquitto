#ifndef DAP_PERSIST_H
#define DAP_PERSIST_H

#include <stddef.h>
#include <stdint.h>
#include <time.h>

/* Persist DAP pending ops through a persistence plugin (e.g. persist-sqlite). */

/* DELETE/RESTRICT op added to the pending-op map. */
void dap_persist__op_add(uint64_t op_id, const char *publisher_id, int op_type, time_t timestamp,
		const char *topic_filters, const char *purpose_filters, const char *client_filters);

/* Op registered with the deadline tracker. */
void dap_persist__tracked_op_add(uint64_t op_id, const char *publisher_id,
		const char *const *expected_subs, size_t num_expected, time_t deadline);

/* Subscriber responded to an op. */
void dap_persist__tracked_op_response(uint64_t op_id, const char *subscriber_id);

/* Op settled; only its requester mapping is kept. */
void dap_persist__tracked_op_delete(uint64_t op_id);

/* DAP-Timestamp from a message's properties, or 0. */
time_t dap_persist__recv_time(const struct mqtt5__property *properties);

#endif
