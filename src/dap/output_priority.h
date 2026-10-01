/* output_priority.h
 *
 * Output-side priority queue for the send path (MQTT-DAP paper 5.2(ii)):
 * operation and PBMR/control-plane messages are processed ahead of data on a
 * client's outgoing queue (msgs_out.queued).
 *
 * The single helper here, db__queued_insert_prioritized, replaces the plain
 * DL_APPEND used when a message is parked in msgs_out.queued. It keeps op/PBMR
 * messages ahead of data while preserving FIFO order within each class. The
 * reordering is strictly cross-class: op/PBMR topics (dap_is_op_system_topic) and
 * application data topics are disjoint, so two messages on the same topic always
 * keep their relative order. That is what the per-topic DAP verification queues
 * and the send-path hold/bump gate depend on, so both are left untouched; the
 * inflight list is never reordered either.
 *
 * The function is static and header-defined so it compiles directly into the one
 * translation unit that uses it (database.c) without new linkage, and so a
 * standalone unit test can include and exercise the identical code. The includer
 * must already have the broker message structs in scope (mosquitto__client_msg,
 * mosquitto_msg_data, mosquitto__base_msg), i.e. include mosquitto_broker_internal.h
 * before this header.
 */
#ifndef OUTPUT_PRIORITY_H
#define OUTPUT_PRIORITY_H

#include <utlist.h>

#include "dap/dap_topics.h"

/*
 * Insert client_msg into msg_data->queued. A data message is appended at the tail
 * (ordinary FIFO). An op/PBMR message (topic matches dap_is_op_system_topic) is
 * spliced in after any op messages already queued but before the first data
 * message, so the op class drains ahead of data while each class stays FIFO. When
 * the queue is empty or holds only op messages, DL_PREPEND_ELEM with a NULL element
 * appends at the tail. Only msg_data->queued is touched.
 */
static void db__queued_insert_prioritized(struct mosquitto_msg_data *msg_data,
		struct mosquitto__client_msg *client_msg, const char *topic)
{
	struct mosquitto__client_msg *cur, *first_data = NULL;

	if(!dap_is_op_system_topic(topic)){
		/* Data message: ordinary FIFO append at the tail. */
		DL_APPEND(msg_data->queued, client_msg);
		return;
	}

	/* Op/PBMR message: find the first data (non-op) message and insert before it. */
	DL_FOREACH(msg_data->queued, cur){
		if(!cur->base_msg || !dap_is_op_system_topic(cur->base_msg->data.topic)){
			first_data = cur;
			break;
		}
	}
	DL_PREPEND_ELEM(msg_data->queued, first_data, client_msg);
}

#endif /* OUTPUT_PRIORITY_H */
