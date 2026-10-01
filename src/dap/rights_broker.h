#ifndef RIGHTS_BROKER_H
#define RIGHTS_BROKER_H

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include <time.h>

#include "property_common.h"

/* Forward-declare mosquitto struct if needed. */
struct mosquitto;
struct mosquitto_base_msg;
struct dr_sublist; /* lib/dr_registry.h: relevant-subscriber list node */

/* Look up a client context by ID. */
struct mosquitto *broker_find_context_by_id(const char *client_id);

/* A DELETE also removes the requester's will and retained messages it covers. */
void handle_remove_stored_messages(const char *publisher_id, struct dap__op_property *dap_op_properties);

/* Replace the request's failure reason with a copy of reason. */
void dap_op_set_reason(struct dap__op_property *dap_op_properties, const char *reason);

/* Responses back to a publisher on RNP/<publisher_id>. */
void broker_send_response_success(const char *publisher_id, const char *operation, const char *corr_data, uint16_t correlation_data_len, const char *payload, char* response_topic);
/* Acknowledge a validated pending op to the requester's ONP, carrying the
 * broker-assigned numeric op id and the absolute deadline (epoch seconds). */
void broker_send_response_pending(const char *publisher_id, struct dap__op_property* dap_op_properties, time_t deadline);
/* Final failure for a request. Like broker_send_response_success it is sent to the
 * request's response topic, or to ONP/<publisher_id> when the request named none. */
void broker_send_response_failure(const char *publisher_id, struct dap__op_property* dap_op_properties);

/* Subscriber-involving dispatch: forward the request to the relevant subscribers on
 * their ORS (carrying op_id), holding it for those not yet able to receive it, register
 * the op with the deadline tracker so unresponded subscribers can be reported at expiry,
 * and echo a Pending ack with the op id + deadline back to the requester. The relevant
 * list is borrowed, not freed. */
void broker_dispatch_pending_operation(const char *publisher_id, struct dr_sublist *relevant, struct mosquitto_base_msg *msg_data, struct dap__op_property *dap_op_properties, time_t deadline);

/* Deadline-expiry notification: tell the requester on ONP that op_id expired with
 * the given unresponded subscriber ids (DAP-Status=Failure, DAP-OpId, DAP-UnreachedClients). */
void broker_send_deadline_failure(uint64_t op_id, const char *publisher_id,
    char **unresponded_subs, size_t num_unresponded);

/* All-responded notification: tell the requester on ONP that op_id completed because
 * every relevant subscriber responded before the deadline (DAP-Status=Success, DAP-OpId). */
void broker_send_deadline_success(uint64_t op_id, const char *publisher_id);

/* Forward a subscriber's status notification (success/failure/pending) to the
 * requester on ONP/<requester_id>, preserving DAP-OpId, DAP-Status, DAP-Reason and
 * the responding subscriber id (DAP-ClientID), plus any payload/correlation data. */
void broker_forward_status_to_requester(const char *requester_id, struct dap__op_property *dap_op_properties, const char *responder_id,
    const void *payload, uint32_t payloadlen);

/* Answer a status request (paper 6.3) from requester_id for the operation named by
 * DAP-OpId with every relevant subscriber's status, as a JSON payload. */
void broker_send_operation_status(const char *requester_id, struct dap__op_property *dap_op_properties);

/* Deliver the requests held for a subscriber that is now connected and subscribed
 * to its request topic. */
void broker_deliver_held_requests(struct mosquitto *context);

#endif
