#include "config.h"

#include <stdlib.h>
#include <string.h>

#include "mosquitto_broker_internal.h"
#include "mosquitto/broker.h"
#include "utlist.h"
#include "dap/dap_persist.h"
#include "dap/dap_pending_ops.h"
#include "dap/dap_deadline_tracker.h"
#include "dap/dap_op_requester.h"
#include "dap/dr_registry.h"
#include "dap/dap_request_store.h"


void dap_persist__op_add(uint64_t op_id, const char *publisher_id, int op_type, time_t timestamp,
		time_t deadline, const char *topic_filters, const char *purpose_filters, const char *client_filters)
{
	struct mosquitto_evt_persist_dap_op event_data;
	struct mosquitto__callback *cb_base, *cb_next;
	struct mosquitto__security_options *opts;

	if(db.shutdown){
		return;
	}

	opts = &db.config->security_options;
	memset(&event_data, 0, sizeof(event_data));
	event_data.data.op_id = op_id;
	event_data.data.publisher_id = publisher_id;
	event_data.data.op_type = op_type;
	event_data.data.timestamp = timestamp;
	event_data.data.deadline = deadline;
	event_data.data.topic_filters = topic_filters;
	event_data.data.purpose_filters = purpose_filters;
	event_data.data.client_filters = client_filters;

	DL_FOREACH_SAFE(opts->plugin_callbacks.persist_dap_op_add, cb_base, cb_next){
		cb_base->cb(MOSQ_EVT_PERSIST_DAP_OP_ADD, &event_data, cb_base->userdata);
	}
}


void dap_persist__op_delete(uint64_t op_id)
{
	struct mosquitto_evt_persist_dap_op event_data;
	struct mosquitto__callback *cb_base, *cb_next;
	struct mosquitto__security_options *opts;

	if(db.shutdown){
		return;
	}

	opts = &db.config->security_options;
	memset(&event_data, 0, sizeof(event_data));
	event_data.data.op_id = op_id;

	DL_FOREACH_SAFE(opts->plugin_callbacks.persist_dap_op_delete, cb_base, cb_next){
		cb_base->cb(MOSQ_EVT_PERSIST_DAP_OP_DELETE, &event_data, cb_base->userdata);
	}
}


void dap_persist__flow_add(const char *publisher_id, const char *topic, const struct dr_sublist *flow)
{
	struct mosquitto_evt_persist_dap_flow event_data;
	struct mosquitto__callback *cb_base, *cb_next;
	struct mosquitto__security_options *opts;

	if(db.shutdown){
		return;
	}

	opts = &db.config->security_options;
	memset(&event_data, 0, sizeof(event_data));
	event_data.data.publisher_id = publisher_id;
	event_data.data.topic = topic;
	event_data.data.subscriber_id = flow->sub_id;
	event_data.data.purposes = flow->sp;
	event_data.data.first_time = flow->first_time;
	event_data.data.last_time = flow->last_time;

	DL_FOREACH_SAFE(opts->plugin_callbacks.persist_dap_flow_add, cb_base, cb_next){
		cb_base->cb(MOSQ_EVT_PERSIST_DAP_FLOW_ADD, &event_data, cb_base->userdata);
	}
}


static void dap_persist__request_event(int event, struct mosquitto__callback *callbacks,
		const char *subscriber_id, uint64_t op_id, time_t deadline,
		const void *payload, uint32_t payloadlen, const mosquitto_property *properties)
{
	struct mosquitto_evt_persist_dap_request event_data;
	struct mosquitto__callback *cb_base, *cb_next;

	if(db.shutdown){
		return;
	}

	memset(&event_data, 0, sizeof(event_data));
	event_data.data.subscriber_id = subscriber_id;
	event_data.data.op_id = op_id;
	event_data.data.deadline = deadline;
	event_data.data.payload = payload;
	event_data.data.payloadlen = payloadlen;
	event_data.data.properties = properties;

	DL_FOREACH_SAFE(callbacks, cb_base, cb_next){
		cb_base->cb(event, &event_data, cb_base->userdata);
	}
}


void dap_persist__request_add(const char *subscriber_id, uint64_t op_id, time_t deadline,
		const void *payload, uint32_t payloadlen, const mosquitto_property *properties)
{
	dap_persist__request_event(MOSQ_EVT_PERSIST_DAP_REQUEST_ADD,
			db.config->security_options.plugin_callbacks.persist_dap_request_add,
			subscriber_id, op_id, deadline, payload, payloadlen, properties);
}


void dap_persist__request_delete(const char *subscriber_id, time_t deadline)
{
	dap_persist__request_event(MOSQ_EVT_PERSIST_DAP_REQUEST_DELETE,
			db.config->security_options.plugin_callbacks.persist_dap_request_delete,
			subscriber_id, 0, deadline, NULL, 0, NULL);
}


BROKER_EXPORT int mosquitto_persist_dap_request_add(const struct mosquitto_dap_request *request)
{
	mosquitto_property *properties = NULL;

	if(request == NULL || request->subscriber_id == NULL || db.dap_request_store == NULL){
		return MOSQ_ERR_INVAL;
	}
	if(request->properties && mosquitto_property_copy_all(&properties, request->properties)){
		return MOSQ_ERR_NOMEM;
	}
	if(dap_request_store_add(db.dap_request_store, request->subscriber_id, request->op_id, request->deadline,
			request->payload, request->payloadlen, properties)){
		mosquitto_property_free_all(&properties);
		return MOSQ_ERR_NOMEM;
	}
	return MOSQ_ERR_SUCCESS;
}


BROKER_EXPORT int mosquitto_persist_dap_flow_add(const struct mosquitto_dap_flow *flow)
{
	if(flow == NULL || flow->publisher_id == NULL || flow->topic == NULL || flow->subscriber_id == NULL){
		return MOSQ_ERR_INVAL;
	}
	return dr__restore_flow(flow->publisher_id, flow->topic, flow->subscriber_id, flow->purposes,
			flow->first_time, flow->last_time);
}


void dap_persist__tracked_op_add(uint64_t op_id, const char *publisher_id,
		const char *const *expected_subs, size_t num_expected, time_t deadline)
{
	struct mosquitto_evt_persist_dap_tracked_op event_data;
	struct mosquitto__callback *cb_base, *cb_next;
	struct mosquitto__security_options *opts;

	if(db.shutdown){
		return;
	}

	opts = &db.config->security_options;
	memset(&event_data, 0, sizeof(event_data));
	event_data.data.op_id = op_id;
	event_data.data.publisher_id = publisher_id;
	event_data.data.deadline = deadline;
	event_data.data.expected_subs = expected_subs;
	event_data.data.num_expected = num_expected;

	DL_FOREACH_SAFE(opts->plugin_callbacks.persist_dap_tracked_op_add, cb_base, cb_next){
		cb_base->cb(MOSQ_EVT_PERSIST_DAP_TRACKED_OP_ADD, &event_data, cb_base->userdata);
	}
}


void dap_persist__tracked_op_response(uint64_t op_id, const char *subscriber_id)
{
	struct mosquitto_evt_persist_dap_tracked_op event_data;
	struct mosquitto__callback *cb_base, *cb_next;
	struct mosquitto__security_options *opts;

	if(db.shutdown){
		return;
	}

	opts = &db.config->security_options;
	memset(&event_data, 0, sizeof(event_data));
	event_data.data.op_id = op_id;
	event_data.subscriber_id = subscriber_id;

	DL_FOREACH_SAFE(opts->plugin_callbacks.persist_dap_tracked_op_response, cb_base, cb_next){
		cb_base->cb(MOSQ_EVT_PERSIST_DAP_TRACKED_OP_RESPONSE, &event_data, cb_base->userdata);
	}
}


void dap_persist__tracked_op_delete(uint64_t op_id)
{
	struct mosquitto_evt_persist_dap_tracked_op event_data;
	struct mosquitto__callback *cb_base, *cb_next;
	struct mosquitto__security_options *opts;

	if(db.shutdown){
		return;
	}

	opts = &db.config->security_options;
	memset(&event_data, 0, sizeof(event_data));
	event_data.data.op_id = op_id;

	DL_FOREACH_SAFE(opts->plugin_callbacks.persist_dap_tracked_op_delete, cb_base, cb_next){
		cb_base->cb(MOSQ_EVT_PERSIST_DAP_TRACKED_OP_DELETE, &event_data, cb_base->userdata);
	}
}


time_t dap_persist__recv_time(const mosquitto_property *properties)
{
	const mosquitto_property *p = properties;
	time_t recv_time = 0;
	char *name, *value;

	while((p = mosquitto_property_read_string_pair(p, MQTT_PROP_USER_PROPERTY, &name, &value, false)) != NULL){
		/* name/value are NULL when the key/value is empty. */
		bool found = name && !strcmp(name, MOSQ_DAP_TIMESTAMP_KEY);
		if(found && value){
			recv_time = (time_t)strtoll(value, NULL, 10);
		}
		mosquitto_FREE(name);
		mosquitto_FREE(value);
		if(found) break;
		p = mosquitto_property_next(p);
	}
	return recv_time;
}


/* Restore entry points: write DAP state directly, without firing events. */

BROKER_EXPORT int mosquitto_persist_dap_op_add(const struct mosquitto_dap_op *op)
{
	if(op == NULL || op->publisher_id == NULL || op->op_id == 0){
		return MOSQ_ERR_INVAL;
	}
	if(op->op_type != DAP_OP_DELETE && op->op_type != DAP_OP_RESTRICT){
		return MOSQ_ERR_INVAL;
	}
	if(db.dap_pending_ops == NULL){
		return MOSQ_ERR_NOMEM;
	}

	/* Persistence keeps whole seconds: order a restored operation at the end of its
	 * second, so it still covers every message received before it, and order every
	 * message received from now on after it. */
	uint64_t order = (uint64_t)op->timestamp * 1000000000ULL + 999999999ULL;
	/* Rows written before deadlines were persisted get the default one. */
	time_t deadline = op->deadline ? op->deadline : op->timestamp + MOSQ_DAP_DEFAULT_DEADLINE_SECS;
	if(dap_pending_ops_restore_operation(db.dap_pending_ops, op->op_id, op->publisher_id,
			(enum dap_op_type)op->op_type, order, deadline,
			op->topic_filters, op->purpose_filters, op->client_filters)){

		return MOSQ_ERR_INVAL;
	}
	if(order > db.dap_last_order){
		db.dap_last_order = order;
	}
	return MOSQ_ERR_SUCCESS;
}


BROKER_EXPORT int mosquitto_persist_dap_tracked_op_add(const struct mosquitto_dap_tracked_op *op)
{
	if(op == NULL || op->publisher_id == NULL || op->op_id == 0){
		return MOSQ_ERR_INVAL;
	}
	if(op->num_expected > 0 && op->expected_subs == NULL){
		return MOSQ_ERR_INVAL;
	}
	if(db.dap_pending_ops == NULL || db.dap_op_requester == NULL || db.dap_deadline_tracker == NULL){
		return MOSQ_ERR_NOMEM;
	}

	/* HISTORY/UPDATE ids only live here. */
	dap_pending_ops_reserve_op_id(db.dap_pending_ops, op->op_id);

	if(dap_op_requester_record(db.dap_op_requester, op->op_id, op->publisher_id)){
		return MOSQ_ERR_NOMEM;
	}
	if(op->settled){
		return MOSQ_ERR_SUCCESS;
	}

	if(dap_deadline_tracker_register_pending_operation(db.dap_deadline_tracker, op->op_id,
			op->publisher_id, op->expected_subs, op->num_expected, op->deadline)){

		return MOSQ_ERR_INVAL;
	}
	if(op->responded){
		for(size_t i=0; i<op->num_expected; i++){
			if(op->responded[i]){
				dap_deadline_tracker_mark_subscriber_responded(db.dap_deadline_tracker,
						op->op_id, op->expected_subs[i]);
			}
		}
	}
	return MOSQ_ERR_SUCCESS;
}
