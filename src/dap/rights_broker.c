#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include "rights_broker.h"
#include "mosquitto_broker_internal.h" 
#include "util_mosq.h"
#include "property_mosq.h"
#include "send_mosq.h"
#include "dap/dr_registry.h"
#include "dap/dap_deadline_tracker.h"
#include "dap/dap_op_requester.h"
#include "dap/dap_persist.h"
#include "dap/dap_request_store.h"
#include "dap/purpose_filters.h"

/* Paper 6.2: requests and notifications are delivered at least once. */
#define DAP_OP_QOS 1

/* Finds a client context by ID by calling db__find_context_by_id(). */
struct mosquitto *broker_find_context_by_id(const char *client_id)
{
    return db__find_context_by_id(client_id);
}

/* Removes Wills or retained messages invoking erasure. */
void handle_remove_stored_messages(const char *publisher_id)
{
    /* Remove will message for publisher*/
    struct mosquitto *pub_ctx = broker_find_context_by_id(publisher_id);
    if(pub_ctx && pub_ctx->will){
        mosquitto_FREE(pub_ctx->will->msg.topic);
        mosquitto_FREE(pub_ctx->will->msg.payload);
        mosquitto_FREE(pub_ctx->will);
        pub_ctx->will = NULL;
    }

    /* Remove retained message for publisher */
    extern struct dr_retained_entry *dr_retained_head;
    struct dr_retained_entry *cur = dr_retained_head;
    while(cur){
        if(!strcmp(cur->pub_id, publisher_id)){
            mosquitto_persist_retain_msg_delete(cur->topic);
        }
        cur = cur->next;
    }
}

void dap_op_set_reason(struct dap__op_property *dap_op_properties, const char *reason)
{
    mosquitto_FREE(dap_op_properties->op_reason);
    dap_op_properties->op_reason = mosquitto_strdup(reason);
}

void broker_send_response_success(const char *publisher_id, const char *operation, const char *corr_data, uint16_t correlation_data_len, const char *payload, char* response_topic)
{
    if(!publisher_id) return;

    char onp_topic[256];
    if (response_topic == NULL)
    {
        snprintf(onp_topic, sizeof(onp_topic), "%s/%s", MOSQ_DAP_TOPIC_ONP, publisher_id);
        response_topic = onp_topic;
    }

    mosquitto_property *props = NULL;
    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_CONSENT_KEY, "1");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_ID_KEY, "Broker");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_OP_KEY, operation);

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_STATUS_KEY, "Success");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_REASON_KEY, "");
    
    if(corr_data){
        mosquitto_property_add_binary(&props, MQTT_PROP_CORRELATION_DATA, corr_data, correlation_data_len);
    }

    if(payload)
    {
        db__messages_easy_queue_with_purpose(NULL, response_topic, MOSQ_DAP_OP_PURPOSE, DAP_OP_QOS, (uint32_t)strlen(payload), payload, false, 0, &props);
    }
    else
    {
        db__messages_easy_queue_with_purpose(NULL, response_topic, MOSQ_DAP_OP_PURPOSE, DAP_OP_QOS, 0, NULL, false, 0, &props);
    }

    mosquitto_property_free_all(&props);
}

/* Acknowledge a validated pending op to ONP/<publisher_id>, carrying the
 * broker-assigned numeric op id and the absolute deadline (epoch seconds). The final
 * Success/Failure is settled later by the status path / deadline sweep, not here. */
void broker_send_response_pending(const char *publisher_id, struct dap__op_property* dap_op_properties, time_t deadline)
{
    if(!publisher_id) return;
    char onp_topic[256];
    snprintf(onp_topic, sizeof(onp_topic), "%s/%s", MOSQ_DAP_TOPIC_ONP, publisher_id);

    mosquitto_property *props = NULL;
    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_CONSENT_KEY, "1");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_ID_KEY, "Broker");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_OP_KEY, dap_op_properties->op_id);

    char opid_buf[32];
    snprintf(opid_buf, sizeof(opid_buf), "%llu", (unsigned long long)dap_op_properties->op_id_num);
    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_OP_ID_KEY, opid_buf);

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_STATUS_KEY, "Pending");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_REASON_KEY, "Awaiting subscriber responses");

    char deadline_buf[32];
    snprintf(deadline_buf, sizeof(deadline_buf), "%lld", (long long)deadline);
    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY, MOSQ_DAP_DEADLINE_KEY, deadline_buf);

    if(dap_op_properties->correlation_data){
        mosquitto_property_add_binary(&props, MQTT_PROP_CORRELATION_DATA, dap_op_properties->correlation_data, dap_op_properties->correlation_data_len);
    }

    db__messages_easy_queue_with_purpose(NULL, onp_topic, MOSQ_DAP_OP_PURPOSE, DAP_OP_QOS, 0, NULL, false, 0, &props);
    mosquitto_property_free_all(&props);
}

/* Sends a final failure for a request. It mirrors broker_send_response_success: the
 * failure goes to the requester's response topic (op_resp/<publisher_id>) so the
 * Success and Failure outcomes of a request land on the same channel and the requester
 * correlates either by its correlation data. When the request carried no response topic
 * (response_topic == NULL) it falls back to ONP/<publisher_id>. */
void broker_send_response_failure(const char *publisher_id, struct dap__op_property* dap_op_properties)
{
    if(!publisher_id) return;
    char onp_topic[256];
    const char *response_topic = dap_op_properties->response_topic;
    if(response_topic == NULL)
    {
        snprintf(onp_topic, sizeof(onp_topic), "%s/%s", MOSQ_DAP_TOPIC_ONP, publisher_id);
        response_topic = onp_topic;
    }

    mosquitto_property *props = NULL;
    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_CONSENT_KEY, "1");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_ID_KEY, "Broker");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_OP_KEY, dap_op_properties->op_id);

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_STATUS_KEY, "Failure");

    if(dap_op_properties->op_reason){
        mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
            MOSQ_DAP_REASON_KEY, dap_op_properties->op_reason);
    }

    if(dap_op_properties->correlation_data){
        mosquitto_property_add_binary(&props, MQTT_PROP_CORRELATION_DATA, dap_op_properties->correlation_data, dap_op_properties->correlation_data_len);
    }

    db__messages_easy_queue_with_purpose(NULL, response_topic, MOSQ_DAP_OP_PURPOSE, DAP_OP_QOS, 0, NULL, false, 0, &props);
    mosquitto_property_free_all(&props);
}

/* OP_REQ/<sub_id>, allocated. */
static char *dap__request_topic(const char *sub_id)
{
    size_t len = strlen(MOSQ_DAP_TOPIC_ORS) + 1 + strlen(sub_id) + 1;
    char *topic = mosquitto_malloc(len);
    if(topic) snprintf(topic, len, "%s/%s", MOSQ_DAP_TOPIC_ORS, sub_id);
    return topic;
}

/* True when sub_id is connected and subscribed to its request topic with an SP
 * that operation messages are permitted for. */
static bool dap__request_topic_ready(const char *sub_id, const char *topic)
{
    struct mosquitto *ctx = broker_find_context_by_id(sub_id);

    if(!ctx || ctx->state != mosq_cs_active) return false;
    for(int i = 0; i < ctx->subs_capacity; i++){
        struct mosquitto__subleaf *leaf = ctx->subs[i];
        if(leaf && !strcmp(leaf->topic_filter, topic)
                && purpose_mp_permits(MOSQ_DAP_OP_PURPOSE, leaf->purpose_filters, leaf->purpose_filter_count)){
            return true;
        }
    }
    return false;
}

/* The properties of a request forwarded to a subscriber. */
static mosquitto_property *dap__request_properties(struct mosquitto_base_msg *msg_data, struct dap__op_property *dap_op_properties)
{
    mosquitto_property *props = NULL;
    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_CONSENT_KEY, "1");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_ID_KEY, msg_data->source_id);

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_OP_KEY, dap_op_properties->op_id);

    /* Carry the broker-assigned numeric id so the subscriber can reference it in its
     * status reply. */
    if(dap_op_properties->op_id_num != 0){
        char opid_buf[32];
        snprintf(opid_buf, sizeof(opid_buf), "%llu", (unsigned long long)dap_op_properties->op_id_num);
        mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
            MOSQ_DAP_OP_ID_KEY, opid_buf);
    }

    /* Propagate the broker's receipt timestamp (paper 3.3) onto the forwarded
     * request so subscribers share the same reference time the operation is
     * scoped against. It was stamped onto the message's properties at PUBLISH
     * receipt (handle_publish.c); copy that value here, since the forwarder has
     * only the message, not the store entry that carries dap_recv_time. */
    if(msg_data->properties){
        const mosquitto_property *p = msg_data->properties;
        char *pn = NULL, *pv = NULL;
        while((p = mosquitto_property_read_string_pair(p, MQTT_PROP_USER_PROPERTY, &pn, &pv, false)) != NULL){
            bool is_ts = (pn && !strcmp(pn, MOSQ_DAP_TIMESTAMP_KEY) && pv);
            if(is_ts){
                mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
                    MOSQ_DAP_TIMESTAMP_KEY, pv);
            }
            mosquitto_FREE(pn);
            mosquitto_FREE(pv);
            if(is_ts) break;
            p = p->next;
        }
    }

    if(dap_op_properties->correlation_data){
        mosquitto_property_add_binary(&props, MQTT_PROP_CORRELATION_DATA, dap_op_properties->correlation_data, dap_op_properties->correlation_data_len);
    }

    if(dap_op_properties->response_topic){
        mosquitto_property_add_string(&props, MQTT_PROP_RESPONSE_TOPIC, dap_op_properties->response_topic);
    }
    return props;
}

/* Forward a request to sub_id's request topic, or hold it until the deadline when
 * the subscriber is not connected and subscribed to it (paper 6.3). */
static void dap__forward_request(const char *sub_id, struct mosquitto_base_msg *msg_data,
    struct dap__op_property *dap_op_properties, time_t deadline)
{
    char *topic = dap__request_topic(sub_id);
    mosquitto_property *props;

    if(!topic) return;
    props = dap__request_properties(msg_data, dap_op_properties);
    /* A request expires at its deadline, or earlier with the message. */
    time_t until = deadline;
    if(msg_data->expiry_time && msg_data->expiry_time < until){
        until = msg_data->expiry_time;
    }
    if(dap__request_topic_ready(sub_id, topic)){
        uint32_t expiry_interval = until > db.now_real_s ? (uint32_t)(until - db.now_real_s) : 0;
        db__messages_easy_queue_with_purpose(NULL, topic, MOSQ_DAP_OP_PURPOSE, DAP_OP_QOS,
                msg_data->payloadlen, msg_data->payload, false, expiry_interval, &props);
    }else if(db.dap_request_store){
        if(dap_request_store_add(db.dap_request_store, sub_id, dap_op_properties->op_id_num, until,
                msg_data->payload, msg_data->payloadlen, props) == 0){
            props = NULL;
        }
    }
    mosquitto_property_free_all(&props);
    mosquitto_FREE(topic);
}

void broker_deliver_held_requests(struct mosquitto *context)
{
    struct dap_stored_request *held;
    char *topic;

    if(!context->id || !db.dap_request_store || !dap_request_store_has(db.dap_request_store, context->id)){
        return;
    }
    topic = dap__request_topic(context->id);
    if(!topic) return;
    if(dap__request_topic_ready(context->id, topic)){
        held = dap_request_store_take(db.dap_request_store, context->id, db.now_real_s);
        for(struct dap_stored_request *r = held; r; r = r->next){
            db__messages_easy_queue_with_purpose(NULL, topic, MOSQ_DAP_OP_PURPOSE, DAP_OP_QOS,
                    r->payloadlen, r->payload, false, (uint32_t)(r->deadline - db.now_real_s), &r->properties);
        }
        dap_request_store_free_list(held);
    }
    mosquitto_FREE(topic);
}

/* Subscriber-involving dispatch (DELETE/RESTRICT/HISTORY/UPDATE). The relevant set
 * was computed by dr__find_relevant_subscribers (topic/purpose/client filters +
 * OpBefore/OpAfter bounds). (1) forward the request to each relevant subscriber on
 * its ORS, carrying the numeric op id, holding it for one that is offline until it
 * subscribes again; (2) register the op with the deadline tracker against the full
 * relevant set, so any subscriber that has not responded by the deadline is reported;
 * (3) echo a Pending ack with the op id + deadline back to the requester. */
void broker_dispatch_pending_operation(const char *publisher_id, struct dr_sublist *relevant, struct mosquitto_base_msg *msg_data, struct dap__op_property *dap_op_properties, time_t deadline)
{
    if(!publisher_id) return;

    /* Count the relevant subs for the tracker's borrowed id array. */
    size_t n = 0;
    for(struct dr_sublist *s = relevant; s; s = s->next) n++;

    /* Paper 5.3: "If no relevant subscriptions are identified, the broker sends a
     * failure message to the requester." No subscriber ever received matching data
     * from this publisher, so the operation cannot be carried out - reject it rather
     * than echoing Pending (which would silently expire) or Success. The requester
     * correlates via correlation data, like the other immediate responses. */
    if(n == 0){
        dap_op_set_reason(dap_op_properties, "No relevant subscribers found");
        broker_send_response_failure(publisher_id, dap_op_properties);
        return;
    }

    const char **ids = mosquitto_calloc(n, sizeof(char*));
    size_t i = 0;
    for(struct dr_sublist *s = relevant; s; s = s->next){
        if(ids) ids[i] = s->sub_id; /* borrowed; the tracker copies on register */
        i++;
        /* (1) Forward to the relevant subscriber, or hold it until they can receive it. */
        dap__forward_request(s->sub_id, msg_data, dap_op_properties, deadline);
    }

    /* (2) Track the op so the main-loop sweep can report unresponded subs at expiry.
     * Guard the count against a failed ids allocation so a NULL array is never paired
     * with a non-zero count (the op then tracks no expected subs rather than crashing). */
    if(db.dap_deadline_tracker){
        if(dap_deadline_tracker_register_pending_operation(db.dap_deadline_tracker, dap_op_properties->op_id_num,
                publisher_id, ids, ids ? n : 0, deadline) == 0){
            dap_persist__tracked_op_add(dap_op_properties->op_id_num, publisher_id,
                ids, ids ? n : 0, deadline);
        }
    }
    mosquitto_FREE(ids);

    /* Remember who requested this op id so an inbound status notification - even one
     * that arrives after the deadline has passed - can be routed back to the requester. */
    if(db.dap_op_requester){
        dap_op_requester_record(db.dap_op_requester, dap_op_properties->op_id_num, publisher_id);
    }

    /* (3) Echo the validated request back to the requester with op id + deadline. */
    broker_send_response_pending(publisher_id, dap_op_properties, deadline);
}

/* Deadline-expiry notification, built from the deadline tracker's expired-op result
 * (op id + publisher + the ids that never responded). Sent to ONP/<publisher_id> as
 * a Failure keyed by DAP-OpId, listing the unresponded subscribers in
 * DAP-UnreachedClients. */
void broker_send_deadline_failure(uint64_t op_id, const char *publisher_id,
    char **unresponded_subs, size_t num_unresponded)
{
    if(!publisher_id) return;
    char onp_topic[256];
    snprintf(onp_topic, sizeof(onp_topic), "%s/%s", MOSQ_DAP_TOPIC_ONP, publisher_id);

    mosquitto_property *props = NULL;
    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_CONSENT_KEY, "1");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_ID_KEY, "Broker");

    char opid_buf[32];
    snprintf(opid_buf, sizeof(opid_buf), "%llu", (unsigned long long)op_id);
    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_OP_ID_KEY, opid_buf);

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_STATUS_KEY, "Failure");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_REASON_KEY, "Operation deadline expired");

    if(unresponded_subs && num_unresponded > 0){
        /* Space-separated ids, sized to fit them all. */
        size_t len = 1;
        for(size_t i = 0; i < num_unresponded; i++){
            len += strlen(unresponded_subs[i]) + 1;
        }
        char *contacts = mosquitto_malloc(len);
        if(contacts){
            size_t pos = 0;
            for(size_t i = 0; i < num_unresponded; i++){
                size_t id_len = strlen(unresponded_subs[i]);
                memcpy(&contacts[pos], unresponded_subs[i], id_len);
                pos += id_len;
                contacts[pos++] = ' ';
            }
            contacts[pos] = '\0';
            mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
                MOSQ_DAP_UNREACHED_CLIENTS_KEY, contacts);
            mosquitto_FREE(contacts);
        }
    }

    db__messages_easy_queue_with_purpose(NULL, onp_topic, MOSQ_DAP_OP_PURPOSE, DAP_OP_QOS, 0, NULL, false, 0, &props);
    mosquitto_property_free_all(&props);
}

/* All-responded notification, built from the deadline tracker's expired-op result
 * (op id + publisher) when every expected subscriber responded before the deadline.
 * Sent to ONP/<publisher_id> as a Success keyed by DAP-OpId, mirroring
 * broker_send_deadline_failure. This can only fire once an inbound status path marks
 * subscribers responded; until then a tracked op with >=1 relevant subscriber always
 * reaches the deadline unresponded and takes the failure branch. */
void broker_send_deadline_success(uint64_t op_id, const char *publisher_id)
{
    if(!publisher_id) return;
    char onp_topic[256];
    snprintf(onp_topic, sizeof(onp_topic), "%s/%s", MOSQ_DAP_TOPIC_ONP, publisher_id);

    mosquitto_property *props = NULL;
    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_CONSENT_KEY, "1");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_ID_KEY, "Broker");

    char opid_buf[32];
    snprintf(opid_buf, sizeof(opid_buf), "%llu", (unsigned long long)op_id);
    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_OP_ID_KEY, opid_buf);

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_STATUS_KEY, "Success");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_REASON_KEY, "All subscribers responded");

    db__messages_easy_queue_with_purpose(NULL, onp_topic, MOSQ_DAP_OP_PURPOSE, DAP_OP_QOS, 0, NULL, false, 0, &props);
    mosquitto_property_free_all(&props);
}

/* Forward a subscriber's status notification to the requester on ONP/<requester_id>.
 * The broker relays the responding subscriber's id, op id, status, reason and any
 * payload/correlation data so the requester sees each response as it arrives. */
void broker_forward_status_to_requester(const char *requester_id, struct dap__op_property *dap_op_properties, const char *responder_id,
    const void *payload, uint32_t payloadlen)
{
    if(!requester_id || !dap_op_properties->op_status) return;
    char onp_topic[256];
    snprintf(onp_topic, sizeof(onp_topic), "%s/%s", MOSQ_DAP_TOPIC_ONP, requester_id);

    mosquitto_property *props = NULL;
    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_CONSENT_KEY, "1");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_ID_KEY, responder_id ? responder_id : "");

    if(dap_op_properties->op_id){
        mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
            MOSQ_DAP_OP_KEY, dap_op_properties->op_id);
    }

    char opid_buf[32];
    snprintf(opid_buf, sizeof(opid_buf), "%llu", (unsigned long long)dap_op_properties->op_id_num);
    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_OP_ID_KEY, opid_buf);

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_STATUS_KEY, dap_op_properties->op_status);

    if(dap_op_properties->op_reason){
        mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
            MOSQ_DAP_REASON_KEY, dap_op_properties->op_reason);
    }

    if(dap_op_properties->correlation_data){
        mosquitto_property_add_binary(&props, MQTT_PROP_CORRELATION_DATA, dap_op_properties->correlation_data, dap_op_properties->correlation_data_len);
    }

    db__messages_easy_queue_with_purpose(NULL, onp_topic, MOSQ_DAP_OP_PURPOSE, DAP_OP_QOS,
        payloadlen, payload, false, 0, &props);
    mosquitto_property_free_all(&props);
}