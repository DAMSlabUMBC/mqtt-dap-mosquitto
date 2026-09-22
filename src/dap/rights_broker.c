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

/* Finds a client context by ID by calling db__find_context_by_id(). */
struct mosquitto *broker_find_context_by_id(const char *client_id)
{
    return db__find_context_by_id(client_id);
}

/* Checks if a subscriber is online (active state). */
bool is_sub_online(const char *sub_id)
{
    struct mosquitto *ctx = broker_find_context_by_id(sub_id);
    if(!ctx) return false;
    return (ctx->state == mosq_cs_active);
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

void broker_send_response_success(const char *publisher_id, const char *operation, const char *corr_data, uint16_t correlation_data_len, const char *payload, char* response_topic)
{
    if(!publisher_id) return;

    if (response_topic == NULL)
    {
        char onp_topic[256];
        snprintf(onp_topic, sizeof(onp_topic), "%s/%s", MOSQ_DAP_TOPIC_ONP, publisher_id);
        response_topic = mosquitto_strdup(onp_topic);
    }

    mosquitto_property *props = NULL;
    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_CONSENT_KEY, "1");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_ID_KEY, "Broker");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_OP_KEY, mosquitto_strdup(operation));

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_STATUS_KEY, "Success");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_REASON_KEY, "");
    
    if(corr_data){
        mosquitto_property_add_binary(&props, MQTT_PROP_CORRELATION_DATA, corr_data, correlation_data_len);
    }

    if(payload)
    {
        db__messages_easy_queue_with_purpose(NULL, response_topic, MOSQ_DAP_OP_PURPOSE, 0, (uint32_t)strlen(payload), payload, false, 0, &props);
    }
    else
    {
        db__messages_easy_queue_with_purpose(NULL, response_topic, MOSQ_DAP_OP_PURPOSE, 0, 0, NULL, false, 0, &props);
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

    db__messages_easy_queue_with_purpose(NULL, onp_topic, MOSQ_DAP_OP_PURPOSE, 0, 0, NULL, false, 0, &props);
    mosquitto_property_free_all(&props);
}

/* Sends a final failure for a request. It mirrors broker_send_response_success: the
 * failure goes to the requester's response topic (op_resp/<publisher_id>) so the
 * Success and Failure outcomes of a request land on the same channel and the requester
 * correlates either by its correlation data. When the request carried no response topic
 * (response_topic == NULL) it falls back to ONP/<publisher_id>. */
void broker_send_response_failure(const char *publisher_id, struct dap__op_property* dap_op_properties, struct subscriber_list *unreached_subs)
{
    if(!publisher_id) return;
    char onp_topic[256];
    if(dap_op_properties->response_topic == NULL)
    {
        snprintf(onp_topic, sizeof(onp_topic), "%s/%s", MOSQ_DAP_TOPIC_ONP, publisher_id);
        dap_op_properties->response_topic = onp_topic;
    }

    mosquitto_property *props = NULL;
    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_CONSENT_KEY, "1");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_ID_KEY, "Broker");

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_OP_KEY, mosquitto_strdup(dap_op_properties->op_id));

    mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
        MOSQ_DAP_STATUS_KEY, "Failure");

    if(dap_op_properties->op_reason){
        mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
            MOSQ_DAP_REASON_KEY, dap_op_properties->op_reason);
    }

    if(dap_op_properties->correlation_data){
        mosquitto_property_add_binary(&props, MQTT_PROP_CORRELATION_DATA, dap_op_properties->correlation_data, dap_op_properties->correlation_data_len);
    }

    if(unreached_subs)
    {
        char contact_buf[256];
        contact_buf[0] = '\0';
        while(unreached_subs){
            strncat(contact_buf, unreached_subs->sub_id, sizeof(contact_buf)-1);
            strncat(contact_buf, " ", sizeof(contact_buf)-1);
            unreached_subs = unreached_subs->next;
        }
        mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
            MOSQ_DAP_UNREACHED_CLIENTS_KEY, contact_buf);
    }

    db__messages_easy_queue_with_purpose(NULL, dap_op_properties->response_topic, MOSQ_DAP_OP_PURPOSE, 0, 0, NULL, false, 0, &props);
    mosquitto_property_free_all(&props);
}

/* Finds (sub_id, topic) for all subscriptions that got data from publisher_id. */
struct subscription_list *find_subscriptions_for_publisher(const char *publisher_id)
{
    struct subscription_list *head = NULL;
    struct subscription_list *check_ptr = NULL;
    extern struct dr_entry *dr_head;
    struct dr_entry *cur = dr_head;

    while(cur){
        if(!strcmp(cur->pub_id, publisher_id)){
            struct dr_sublist *s = cur->sub_list;
            while(s){

                bool found = false;
                check_ptr = head;
                while(check_ptr)
                {
                    /* We've already added this subscriber */
                    if(!strcmp(check_ptr->subscriber_id, s->sub_id))
                    {
                        found = true;
                        break;
                    }

                    check_ptr = check_ptr->next;
                }

                if (!found) 
                {
                    struct subscription_list *node = mosquitto_calloc(1, sizeof(*node));
                    if(!node) return head;
                    node->subscriber_id = mosquitto_strdup(s->sub_id);
                    node->topic        = mosquitto_strdup(cur->topic);
                    node->next         = head;
                    head = node;
                }

                s = s->next;
            }
        }
        cur = cur->next;
    }
    return head;
}

/* Finds subs with matching topic if data_filter is in the topic name. */
struct subscriber_list *find_subscribers_with_data(const char *publisher_id, const char *data_filter)
{
    struct subscriber_list *head = NULL;
    struct subscriber_list *check_ptr = NULL;
    extern struct dr_entry *dr_head;
    struct dr_entry *cur = dr_head;

    while(cur){
        if(!strcmp(cur->pub_id, publisher_id)){
            if(data_filter && (strstr(cur->topic, data_filter) || !strcmp(data_filter, MOSQ_DAP_ALLOW_ALL_FILTER))) {
                struct dr_sublist *s = cur->sub_list;
                while(s){

                    bool found = false;
                    check_ptr = head;
                    while(check_ptr)
                    {
                        /* We've already added this subscriber */
                        if(!strcmp(check_ptr->sub_id, s->sub_id))
                        {
                            found = true;
                            break;
                        }

                        check_ptr = check_ptr->next;
                    }

                    if (!found) 
                    {
                        /* New subscriber, add */
                        struct subscriber_list *node = mosquitto_calloc(1, sizeof(*node));
                        if(!node) return head;
                        node->sub_id = mosquitto_strdup(s->sub_id);
                        node->next   = head;
                        head         = node;
                    }
                    s = s->next;
                }
            }
        }
        cur = cur->next;
    }
    return head;
}

/* Publishes a right request to RRS/<sub> if online, else collects them offline. */
struct subscriber_list *forward_request_to_connected(struct subscriber_list *sub_list, struct mosquitto_base_msg *msg_data, struct dap__op_property *dap_op_properties)
{
    struct subscriber_list *offline_head = NULL;

    while(sub_list){
        if(is_sub_online(sub_list->sub_id)){
            char ors_topic[256];
            snprintf(ors_topic, sizeof(ors_topic), "%s/%s", MOSQ_DAP_TOPIC_ORS, sub_list->sub_id);

            mosquitto_property *props = NULL;
            mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
                MOSQ_DAP_CONSENT_KEY, "1");

            mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
                MOSQ_DAP_ID_KEY, mosquitto_strdup(msg_data->source_id));

            mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
                MOSQ_DAP_OP_KEY, mosquitto_strdup(dap_op_properties->op_id));

            /* For a pending op (DELETE/RESTRICT) carry the broker-assigned numeric id so
             * the subscriber can reference it in its status reply. */
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

            db__messages_easy_queue_with_purpose(NULL, mosquitto_strdup(ors_topic), MOSQ_DAP_OP_PURPOSE, msg_data->qos, msg_data->payloadlen, msg_data->payload, msg_data->retain, (uint32_t)msg_data->expiry_time, &props);
        } else {
            struct subscriber_list *off = mosquitto_calloc(1, sizeof(*off));
            off->sub_id = mosquitto_strdup(sub_list->sub_id);
            off->next   = offline_head;
            offline_head= off;
        }
        sub_list = sub_list->next;
    }
    return offline_head;
}

/* Pending-op dispatch (DELETE/RESTRICT). The relevant set was computed by
 * dr__find_relevant_subscribers (topic/purpose/client filters + OpBefore/OpAfter
 * bounds). (1) forward the request to whoever is online now on their ORS, carrying
 * the numeric op id; (2) register the op with the deadline tracker against the full
 * relevant set, so any subscriber that has not responded by the deadline is reported;
 * (3) echo a Pending ack with the op id + deadline back to the requester. Offline
 * relevant subs are not forwarded now; they remain unresponded and surface as
 * unreached clients when the deadline passes. */
void broker_dispatch_pending_operation(const char *publisher_id, struct dr_sublist *relevant, struct mosquitto_base_msg *msg_data, struct dap__op_property *dap_op_properties, time_t deadline)
{
    if(!publisher_id) return;

    /* Count the relevant subs, then build both a transient subscriber_list (so the
     * existing ORS forwarder can be reused) and a borrowed id array for the tracker. */
    size_t n = 0;
    for(struct dr_sublist *s = relevant; s; s = s->next) n++;

    /* Paper 5.3: "If no relevant subscriptions are identified, the broker sends a
     * failure message to the requester." No subscriber ever received matching data
     * from this publisher, so the operation cannot be carried out - reject it rather
     * than echoing Pending (which would silently expire) or Success. The requester
     * correlates via correlation data, like the other immediate responses. */
    if(n == 0){
        dap_op_properties->op_reason = "No relevant subscribers found";
        broker_send_response_failure(publisher_id, dap_op_properties, NULL);
        return;
    }

    const char **ids = NULL;
    if(n > 0){
        ids = mosquitto_calloc(n, sizeof(char*));
    }

    struct subscriber_list *fwd = NULL;
    size_t i = 0;
    for(struct dr_sublist *s = relevant; s; s = s->next){
        struct subscriber_list *node = mosquitto_calloc(1, sizeof(*node));
        if(node){
            node->sub_id = mosquitto_strdup(s->sub_id);
            node->next   = fwd;
            fwd          = node;
        }
        if(ids) ids[i] = s->sub_id; /* borrowed; the tracker copies on register */
        i++;
    }

    /* (1) Forward to the online relevant subs; discard the offline list - the tracker,
     * not an immediate failure, now owns the "didn't reach them" outcome. */
    struct subscriber_list *offline = forward_request_to_connected(fwd, msg_data, dap_op_properties);
    while(fwd){
        struct subscriber_list *t = fwd->next;
        mosquitto_FREE(fwd->sub_id);
        mosquitto_FREE(fwd);
        fwd = t;
    }
    while(offline){
        struct subscriber_list *t = offline->next;
        mosquitto_FREE(offline->sub_id);
        mosquitto_FREE(offline);
        offline = t;
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
        char contact_buf[256];
        contact_buf[0] = '\0';
        for(size_t i = 0; i < num_unresponded; i++){
            size_t used = strlen(contact_buf);
            if(used + 1 >= sizeof(contact_buf)) break;
            strncat(contact_buf, unresponded_subs[i], sizeof(contact_buf) - 1 - used);
            used = strlen(contact_buf);
            if(used + 1 < sizeof(contact_buf)){
                strncat(contact_buf, " ", sizeof(contact_buf) - 1 - used);
            }
        }
        mosquitto_property_add_string_pair(&props, MQTT_PROP_USER_PROPERTY,
            MOSQ_DAP_UNREACHED_CLIENTS_KEY, contact_buf);
    }

    db__messages_easy_queue_with_purpose(NULL, onp_topic, MOSQ_DAP_OP_PURPOSE, 0, 0, NULL, false, 0, &props);
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

    db__messages_easy_queue_with_purpose(NULL, onp_topic, MOSQ_DAP_OP_PURPOSE, 0, 0, NULL, false, 0, &props);
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

    db__messages_easy_queue_with_purpose(NULL, onp_topic, MOSQ_DAP_OP_PURPOSE, 0,
        payloadlen, payload, false, 0, &props);
    mosquitto_property_free_all(&props);
}