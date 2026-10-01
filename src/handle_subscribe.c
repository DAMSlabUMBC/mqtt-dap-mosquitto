/*
Copyright (c) 2009-2021 Roger Light <roger@atchoo.org>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License 2.0
and Eclipse Distribution License v1.0 which accompany this distribution.

The Eclipse Public License is available at
   https://www.eclipse.org/legal/epl-2.0/
and the Eclipse Distribution License is available at
  http://www.eclipse.org/org/documents/edl-v10.php.

SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause

Contributors:
   Roger Light - initial implementation and documentation.
*/

#include "config.h"

#include <stdio.h>
#include <string.h>

#include "mosquitto_broker_internal.h"
#include "mosquitto/mqtt_protocol.h"
#include "packet_mosq.h"
#include "property_common.h"
#include "property_mosq.h"
#include "dap/mp_registry.h" 
#include "dap/ri_registry.h" 
#include "dap/dr_registry.h" 
#include "dap/rights_registry.h"
#include "dap/rights_broker.h"
#include "dap/purpose_filters.h"
#include "dap/dap_topics.h"

int handle__subscribe(struct mosquitto *context)
{
	int rc = 0;
	int rc2;
	uint16_t mid;
	uint8_t qos;
	uint8_t retain_handling = 0;
	uint8_t *payload = NULL, *tmp_payload;
	uint32_t payloadlen = 0;
	size_t len;
	uint16_t slen;
	char *sub_mount;
	mosquitto_property *properties = NULL;
	bool allowed;
	struct mosquitto_subscription sub;
	uint32_t subscription_identifier = 0;
	/* Purpose filtering (MQTT v5 only) */
	uint32_t purpose_filter_count = 0;
	char* purpose_filters[MOSQ_DAP_MAX_FILTERS_PER_SUB];
	/* MQTT-DAP (paper 4.3): did this v5 packet declare an SP? User properties are
	 * packet-scoped in MQTT v5, so this is a packet-level fact applied per subscription. */
	bool has_sp = false;

	if(!context){
		return MOSQ_ERR_INVAL;
	}

	if(context->state != mosq_cs_active){
		log__printf(NULL, MOSQ_LOG_INFO, "Protocol error from %s: SUBSCRIBE before session is active.", context->id);
		return MOSQ_ERR_PROTOCOL;
	}
	if(context->in_packet.command != (CMD_SUBSCRIBE|2)){
		return MOSQ_ERR_MALFORMED_PACKET;
	}

	log__printf(NULL, MOSQ_LOG_DEBUG, "Received SUBSCRIBE from %s", context->id);

	if(context->protocol != mosq_p_mqtt31){
		if((context->in_packet.command&0x0F) != 0x02){
			return MOSQ_ERR_MALFORMED_PACKET;
		}
	}
	if(packet__read_uint16(&context->in_packet, &mid)){
		return MOSQ_ERR_MALFORMED_PACKET;
	}
	if(mid == 0){
		return MOSQ_ERR_MALFORMED_PACKET;
	}

	if(context->protocol == mosq_p_mqtt5){
		rc = property__read_all(CMD_SUBSCRIBE, &context->in_packet, &properties);
		if(rc){
			/* FIXME - it would be better if property__read_all() returned
			 * MOSQ_ERR_MALFORMED_PACKET, but this is would change the library
			 * return codes so needs doc changes as well. */
			if(rc == MOSQ_ERR_PROTOCOL){
				log__printf(NULL, MOSQ_LOG_INFO, "Protocol error from %s: SUBSCRIBE packet with invalid properties.", context->id);
				return MOSQ_ERR_MALFORMED_PACKET;
			}else{
				return rc;
			}
		}

		if(mosquitto_property_read_varint(properties, MQTT_PROP_SUBSCRIPTION_IDENTIFIER,
				&subscription_identifier, false)){

			/* If the identifier was force set to 0, this is an error */
			if(subscription_identifier == 0){
				mosquitto_property_free_all(&properties);
				return MOSQ_ERR_MALFORMED_PACKET;
			}
		}

		/* Check for purpose filtering which requires registration at subscribe-time by the subscriber */
		// Since there can be multiple user properties, loop through entire list
		const mosquitto_property* curr_prop_ptr = properties;
		while(curr_prop_ptr)
		{
			/* Parse current property; name/value are NULL when empty */
			char* name;
			char* value;

			/* This automatically increments the curr_prop_ptr to the next user property */
			curr_prop_ptr = mosquitto_property_read_string_pair(curr_prop_ptr, MQTT_PROP_USER_PROPERTY, &name, &value, false);
			if(curr_prop_ptr)
			{
				/* Check if this is a purpose filtering property and assign if so */
				if(name && !strcmp(name, MOSQ_DAP_SP_KEY))
				{
					/* The DAP-SP property is present, so the subscriber HAS declared
					 * an SP - even an empty one. An empty SP is a valid consent
					 * withdrawal (the subscription then matches nothing, see subs.c
					 * purpose_filter_count <= 0), NOT a missing declaration. Mark
					 * has_sp here so the no-SP rejection below does not fire and
					 * disconnect a subscriber on a withdrawn-purpose rotation window. */
					has_sp = true;

					/* Parse all purposes this filter describes. An empty/absent value
					 * parses to zero filters (handled in parse_purpose_filter). */
					uint32_t num_results = 0;
					char** purposes = parse_purpose_filter(value, &num_results);

					for(uint32_t i = 0; i < num_results; i++)
					{
						/* Verify this isn't a dupe */
						bool found_dupe = false;
						for(uint32_t j = 0; j < purpose_filter_count; j++)
						{
							if(strcmp(purpose_filters[j], purposes[i]) == 0)
							{
								found_dupe = true;
								break;
							}
						}

						/* Skip if it is */
						if(found_dupe)
						{
							continue;
						}

						/* Verify we haven't exceeded the maximum */
						if(purpose_filter_count == MOSQ_DAP_MAX_FILTERS_PER_SUB)
						{
							log__printf(NULL, MOSQ_LOG_INFO,
								"Too many purpose filters from %s, disconnecting.",
								context->address);

								/* Free purpose struct and purposes */
								for(uint32_t j = 0; j < num_results; j++)
								{
									mosquitto_FREE(purposes[j]);
								}
								mosquitto_FREE(purposes);
								mosquitto_FREE(name);
								mosquitto_FREE(value);

								mosquitto_property_free_all(&properties);
								return MOSQ_ERR_MALFORMED_PACKET;
						}

						/* Store an independent, correctly-sized copy of the filter. */
						char* filter = purpose_filter_store_dup(purposes[i]);
						if(!filter)
						{
							/* Free purpose struct and purposes */
							for(uint32_t j = 0; j < num_results; j++)
							{
								mosquitto_FREE(purposes[j]);
							}
							mosquitto_FREE(purposes);
							mosquitto_FREE(name);
							mosquitto_FREE(value);

							mosquitto_property_free_all(&properties);
							return MOSQ_ERR_NOMEM;
						}
						purpose_filters[purpose_filter_count] = filter;
						purpose_filter_count++;
						has_sp = true;
					}

					/* Free purpose struct and purposes; the kept ones were copied */
					for(uint32_t i = 0; i < num_results; i++)
					{
						mosquitto_FREE(purposes[i]);
					}
					mosquitto_FREE(purposes);
				}

				mosquitto_FREE(name);
				mosquitto_FREE(value);
				curr_prop_ptr = curr_prop_ptr->next;
			}
		}

		mosquitto_property_free_all(&properties);
		/* Note - User Property not handled */
	}

	while(context->in_packet.pos < context->in_packet.remaining_length){
		memset(&sub, 0, sizeof(sub));
		sub.identifier = subscription_identifier;
		sub.properties = properties;
		if(packet__read_string(&context->in_packet, &sub.topic_filter, &slen)){
			mosquitto_FREE(payload);
			return MOSQ_ERR_MALFORMED_PACKET;
		}

		if(sub.topic_filter){
			if(!slen){
				log__printf(NULL, MOSQ_LOG_INFO,
						"Empty subscription string from %s, disconnecting.",
						context->address);
				mosquitto_FREE(sub.topic_filter);
				mosquitto_FREE(payload);
				return MOSQ_ERR_MALFORMED_PACKET;
			}
			if(mosquitto_sub_topic_check(sub.topic_filter)){
				log__printf(NULL, MOSQ_LOG_INFO,
						"Invalid subscription string from %s, disconnecting.",
						context->address);
				mosquitto_FREE(sub.topic_filter);
				mosquitto_FREE(payload);
				return MOSQ_ERR_MALFORMED_PACKET;
			}

			if(packet__read_byte(&context->in_packet, &sub.options)){
				mosquitto_FREE(sub.topic_filter);
				mosquitto_FREE(payload);
				return MOSQ_ERR_MALFORMED_PACKET;
			}
			if(sub.options & MQTT_SUB_OPT_NO_LOCAL && !strncmp(sub.topic_filter, "$share/", strlen("$share/"))){
				mosquitto_FREE(sub.topic_filter);
				mosquitto_FREE(payload);
				log__printf(NULL, MOSQ_LOG_INFO, "Protocol error from %s: $share subscription with no-local set.", context->id);
				return MOSQ_ERR_PROTOCOL;
			}

			if(context->protocol == mosq_p_mqtt31 || context->protocol == mosq_p_mqtt311){
				qos = sub.options;
				sub.options = 0;
				if(context->is_bridge){
					sub.options |= MQTT_SUB_OPT_RETAIN_AS_PUBLISHED | MQTT_SUB_OPT_NO_LOCAL;
				}
			}else{
				qos = sub.options & 0x03;
				sub.options &= 0xFC;

				if(MQTT_SUB_OPT_GET_NO_LOCAL(sub.options) && !strncmp(sub.topic_filter, "$share/", 7)){
					mosquitto_FREE(sub.topic_filter);
					mosquitto_FREE(payload);
					return MOSQ_ERR_PROTOCOL;
				}
				retain_handling = MQTT_SUB_OPT_GET_RETAIN_HANDLING(sub.options);
				if(retain_handling == 0x30 || (sub.options & 0xC0) != 0){
					mosquitto_FREE(sub.topic_filter);
					mosquitto_FREE(payload);
					return MOSQ_ERR_MALFORMED_PACKET;
				}
			}
			if(qos > 2){
				log__printf(NULL, MOSQ_LOG_INFO,
						"Invalid QoS in subscription command from %s, disconnecting.",
						context->address);
				mosquitto_FREE(sub.topic_filter);
				mosquitto_FREE(payload);
				return MOSQ_ERR_MALFORMED_PACKET;
			}
			if(qos > context->max_qos){
				qos = context->max_qos;
			}
			sub.options |= qos;


			if(context->listener && context->listener->mount_point){
				len = strlen(context->listener->mount_point) + slen + 1;
				sub_mount = mosquitto_malloc(len+1);
				if(!sub_mount){
					mosquitto_FREE(sub.topic_filter);
					mosquitto_FREE(payload);
					return MOSQ_ERR_NOMEM;
				}
				snprintf(sub_mount, len, "%s%s", context->listener->mount_point, sub.topic_filter);
				sub_mount[len] = '\0';

				mosquitto_FREE(sub.topic_filter);
				sub.topic_filter = sub_mount;

			}

			/* MQTT-DAP subscribe-time policy. Gated on the protection framework so
			 * non-DAP deployments and the existing test-suite are unaffected. On
			 * violation the SUBSCRIBE is malformed and the connection is dropped. */

			/* Paper 5.1: keyed topics may only be subscribed by the client they are
				* keyed to. ORS/<sub> is a subscriber's operation-request inbox and
				* ONP/<pub> a publisher's notification inbox. */
			const char *keyed_id = NULL;
			if(!strncmp(sub.topic_filter, MOSQ_DAP_TOPIC_ORS "/", strlen(MOSQ_DAP_TOPIC_ORS) + 1)){
				keyed_id = sub.topic_filter + strlen(MOSQ_DAP_TOPIC_ORS) + 1;
			}else if(!strncmp(sub.topic_filter, MOSQ_DAP_TOPIC_ONP "/", strlen(MOSQ_DAP_TOPIC_ONP) + 1)){
				keyed_id = sub.topic_filter + strlen(MOSQ_DAP_TOPIC_ONP) + 1;
			}
			if(keyed_id && (!context->id || strcmp(keyed_id, context->id) != 0)){
				log__printf(NULL, MOSQ_LOG_INFO,
					"Subscription from %s to keyed topic %s does not match the client id, rejecting.",
					context->id, sub.topic_filter);
				mosquitto_FREE(sub.topic_filter);
				mosquitto_FREE(payload);
				return MOSQ_ERR_MALFORMED_PACKET;
			}

			/* Paper 4.3: every data subscription must declare an SP. Operation-system
				* topics ($OSYS, $MP_REG, the keyed inboxes) are exempt.
				* SP is an MQTT v5 user property, so the requirement applies to v5 only. */
			if(context->protocol == mosq_p_mqtt5 && !has_sp && !dap_is_op_system_topic(sub.topic_filter)){
				log__printf(NULL, MOSQ_LOG_INFO,
					"Subscription from %s to %s lacks a DAP-SP declaration, rejecting.",
					context->id, sub.topic_filter);
				mosquitto_FREE(sub.topic_filter);
				mosquitto_FREE(payload);
				return MOSQ_ERR_MALFORMED_PACKET;
			}

			/* Setup purpose filters */
			if(purpose_filter_count > 0)
			{
				sub.purpose_filter_count = purpose_filter_count;
				sub.purpose_filters = mosquitto_calloc(purpose_filter_count, sizeof(char*));
				if(!sub.purpose_filters){
					mosquitto_FREE(sub.topic_filter);
					mosquitto_FREE(payload);
					return MOSQ_ERR_NOMEM;
				}

				for(size_t i = 0; i < purpose_filter_count; i++)
				{
					/* MQTT-DAP: each subscription leaf must OWN its purpose-filter
					 * strings. The packet-scoped purpose_filters[] are freed once
					 * after the topic-filter loop, so copy (don't borrow) here -
					 * otherwise every topic filter in a multi-topic SUBSCRIBE shares
					 * one allocation and session teardown double-frees it
					 * (subs.c sub__free_purpose_filters). */
					sub.purpose_filters[i] = mosquitto_strdup(purpose_filters[i]);
					if(!sub.purpose_filters[i]){
						for(size_t j = 0; j < i; j++){
							mosquitto_FREE(sub.purpose_filters[j]);
						}
						mosquitto_FREE(sub.purpose_filters);
						mosquitto_FREE(sub.topic_filter);
						mosquitto_FREE(payload);
						for(uint32_t k = 0; k < purpose_filter_count; k++){
							mosquitto_FREE(purpose_filters[k]);
						}
						return MOSQ_ERR_NOMEM;
					}
				}
			}
			else
			{
				sub.purpose_filter_count = 0;
				sub.purpose_filters = NULL;
			}

			allowed = true;
			rc2 = mosquitto_acl_check(context, sub.topic_filter, 0, NULL, qos, false, properties, MOSQ_ACL_SUBSCRIBE);
			switch(rc2){
				case MOSQ_ERR_SUCCESS:
					break;
				case MOSQ_ERR_ACL_DENIED:
					allowed = false;
					if(context->protocol == mosq_p_mqtt5){
						qos = MQTT_RC_NOT_AUTHORIZED;
					}else if(context->protocol == mosq_p_mqtt311){
						qos = 0x80;
					}
					break;
				default:
					mosquitto_FREE(sub.topic_filter);
					mosquitto_FREE(payload);
					return rc2;
			}
			if(qos > 127){
				log__printf(NULL, MOSQ_LOG_DEBUG, "\t%s (denied)", sub.topic_filter);
			}else{
				log__printf(NULL, MOSQ_LOG_DEBUG, "\t%s (QoS %d)", sub.topic_filter, qos);
			}

			if(allowed){
				rc2 = plugin__handle_subscribe(context, &sub);
				if(rc2){
					mosquitto_FREE(sub.topic_filter);
					mosquitto_FREE(payload);
					return rc2;
				}

				rc2 = sub__add(context, &sub);
				if(rc2 > 0){
					mosquitto_FREE(sub.topic_filter);
					mosquitto_FREE(payload);
					return rc2;
				}
				if(context->protocol == mosq_p_mqtt311 || context->protocol == mosq_p_mqtt31){
					if(rc2 == MOSQ_ERR_SUCCESS || rc2 == MOSQ_ERR_SUB_EXISTS){
						if(retain__queue(context, &sub)){
							mosquitto_FREE(sub.topic_filter);
							mosquitto_FREE(payload);
							return rc;
						}
					}
				}else{
					if((retain_handling == MQTT_SUB_OPT_SEND_RETAIN_ALWAYS)
							|| (rc2 == MOSQ_ERR_SUCCESS && retain_handling == MQTT_SUB_OPT_SEND_RETAIN_NEW)){

						if(retain__queue(context, &sub)){
							mosquitto_FREE(sub.topic_filter);
							mosquitto_FREE(payload);
							return rc;
						}
					}
				}
				log__printf(NULL, MOSQ_LOG_SUBSCRIBE, "%s %d %s", context->id, qos, sub.topic_filter);

				plugin_persist__handle_subscription_add(context, &sub);
			}
			mosquitto_FREE(sub.topic_filter);

			tmp_payload = mosquitto_realloc(payload, payloadlen + 1);
			if(tmp_payload){
				payload = tmp_payload;
				payload[payloadlen] = qos;
				payloadlen++;
			}else{
				mosquitto_FREE(payload);

				return MOSQ_ERR_NOMEM;
			}
		}
	}

	/* MQTT-DAP: each subscription leaf above took its own strdup'd copy of the
	 * purpose filters, so free the packet-scoped originals exactly once here. */
	for(uint32_t i = 0; i < purpose_filter_count; i++){
		mosquitto_FREE(purpose_filters[i]);
	}

	if(context->protocol != mosq_p_mqtt31){
		if(payloadlen == 0){
			/* No subscriptions specified, protocol error. */
			return MOSQ_ERR_MALFORMED_PACKET;
		}
	}
	if(send__suback(context, mid, payloadlen, payload)){
		rc = 1;
	}
	mosquitto_FREE(payload);

#ifdef WITH_PERSISTENCE
	db.persistence_changes++;
#endif

	if(context->out_packet == NULL){
		rc = db__message_write_queued_out(context);
		if(rc){
			return rc;
		}
		rc = db__message_write_inflight_out_latest(context);
		if(rc){
			return rc;
		}
	}

	return rc;
}
