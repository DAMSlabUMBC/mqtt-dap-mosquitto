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

/* A DAP-SP declaration: its purposes, bound to one topic filter by the paper's
 * <SP>:<topic_filter> form, or to every subscription in the packet when bare. */
struct sp_decl {
	char *topic_filter;
	char **purposes;
	uint32_t count;
};

static void free_sp_decls(struct sp_decl *decls, uint32_t count)
{
	for(uint32_t i = 0; i < count; i++){
		mosquitto_FREE(decls[i].topic_filter);
		purpose_set_free(decls[i].purposes, decls[i].count);
	}
	mosquitto_FREE(decls);
}

/* Parse one DAP-SP value onto decls. */
static int add_sp_decl(struct sp_decl **decls, uint32_t *count, const char *value)
{
	const char *sep = value ? strchr(value, ':') : NULL;
	struct sp_decl decl = {NULL, NULL, 0};
	struct sp_decl *grown;
	char *sp = NULL;
	int rc;

	if(sep){
		sp = mosquitto_strndup(value, (size_t)(sep - value));
		decl.topic_filter = mosquitto_strdup(sep + 1);
		if(!sp || !decl.topic_filter){
			mosquitto_FREE(sp);
			mosquitto_FREE(decl.topic_filter);
			return MOSQ_ERR_NOMEM;
		}
	}
	rc = purpose_set_expand(sep ? sp : value, &decl.purposes, &decl.count);
	mosquitto_FREE(sp);
	if(rc == MOSQ_ERR_SUCCESS){
		grown = mosquitto_realloc(*decls, (*count + 1) * sizeof(struct sp_decl));
		if(grown){
			*decls = grown;
			(*decls)[(*count)++] = decl;
			return MOSQ_ERR_SUCCESS;
		}
		rc = MOSQ_ERR_NOMEM;
	}
	mosquitto_FREE(decl.topic_filter);
	purpose_set_free(decl.purposes, decl.count);
	return rc;
}

/* The SP the packet declares for topic_filter: the union of its bare declarations
 * and those bound to topic_filter, sorted. *declared is false when none apply. */
static int sp_for_topic(const struct sp_decl *decls, uint32_t decl_count, const char *topic_filter,
		char ***purposes, uint32_t *count, bool *declared)
{
	*purposes = NULL;
	*count = 0;
	*declared = false;
	for(uint32_t i = 0; i < decl_count; i++){
		char **grown;

		if(decls[i].topic_filter && strcmp(decls[i].topic_filter, topic_filter)){
			continue;
		}
		*declared = true;
		if(decls[i].count == 0){
			continue;
		}
		grown = mosquitto_realloc(*purposes, (*count + decls[i].count) * sizeof(char *));
		if(!grown){
			purpose_set_free(*purposes, *count);
			*purposes = NULL;
			*count = 0;
			return MOSQ_ERR_NOMEM;
		}
		*purposes = grown;
		for(uint32_t j = 0; j < decls[i].count; j++){
			(*purposes)[*count] = mosquitto_strdup(decls[i].purposes[j]);
			if(!(*purposes)[*count]){
				purpose_set_free(*purposes, *count);
				*purposes = NULL;
				*count = 0;
				return MOSQ_ERR_NOMEM;
			}
			(*count)++;
		}
		purpose_set_normalize(*purposes, count);
		if(*count > MOSQ_DAP_MAX_FILTERS_PER_SUB){
			purpose_set_free(*purposes, *count);
			*purposes = NULL;
			*count = 0;
			return MOSQ_ERR_MALFORMED_PACKET;
		}
	}
	return MOSQ_ERR_SUCCESS;
}


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
	struct sp_decl *sp_decls = NULL;
	uint32_t sp_decl_count = 0;

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

		/* Collect the DAP-SP declarations. User properties are packet-scoped, so a
		 * subscription takes the bare ones and those bound to its topic filter. An
		 * empty SP is a valid consent withdrawal: the subscription then matches nothing. */
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
				if(name && !strcmp(name, MOSQ_DAP_SP_KEY))
				{
					rc = add_sp_decl(&sp_decls, &sp_decl_count, value);
					if(rc){
						if(rc != MOSQ_ERR_NOMEM){
							log__printf(NULL, MOSQ_LOG_INFO,
								"Invalid DAP-SP from %s, disconnecting.",
								context->address);
							rc = MOSQ_ERR_MALFORMED_PACKET;
						}
						mosquitto_FREE(name);
						mosquitto_FREE(value);
						free_sp_decls(sp_decls, sp_decl_count);
						mosquitto_property_free_all(&properties);
						return rc;
					}
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
			free_sp_decls(sp_decls, sp_decl_count);
			return MOSQ_ERR_MALFORMED_PACKET;
		}

		if(sub.topic_filter){
			if(!slen){
				log__printf(NULL, MOSQ_LOG_INFO,
						"Empty subscription string from %s, disconnecting.",
						context->address);
				mosquitto_FREE(sub.topic_filter);
				mosquitto_FREE(payload);
				free_sp_decls(sp_decls, sp_decl_count);
				return MOSQ_ERR_MALFORMED_PACKET;
			}
			if(mosquitto_sub_topic_check(sub.topic_filter)){
				log__printf(NULL, MOSQ_LOG_INFO,
						"Invalid subscription string from %s, disconnecting.",
						context->address);
				mosquitto_FREE(sub.topic_filter);
				mosquitto_FREE(payload);
				free_sp_decls(sp_decls, sp_decl_count);
				return MOSQ_ERR_MALFORMED_PACKET;
			}

			if(packet__read_byte(&context->in_packet, &sub.options)){
				mosquitto_FREE(sub.topic_filter);
				mosquitto_FREE(payload);
				free_sp_decls(sp_decls, sp_decl_count);
				return MOSQ_ERR_MALFORMED_PACKET;
			}
			if(sub.options & MQTT_SUB_OPT_NO_LOCAL && !strncmp(sub.topic_filter, "$share/", strlen("$share/"))){
				mosquitto_FREE(sub.topic_filter);
				mosquitto_FREE(payload);
				log__printf(NULL, MOSQ_LOG_INFO, "Protocol error from %s: $share subscription with no-local set.", context->id);
				free_sp_decls(sp_decls, sp_decl_count);
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
					free_sp_decls(sp_decls, sp_decl_count);
					return MOSQ_ERR_PROTOCOL;
				}
				retain_handling = MQTT_SUB_OPT_GET_RETAIN_HANDLING(sub.options);
				if(retain_handling == 0x30 || (sub.options & 0xC0) != 0){
					mosquitto_FREE(sub.topic_filter);
					mosquitto_FREE(payload);
					free_sp_decls(sp_decls, sp_decl_count);
					return MOSQ_ERR_MALFORMED_PACKET;
				}
			}
			if(qos > 2){
				log__printf(NULL, MOSQ_LOG_INFO,
						"Invalid QoS in subscription command from %s, disconnecting.",
						context->address);
				mosquitto_FREE(sub.topic_filter);
				mosquitto_FREE(payload);
				free_sp_decls(sp_decls, sp_decl_count);
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
					free_sp_decls(sp_decls, sp_decl_count);
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
				free_sp_decls(sp_decls, sp_decl_count);
				return MOSQ_ERR_MALFORMED_PACKET;
			}

			/* Paper 4.3: every data subscription must declare an SP. Operation-system
			 * topics ($OP_SYS, $MP_REG, the keyed inboxes) are exempt. SP is an MQTT v5
			 * user property, so the requirement applies to v5 only. A bound SP names the
			 * topic filter as the client sent it, before any mount point. */
			bool has_sp;
			const char *client_filter = sub.topic_filter;
			if(context->listener && context->listener->mount_point){
				client_filter += strlen(context->listener->mount_point);
			}
			rc2 = sp_for_topic(sp_decls, sp_decl_count, client_filter,
					&sub.purpose_filters, &sub.purpose_filter_count, &has_sp);
			if(rc2){
				if(rc2 == MOSQ_ERR_MALFORMED_PACKET){
					log__printf(NULL, MOSQ_LOG_INFO,
						"Too many purpose filters from %s, disconnecting.",
						context->address);
				}
				mosquitto_FREE(sub.topic_filter);
				mosquitto_FREE(payload);
				free_sp_decls(sp_decls, sp_decl_count);
				return rc2;
			}
			if(context->protocol == mosq_p_mqtt5 && !has_sp && !dap_is_op_system_topic(sub.topic_filter)){
				log__printf(NULL, MOSQ_LOG_INFO,
					"Subscription from %s to %s lacks a DAP-SP declaration, rejecting.",
					context->id, sub.topic_filter);
				purpose_set_free(sub.purpose_filters, sub.purpose_filter_count);
				mosquitto_FREE(sub.topic_filter);
				mosquitto_FREE(payload);
				free_sp_decls(sp_decls, sp_decl_count);
				return MOSQ_ERR_MALFORMED_PACKET;
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
					purpose_set_free(sub.purpose_filters, sub.purpose_filter_count);
					mosquitto_FREE(sub.topic_filter);
					mosquitto_FREE(payload);
					free_sp_decls(sp_decls, sp_decl_count);
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
					purpose_set_free(sub.purpose_filters, sub.purpose_filter_count);
					mosquitto_FREE(sub.topic_filter);
					mosquitto_FREE(payload);
					free_sp_decls(sp_decls, sp_decl_count);
					return rc2;
				}

				/* sub__add takes sub.purpose_filters unless it fails. */
				rc2 = sub__add(context, &sub);
				if(rc2 > 0){
					purpose_set_free(sub.purpose_filters, sub.purpose_filter_count);
					mosquitto_FREE(sub.topic_filter);
					mosquitto_FREE(payload);
					free_sp_decls(sp_decls, sp_decl_count);
					return rc2;
				}
				if(context->protocol == mosq_p_mqtt311 || context->protocol == mosq_p_mqtt31){
					if(rc2 == MOSQ_ERR_SUCCESS || rc2 == MOSQ_ERR_SUB_EXISTS){
						if(retain__queue(context, &sub)){
							mosquitto_FREE(sub.topic_filter);
							mosquitto_FREE(payload);
							free_sp_decls(sp_decls, sp_decl_count);
							return rc;
						}
					}
				}else{
					if((retain_handling == MQTT_SUB_OPT_SEND_RETAIN_ALWAYS)
							|| (rc2 == MOSQ_ERR_SUCCESS && retain_handling == MQTT_SUB_OPT_SEND_RETAIN_NEW)){

						if(retain__queue(context, &sub)){
							mosquitto_FREE(sub.topic_filter);
							mosquitto_FREE(payload);
							free_sp_decls(sp_decls, sp_decl_count);
							return rc;
						}
					}
				}
				log__printf(NULL, MOSQ_LOG_SUBSCRIBE, "%s %d %s", context->id, qos, sub.topic_filter);

				plugin_persist__handle_subscription_add(context, &sub);
			}else{
				purpose_set_free(sub.purpose_filters, sub.purpose_filter_count);
			}
			mosquitto_FREE(sub.topic_filter);

			tmp_payload = mosquitto_realloc(payload, payloadlen + 1);
			if(tmp_payload){
				payload = tmp_payload;
				payload[payloadlen] = qos;
				payloadlen++;
			}else{
				mosquitto_FREE(payload);

				free_sp_decls(sp_decls, sp_decl_count);
				return MOSQ_ERR_NOMEM;
			}
		}
	}

	free_sp_decls(sp_decls, sp_decl_count);

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

	/* Paper 6.3: requests held while the subscriber could not receive them. */
	broker_deliver_held_requests(context);

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
