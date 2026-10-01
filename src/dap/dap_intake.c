#include "config.h"

#include <string.h>
#include <time.h>

#include "mosquitto_broker_internal.h"
#include "mosquitto/mqtt_protocol.h"
#include "packet_mosq.h"
#include "property_mosq.h"
#include "utlist.h"
#include "dap/dap_intake.h"

/* A PUBLISH set aside until the end of the pass that read it. */
struct dap_intake_packet {
	struct mosquitto *context;
	struct dap_receipt receipt;
	uint8_t *payload;
	uint32_t remaining_length;
	uint8_t command;
	int8_t remaining_count;
	struct dap_intake_packet *prev;
	struct dap_intake_packet *next;
};

static struct dap_intake_packet *intake_head = NULL;
static bool intake_deferring = false;


void dap_receipt__stamp(struct dap_receipt *receipt)
{
	struct timespec ts_wall, ts_mono;

	receipt->time = time(NULL);
	clock_gettime(CLOCK_REALTIME, &ts_wall);
	clock_gettime(CLOCK_MONOTONIC, &ts_mono);
	receipt->ns_wall = (uint64_t)ts_wall.tv_sec * 1000000000ULL + (uint64_t)ts_wall.tv_nsec;
	receipt->ns_mono = (uint64_t)ts_mono.tv_sec * 1000000000ULL + (uint64_t)ts_mono.tv_nsec;
	/* Paper 5.2(i): a total order over receipts, kept monotone if the clock steps back. */
	receipt->order = receipt->ns_wall > db.dap_last_order ? receipt->ns_wall : db.dap_last_order + 1;
	db.dap_last_order = receipt->order;
}


/* The length of the PUBLISH's topic, or -1 if it cannot be read. */
static int dap_intake__topic_len(const struct mosquitto__packet_in *packet)
{
	uint16_t len;

	if(packet->remaining_length < 2) return -1;
	len = (uint16_t)((packet->payload[0] << 8) | packet->payload[1]);
	if((uint32_t)len + 2 > packet->remaining_length) return -1;
	return len;
}


/* True when the PUBLISH carries data rather than a state change. A topic given only
 * by alias counts as data, so it keeps its place behind the PUBLISH that set it. */
static bool dap_intake__is_data(const struct mosquitto__packet_in *packet)
{
	static const char *state_topics[] = {MOSQ_DAP_MP_REG_TOPIC, MOSQ_DAP_TOPIC_OSYS};
	int len = dap_intake__topic_len(packet);

	if(len < 0) return false;
	for(size_t i = 0; i < sizeof(state_topics)/sizeof(state_topics[0]); i++){
		if((int)strlen(state_topics[i]) == len && !memcmp(&packet->payload[2], state_topics[i], (size_t)len)){
			return false;
		}
	}
	return true;
}


/* True when a state-change PUBLISH may go ahead of its client's own set-aside data:
 * it is QoS 0, so no acknowledgement is reordered, and it sets no topic alias. */
static bool dap_intake__may_overtake(struct mosquitto *context)
{
	struct mosquitto__packet_in packet = context->in_packet;
	mosquitto_property *properties = NULL;
	uint16_t alias;
	bool has_alias;
	int len = dap_intake__topic_len(&packet);

	if(len < 0 || (packet.command & 0x06) != 0) return false;
	if(context->protocol != mosq_p_mqtt5) return true;
	packet.pos = (uint32_t)len + 2;
	if(property__read_all(CMD_PUBLISH, &packet, &properties)) return false;
	has_alias = mosquitto_property_read_int16(properties, MQTT_PROP_TOPIC_ALIAS, &alias, false) != NULL;
	mosquitto_property_free_all(&properties);
	return !has_alias;
}


static void dap_intake__free(struct dap_intake_packet *p)
{
	p->context->dap_set_aside--;
	mosquitto_FREE(p->payload);
	mosquitto_FREE(p);
}


/* Handle a set-aside PUBLISH, swapping it into the context's in_packet around the
 * call so that a packet still being read is kept. */
static int dap_intake__handle(struct dap_intake_packet *p)
{
	struct mosquitto *context = p->context;
	struct mosquitto__packet_in reading = context->in_packet;
	int rc;

	context->in_packet.command = p->command;
	context->in_packet.remaining_count = p->remaining_count;
	context->in_packet.remaining_length = p->remaining_length;
	context->in_packet.payload = p->payload;
	context->in_packet.pos = 0;
	context->in_packet.to_process = 0;
	p->payload = NULL;

	rc = handle__publish(context, &p->receipt);

	packet__cleanup(&context->in_packet);
	context->in_packet = reading;
	return rc;
}


/* Drop a client's set-aside PUBLISHes. */
static void dap_intake__purge(struct mosquitto *context)
{
	struct dap_intake_packet *p, *tmp;

	if(context->dap_set_aside == 0) return;
	DL_FOREACH_SAFE(intake_head, p, tmp){
		if(p->context == context){
			DL_DELETE(intake_head, p);
			dap_intake__free(p);
		}
	}
}


int dap_intake__publish(struct mosquitto *context)
{
	struct dap_receipt receipt;
	struct dap_intake_packet *p;
	int rc;

	dap_receipt__stamp(&receipt);
	if(intake_deferring && context->state == mosq_cs_active){
		if(dap_intake__is_data(&context->in_packet)){
			p = mosquitto_calloc(1, sizeof(*p));
			if(p){
				p->context = context;
				p->receipt = receipt;
				p->command = context->in_packet.command;
				p->remaining_count = context->in_packet.remaining_count;
				p->remaining_length = context->in_packet.remaining_length;
				p->payload = context->in_packet.payload;
				context->in_packet.payload = NULL;
				context->dap_set_aside++;
				DL_APPEND(intake_head, p);
				return MOSQ_ERR_SUCCESS;
			}
		}else if(context->dap_set_aside > 0 && !dap_intake__may_overtake(context)){
			rc = dap_intake__flush(context);
			if(rc) return rc;
		}
	}
	return handle__publish(context, &receipt);
}


void dap_intake__begin(void)
{
	intake_deferring = true;
}


void dap_intake__end(void)
{
	struct dap_intake_packet *p;

	intake_deferring = false;
	/* Take from the head each time: handling one may flush or purge others. */
	while((p = intake_head) != NULL){
		struct mosquitto *context = p->context;
		int rc;

		DL_DELETE(intake_head, p);
		rc = dap_intake__handle(p);
		dap_intake__free(p);
		if(rc){
			/* The client's later data is not handled, as if it had not been read. */
			dap_intake__purge(context);
			handle__packet_error(context, rc);
			do_disconnect(context, rc);
		}
	}
}


int dap_intake__flush(struct mosquitto *context)
{
	struct dap_intake_packet *p;

	if(context->dap_set_aside == 0) return MOSQ_ERR_SUCCESS;
	if(context->state != mosq_cs_active){
		dap_intake__purge(context);
		return MOSQ_ERR_SUCCESS;
	}
	/* Search from the head each time: handling one may flush or purge others. */
	while(context->dap_set_aside > 0){
		for(p = intake_head; p && p->context != context; p = p->next){
		}
		if(!p) break;
		DL_DELETE(intake_head, p);
		int rc = dap_intake__handle(p);
		dap_intake__free(p);
		if(rc){
			dap_intake__purge(context);
			return rc;
		}
	}
	return MOSQ_ERR_SUCCESS;
}


void dap_intake__cleanup(void)
{
	struct dap_intake_packet *p, *tmp;

	DL_FOREACH_SAFE(intake_head, p, tmp){
		DL_DELETE(intake_head, p);
		mosquitto_FREE(p->payload);
		mosquitto_FREE(p);
	}
	intake_deferring = false;
}
