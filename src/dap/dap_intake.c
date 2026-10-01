#include "config.h"

#include <string.h>
#include <time.h>

#include "mosquitto_broker_internal.h"
#include "mosquitto/mqtt_protocol.h"
#include "packet_mosq.h"
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


/* True when the PUBLISH in packet carries data rather than a state change. A topic
 * set only by alias, or one that cannot be read, is handled as read. */
static bool dap_intake__is_data(const struct mosquitto__packet_in *packet)
{
	static const char *state_topics[] = {MOSQ_DAP_MP_REG_TOPIC, MOSQ_DAP_TOPIC_OSYS};
	uint16_t len;

	if(packet->remaining_length < 2) return false;
	len = (uint16_t)((packet->payload[0] << 8) | packet->payload[1]);
	if(len == 0 || (uint32_t)len + 2 > packet->remaining_length) return false;
	for(size_t i = 0; i < sizeof(state_topics)/sizeof(state_topics[0]); i++){
		if(strlen(state_topics[i]) == len && !memcmp(&packet->payload[2], state_topics[i], len)){
			return false;
		}
	}
	return true;
}


static void dap_intake__free(struct dap_intake_packet *p)
{
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

	dap_receipt__stamp(&receipt);
	if(!intake_deferring || context->state != mosq_cs_active || !dap_intake__is_data(&context->in_packet)){
		return handle__publish(context, &receipt);
	}

	p = mosquitto_calloc(1, sizeof(*p));
	if(!p){
		return handle__publish(context, &receipt);
	}
	p->context = context;
	p->receipt = receipt;
	p->command = context->in_packet.command;
	p->remaining_count = context->in_packet.remaining_count;
	p->remaining_length = context->in_packet.remaining_length;
	p->payload = context->in_packet.payload;
	context->in_packet.payload = NULL;
	DL_APPEND(intake_head, p);
	return MOSQ_ERR_SUCCESS;
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
			dap_intake__purge(context);
			do_disconnect(context, rc);
		}
	}
}


void dap_intake__flush(struct mosquitto *context)
{
	struct dap_intake_packet *p;

	if(context->state != mosq_cs_active){
		dap_intake__purge(context);
		return;
	}
	/* Search from the head each time: handling one may flush or purge others. */
	for(;;){
		for(p = intake_head; p && p->context != context; p = p->next){
		}
		if(!p) return;
		DL_DELETE(intake_head, p);
		int rc = dap_intake__handle(p);
		dap_intake__free(p);
		if(rc){
			dap_intake__purge(context);
			return;
		}
	}
}


void dap_intake__cleanup(void)
{
	struct dap_intake_packet *p, *tmp;

	DL_FOREACH_SAFE(intake_head, p, tmp){
		DL_DELETE(intake_head, p);
		dap_intake__free(p);
	}
	intake_deferring = false;
}
