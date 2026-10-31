/* Timing profile shared by the DAP and baseline brokers (temporary, for comparison runs). */
#include <stdio.h>
#include <inttypes.h>

#include "prof.h"

static const char *section_names[PROF_MAX] = {
	"fanout", "write_single", "send_publish", "dap_purpose", "dap_register_info",
	"dap_stamp", "dap_check_send", "dap_loop", "ack_rtt",
};
static uint64_t section_ns[PROF_MAX];
static uint64_t section_count[PROF_MAX];

int prof_in_fanout = 0;
int prof_fanout_subs = 0;

static FILE *msgs_file = NULL;
static FILE *deliveries_file = NULL;
static FILE *acks_file = NULL;
static uint64_t last_tick = 0;

static void prof_open(void)
{
	if(msgs_file) return;
	msgs_file = fopen("/tmp/prof_msgs.csv", "w");
	deliveries_file = fopen("/tmp/prof_deliveries.csv", "w");
	if(msgs_file){
		setvbuf(msgs_file, NULL, _IOFBF, 1 << 20);
		fprintf(msgs_file, "id,recv_to_handle_ns,handle_to_fanout_ns,fanout_ns,recv_to_fanout_end_ns,subs\n");
	}
	acks_file = fopen("/tmp/prof_acks.csv", "w");
	if(acks_file){
		setvbuf(acks_file, NULL, _IOFBF, 1 << 20);
		fprintf(acks_file, "client_id,ack_rtt_ns\n");
	}
	if(deliveries_file){
		setvbuf(deliveries_file, NULL, _IOFBF, 1 << 20);
		fprintf(deliveries_file, "id,recv_to_write_ns,enqueue_to_write_ns,during_fanout\n");
	}
}

void prof_add(enum prof_section s, uint64_t ns)
{
	section_ns[s] += ns;
	section_count[s]++;
}

void prof_msg(uint64_t id, uint64_t recv, uint64_t handle, uint64_t fan_start, uint64_t fan_end, int subs)
{
	prof_open();
	if(!msgs_file || recv == 0) return;
	fprintf(msgs_file, "%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%d\n",
		id, handle - recv, fan_start - handle, fan_end - fan_start, fan_end - recv, subs);
}

void prof_delivery(uint64_t id, uint64_t recv, uint64_t enq, int during_fanout, uint64_t now)
{
	prof_open();
	if(!deliveries_file || recv == 0) return;
	fprintf(deliveries_file, "%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%d\n",
		id, now - recv, enq ? now - enq : 0, during_fanout);
}

void prof_ack(const char *client_id, uint64_t rtt_ns)
{
	prof_open();
	prof_add(PROF_ACK_RTT, rtt_ns);
	if(acks_file) fprintf(acks_file, "%s,%" PRIu64 "\n", client_id ? client_id : "", rtt_ns);
}

static void prof_write_sections(void)
{
	FILE *f = fopen("/tmp/prof_sections.csv", "w");
	if(!f) return;
	fprintf(f, "section,total_ns,count,mean_ns\n");
	for(int i = 0; i < PROF_MAX; i++){
		fprintf(f, "%s,%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n", section_names[i], section_ns[i], section_count[i],
			section_count[i] ? section_ns[i] / section_count[i] : 0);
	}
	fclose(f);
}

/* Flush about once a second so the runner's docker cp sees current data. */
void prof_tick(void)
{
	uint64_t now = prof_now();
	if(now - last_tick < 1000000000ULL) return;
	last_tick = now;
	if(msgs_file) fflush(msgs_file);
	if(deliveries_file) fflush(deliveries_file);
	if(acks_file) fflush(acks_file);
	prof_write_sections();
}

void prof_close(void)
{
	prof_write_sections();
	if(msgs_file) fclose(msgs_file);
	if(deliveries_file) fclose(deliveries_file);
	if(acks_file) fclose(acks_file);
	msgs_file = deliveries_file = acks_file = NULL;
}
