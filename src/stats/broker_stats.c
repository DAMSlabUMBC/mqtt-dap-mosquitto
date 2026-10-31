/* Per-client stats for the MQTT-DAP baseline, same file as the DAP broker. */

#include "config.h"

#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "mosquitto_broker_internal.h"
#include "stats/broker_stats.h"

static FILE *stats_file = NULL;
static struct broker_stats_entry *client_stats = NULL;

/* Monotonic time (ms) at which the next row is due. */
static uint64_t next_write_ms = 0;

static uint64_t broker_stats__now_ms(clockid_t clock)
{
	struct timespec ts;
	clock_gettime(clock, &ts);
	return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

static struct broker_stats_entry *broker_stats__find_or_make_entry(const char *client_id)
{
	struct broker_stats_entry *e = NULL;

	HASH_FIND_STR(client_stats, client_id, e);
	if(e) return e;

	e = mosquitto_calloc(1, sizeof(struct broker_stats_entry));
	if(!e) return NULL;
	e->client_id = mosquitto_strdup(client_id);
	if(!e->client_id){
		mosquitto_FREE(e);
		return NULL;
	}
	HASH_ADD_KEYPTR(hh, client_stats, e->client_id, strlen(e->client_id), e);
	return e;
}

void broker_stats__init(void)
{
	if(db.config->stats_interval_ms == 0 || stats_file) return;

	stats_file = fopen("/tmp/dap_broker_stats.csv", "w");
	if(!stats_file) return;
	fprintf(stats_file,
			"timestamp_ms,client_id,queue_size,inflight_count,"
			"msgs_delivered_interval,time_through_broker_min_ns,time_through_broker_max_ns,time_through_broker_mean_ns\n");
	fflush(stats_file);

	next_write_ms = 0;
}

void broker_stats__int_set(const char *client_id, enum broker_stat_type s, int64_t value)
{
	if(!stats_file || !client_id || (s < 0) || (s >= broker_stat_max)) return;

	struct broker_stats_entry *e = broker_stats__find_or_make_entry(client_id);
	if(e){
		e->stats[s] = value;
	}
}

/* Time a PUBLISH's first write to a client. Restored messages are skipped. */
void broker_stats__record_delivery(const char *client_id, const struct mosquitto__base_msg *base_msg)
{
	if(!stats_file || !client_id || !base_msg || base_msg->stats_recv_time_ns_mono == 0) return;

	struct broker_stats_entry *e = broker_stats__find_or_make_entry(client_id);
	if(!e) return;

	struct timespec now_mono;
	clock_gettime(CLOCK_MONOTONIC, &now_mono);
	uint64_t now_ns = (uint64_t)now_mono.tv_sec * 1000000000ULL + (uint64_t)now_mono.tv_nsec;
	uint64_t latency_ns = now_ns >= base_msg->stats_recv_time_ns_mono ? now_ns - base_msg->stats_recv_time_ns_mono : 0;

	if(e->latency_count == 0 || latency_ns < e->latency_min_ns) e->latency_min_ns = latency_ns;
	if(latency_ns > e->latency_max_ns) e->latency_max_ns = latency_ns;
	e->latency_sum_ns += latency_ns;
	e->latency_count++;
}

/* Write one row per client every stats_interval_ms. */
void broker_stats__write(bool force)
{
	if(!stats_file || db.config->stats_interval_ms == 0) return;

	uint64_t now_ms = broker_stats__now_ms(CLOCK_MONOTONIC);
	if(!force && next_write_ms != 0 && now_ms < next_write_ms){
		loop__update_next_event((time_t)(next_write_ms - now_ms));
		return;
	}

	uint64_t wall_ms = broker_stats__now_ms(CLOCK_REALTIME);
	struct broker_stats_entry *e, *tmp;
	HASH_ITER(hh, client_stats, e, tmp){
		fprintf(stats_file, "%" PRIu64 ",%s,%" PRId64 ",%" PRId64 ",%" PRIu64,
			wall_ms,
			e->client_id,
			e->stats[broker_stat_current_queue_size],
			e->stats[broker_stat_current_inflight_count],
			e->latency_count);
		/* Empty latency columns when nothing was delivered. */
		if(e->latency_count){
			fprintf(stats_file, ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n",
				e->latency_min_ns, e->latency_max_ns, e->latency_sum_ns / e->latency_count);
		}else{
			fprintf(stats_file, ",,,\n");
		}
		e->latency_count = 0;
		e->latency_sum_ns = 0;
		e->latency_min_ns = 0;
		e->latency_max_ns = 0;
	}
	fflush(stats_file);

	/* Stay on the interval grid; if the loop fell behind, restart from now. */
	uint64_t interval = (uint64_t)db.config->stats_interval_ms;
	next_write_ms = (next_write_ms != 0 && next_write_ms + interval > now_ms) ? next_write_ms + interval : now_ms + interval;
	loop__update_next_event((time_t)(next_write_ms - now_ms));
}

void broker_stats__close(void)
{
	if(stats_file){
		broker_stats__write(true);
		fclose(stats_file);
		stats_file = NULL;
	}

	struct broker_stats_entry *e, *tmp;
	HASH_ITER(hh, client_stats, e, tmp){
		HASH_DEL(client_stats, e);
		mosquitto_FREE(e->client_id);
		mosquitto_FREE(e);
	}
}
