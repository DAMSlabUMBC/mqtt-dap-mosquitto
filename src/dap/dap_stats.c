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

#include <math.h>
#include <stdio.h>
#include <inttypes.h>
#include <limits.h>
#include <time.h>

#include "mosquitto_broker_internal.h"
#include "dap_stats.h"

static FILE *stats_file = NULL;

/* Monotonic time (ms) at which the next row is due. */
static uint64_t next_write_ms = 0;

static uint64_t dap_stats__now_ms(clockid_t clock)
{
	struct timespec ts;
	clock_gettime(clock, &ts);
	return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

static struct dap_stats_entry *dap_stats__find_entry(struct dap_stats_list *l, const char *client_id)
{
    struct dap_stats_entry *e = NULL;
    HASH_FIND_STR(l->client_stats, client_id, e);
    return e;
}

static struct dap_stats_entry *dap_stats__find_or_make_entry(struct dap_stats_list *l, const char *client_id)
{
	if(!client_id || !l) return NULL;

	// Check for existing entry
	struct dap_stats_entry *e = dap_stats__find_entry(l, client_id);
	if(e) return e;

	// Entry doesn't exist, create
	e = mosquitto_calloc(1, sizeof(struct dap_stats_entry));
    if(!e) return NULL;
    e->sub_id = mosquitto_strdup(client_id);
    if(!e->sub_id){
        mosquitto_FREE(e);
        return NULL;
    }
    HASH_ADD_KEYPTR(hh, l->client_stats, e->sub_id, strlen(e->sub_id), e);
    return e;
}

void dap_stats__init(void)
{
	db.dap_stats_list->client_stats = NULL;

	if(db.config->stats_interval_ms == 0){
		return;
	}

	if(stats_file) return;
    stats_file = fopen("/tmp/dap_broker_stats.csv", "w");
    if(!stats_file) return;
    fprintf(stats_file,
            "timestamp_ms,client_id,queue_size,inflight_count,initially_dropped_by_policy,dropped_on_reeval,out_of_order,op_msgs_from_broker,total_bump_count,"
            "msgs_delivered_interval,time_through_broker_min_ns,time_through_broker_max_ns,time_through_broker_mean_ns\n");
    fflush(stats_file);

	next_write_ms = 0;
}

void dap_stats__int_inc(const char* client_id, enum dap_stat_type s, int64_t value)
{
	if(!stats_file || !client_id || (s < 0) || (s >= dap_stat_max)) return;

    struct dap_stats_entry *e = dap_stats__find_or_make_entry(db.dap_stats_list, client_id);
    if(e)
	{
		e->stats[s] += value;
	}
}


void dap_stats__int_dec(const char* client_id, enum dap_stat_type s, int64_t value)
{
	if(!stats_file || !client_id || (s < 0) || (s >= dap_stat_max)) return;

    struct dap_stats_entry *e = dap_stats__find_or_make_entry(db.dap_stats_list, client_id);
    if(e)
	{
		e->stats[s] -= value;
	}
}

void dap_stats__int_set(const char* client_id, enum dap_stat_type s, int64_t value)
{
	if(!stats_file || !client_id || (s < 0) || (s >= dap_stat_max)) return;

    struct dap_stats_entry *e = dap_stats__find_or_make_entry(db.dap_stats_list, client_id);
    if(e)
	{
		e->stats[s] = value;
	}
}

void dap_stats__pub_sent(const char* client_id, uint32_t subscription_id, uint64_t msg_recv_time_ns_mono)
{
	if(!stats_file || !client_id) return;

    struct dap_stats_entry *e = dap_stats__find_or_make_entry(db.dap_stats_list, client_id);
    if(e)
	{
		// Now find if sub id maps
		struct dap_stats_out_of_order_entry *e2 = NULL;
    	HASH_FIND_INT(e->sub_out_of_order_stats, &subscription_id, e2);

		if(!e2) {
			e2 = mosquitto_calloc(1, sizeof(struct dap_stats_out_of_order_entry));
			if(!e2) return;
			e2->subscription_id = subscription_id;
			e2->out_of_order_count = 0;
			e2->last_pub_sent_recv_ns_mono = msg_recv_time_ns_mono;
			HASH_ADD_INT(e->sub_out_of_order_stats, subscription_id, e2);
		}

		// Entry exists, check timing
		else
		{
			if(msg_recv_time_ns_mono >= e2->last_pub_sent_recv_ns_mono)
			{
				e2->last_pub_sent_recv_ns_mono = msg_recv_time_ns_mono;
			}
			else
			{
				e2->out_of_order_count++;
			}
		}
	}
}

/* Time a data PUBLISH's first write to a client. Ops and restored messages are skipped. */
void dap_stats__record_delivery(const char *client_id, const struct mosquitto__base_msg *base_msg)
{
	if(!stats_file || !client_id || !base_msg) return;
	if(base_msg->is_op || base_msg->dap_recv_time_ns_mono == 0) return;

	struct dap_stats_entry *e = dap_stats__find_or_make_entry(db.dap_stats_list, client_id);
	if(!e) return;

	struct timespec now_mono;
	clock_gettime(CLOCK_MONOTONIC, &now_mono);
	uint64_t now_ns = (uint64_t)now_mono.tv_sec * 1000000000ULL + (uint64_t)now_mono.tv_nsec;
	uint64_t latency_ns = now_ns >= base_msg->dap_recv_time_ns_mono ? now_ns - base_msg->dap_recv_time_ns_mono : 0;

	if(e->latency_count == 0 || latency_ns < e->latency_min_ns) e->latency_min_ns = latency_ns;
	if(latency_ns > e->latency_max_ns) e->latency_max_ns = latency_ns;
	e->latency_sum_ns += latency_ns;
	e->latency_count++;
}

/* Write one row per client every stats_interval_ms. */
void dap_stats__write(bool force)
{
	if(!stats_file || db.config->stats_interval_ms == 0) return;

	uint64_t now_ms = dap_stats__now_ms(CLOCK_MONOTONIC);
	if(!force && next_write_ms != 0 && now_ms < next_write_ms){
		loop__update_next_event((time_t)(next_write_ms - now_ms));
		return;
	}

	uint64_t wall_ms = dap_stats__now_ms(CLOCK_REALTIME);
	struct dap_stats_entry *e, *tmp;
	HASH_ITER(hh, db.dap_stats_list->client_stats, e, tmp){
		uint64_t total_out_of_order = 0;
		struct dap_stats_out_of_order_entry *e2, *tmp2;
		HASH_ITER(hh, e->sub_out_of_order_stats, e2, tmp2){
			total_out_of_order += e2->out_of_order_count;
		}

		fprintf(stats_file, "%" PRIu64 ",%s,%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRIu64 ",%" PRId64 ",%" PRId64 ",%" PRIu64,
			wall_ms,
			e->sub_id,
			e->stats[dap_stat_current_queue_size],
			e->stats[dap_stat_current_inflight_count],
			e->stats[dap_stat_dropped_by_policy_count],
			e->stats[dap_stat_dropped_by_policy_reeval_count],
			total_out_of_order,
			e->stats[dap_stat_ops_originated_from_broker],
			e->stats[dap_stat_bump_count],
			e->latency_count
			);
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

void dap_stats_close(void)
{
    if(stats_file){
        dap_stats__write(true);
        fclose(stats_file);
        stats_file = NULL;
    }
    if(!db.dap_stats_list) return;

	struct dap_stats_entry *e, *tmp;
	HASH_ITER(hh, db.dap_stats_list->client_stats, e, tmp){
		struct dap_stats_out_of_order_entry *e2, *tmp2;
		HASH_ITER(hh, e->sub_out_of_order_stats, e2, tmp2){
			HASH_DEL(e->sub_out_of_order_stats, e2);
			mosquitto_FREE(e2);
		}
		HASH_DEL(db.dap_stats_list->client_stats, e);
		mosquitto_FREE(e->sub_id);
		mosquitto_FREE(e);
	}
}
