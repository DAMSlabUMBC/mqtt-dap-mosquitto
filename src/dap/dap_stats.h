/*
Copyright (c) 2015-2021 Roger Light <roger@atchoo.org>

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

#ifndef DAP_STATS_H
#define DAP_STATS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>

#include "uthash.h"

/* This ordering *must* match the metrics array in dap_stats.c. */
enum dap_stat_type {
	dap_stat_current_queue_size = 0,
	dap_stat_current_inflight_count = 1,
	dap_stat_dropped_by_policy_count = 2,
	dap_stat_dropped_by_policy_reeval_count = 3,
	dap_stat_ops_originated_from_broker = 4,
	dap_stat_bump_count = 5,
	dap_stat_max,
};

// /* This ordering *must* match the metrics array in dap_stats.c. */
// enum dap_stat_type {
// 	dap_stat_current_queue_size = 0,
// 	dap_stat_current_inflight_count = 1,
// 	dap_stat_dropped_by_policy_count = 2,
// 	dap_stat_out_of_order_count = 3,
// 	dap_stat_time_through_broker_average = 4,
// 	dap_stat_time_through_broker_min = 5,
// 	dap_stat_time_through_broker_max = 6,
// 	dap_stat_time_through_broker_p90 = 7,
// 	dap_stat_time_through_broker_p95 = 8,
// 	dap_stat_time_through_broker_p99 = 9,
// 	dap_stat_ops_originated_from_broker = 10,
// 	dap_stat_bump_count = 11,
// 	dap_stat_max,
// };

struct dap_stats_out_of_order_entry {
    uint32_t subscription_id; /* hash key */
    uint64_t last_pub_sent_recv_ns_mono;
	uint64_t out_of_order_count;
    UT_hash_handle hh;
};

/* The stats for one client keyed by subscription id in the uthash map. */
struct dap_stats_entry {
    char *sub_id;              /* hash key */
    int64_t stats[dap_stat_max];
    /* Time through broker this interval; reset after every row. */
    uint64_t latency_count;
    uint64_t latency_sum_ns;
    uint64_t latency_min_ns;
    uint64_t latency_max_ns;
	struct dap_stats_out_of_order_entry *sub_out_of_order_stats; /* uthash head: subscription_id */
    UT_hash_handle hh;
};

/* Stats list for every client. */
struct dap_stats_list {
    struct dap_stats_entry *client_stats; /* uthash head: sub_id */
};

struct mosquitto__base_msg;

void dap_stats__int_inc(const char* client_id, enum dap_stat_type s, int64_t value);
void dap_stats__int_dec(const char* client_id, enum dap_stat_type s, int64_t value);
void dap_stats__int_set(const char* client_id, enum dap_stat_type s, int64_t value);
void dap_stats__record_delivery(const char *client_id, const struct mosquitto__base_msg *base_msg);
void dap_stats__pub_sent(const char* client_id, uint32_t subscription_id, uint64_t msg_recv_time_ns_mono);
void dap_stats__init(void);
void dap_stats__write(bool force);
void dap_stats_close(void);

#ifdef __cplusplus
}
#endif

#endif