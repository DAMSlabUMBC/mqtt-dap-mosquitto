/* Per-client stats for the MQTT-DAP baseline, mirroring the DAP broker's dap_stats. */

#ifndef BROKER_STATS_H
#define BROKER_STATS_H

#include <stdbool.h>
#include <stdint.h>

#include "uthash.h"

enum broker_stat_type {
	broker_stat_current_queue_size = 0,
	broker_stat_current_inflight_count = 1,
	broker_stat_max,
};

/* The stats for one client keyed by client id in the uthash map. */
struct broker_stats_entry {
	char *client_id;           /* hash key */
	int64_t stats[broker_stat_max];
	/* Time through broker this interval; reset after every row. */
	uint64_t latency_count;
	uint64_t latency_sum_ns;
	uint64_t latency_min_ns;
	uint64_t latency_max_ns;
	UT_hash_handle hh;
};

struct mosquitto__base_msg;

void broker_stats__int_set(const char *client_id, enum broker_stat_type s, int64_t value);
void broker_stats__record_delivery(const char *client_id, const struct mosquitto__base_msg *base_msg);
void broker_stats__init(void);
void broker_stats__write(bool force);
void broker_stats__close(void);

#endif
