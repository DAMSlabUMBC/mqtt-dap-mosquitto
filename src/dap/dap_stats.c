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
#include <inttypes.h>

#include "mosquitto_broker_internal.h"
#include "dap_stats.h"

static FILE *stats_file = NULL;

static time_t start_time = 0;
static time_t last_update = 0;

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
	for(int i = 0; i < dap_stat_max; i++)
	{
		e->stats[i] = 0;
	}
    HASH_ADD_KEYPTR(hh, l->client_stats, e->sub_id, strlen(e->sub_id), e);
    return e;
}

void dap_stats__init(void)
{
	db.dap_stats_list->client_stats = NULL;

	if(db.config->stats_interval == 0){
		return;
	}

	if(stats_file) return;
    stats_file = fopen("/tmp/dap_broker_stats.csv", "w");
    if(!stats_file) return;
    fprintf(stats_file,
            "timestamp,client_id,queue_size,inflight_count,initially_dropped_by_policy,dropped_on_reeval,out_of_order,op_msgs_from_broker,total_bump_count\n");
    fflush(stats_file);

	start_time = mosquitto_time();
	last_update = start_time;
}

void dap_stats__int_inc(const char* client_id, enum dap_stat_type s, int64_t value)
{
	if(!client_id || (s < 0) || (s >= dap_stat_max)) return;

    struct dap_stats_entry *e = dap_stats__find_or_make_entry(db.dap_stats_list, client_id);
    if(e)
	{
		e->stats[s] += value;
	}
}


void dap_stats__int_dec(const char* client_id, enum dap_stat_type s, int64_t value)
{
	if(!client_id || (s < 0) || (s >= dap_stat_max)) return;

    struct dap_stats_entry *e = dap_stats__find_or_make_entry(db.dap_stats_list, client_id);
    if(e)
	{
		e->stats[s] -= value;
	}
}

void dap_stats__int_set(const char* client_id, enum dap_stat_type s, int64_t value)
{
	if(!client_id || (s < 0) || (s >= dap_stat_max)) return;

    struct dap_stats_entry *e = dap_stats__find_or_make_entry(db.dap_stats_list, client_id);
    if(e)
	{
		e->stats[s] = value;
	}
}


/* 
 * Write the statistics to the DAP tracking file
 */
void dap_stats__write(bool force)
{
	static time_t next_update_real = 0;

	if(!stats_file) return;

	if(db.config->stats_interval && ((next_update_real == 0 || db.now_real_s > next_update_real) || force))
	{
		// Perform update by looping through each client
		struct dap_stats_entry *e, *tmp;
		HASH_ITER(hh, db.dap_stats_list->client_stats, e, tmp){
			fprintf(stats_file, "%" PRId64 ",%s,%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64 "\n",
				db.now_s,
				e->sub_id,
				e->stats[dap_stat_current_queue_size],
				e->stats[dap_stat_current_inflight_count],
				e->stats[dap_stat_dropped_by_policy_count],
				e->stats[dap_stat_dropped_by_policy_reeval_count],
				e->stats[dap_stat_out_of_order_count],
				e->stats[dap_stat_ops_originated_from_broker],
				e->stats[dap_stat_bump_count]
				);
		}

		fflush(stats_file);
		
		last_update = db.now_s;
		next_update_real = db.now_real_s + db.config->stats_interval;
	}
}

void dap_stats_close(void)
{
    if(!stats_file) return;
    fclose(stats_file);
    stats_file = NULL;
}