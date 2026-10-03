/*
Copyright (c) 2021 Roger Light <roger@atchoo.org>

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

#include <string.h>
#include <sqlite3.h>

#include "mosquitto.h"
#include "mosquitto/broker.h"
#include "persist_sqlite.h"


/* A subscription's SP as one '|'-joined string, or NULL when it has none. */
static char *purposes_join(const struct mosquitto_subscription *sub)
{
	size_t len = 1;
	char *joined, *q;

	if(sub->purpose_filter_count == 0) return NULL;
	for(uint32_t i = 0; i < sub->purpose_filter_count; i++){
		len += strlen(sub->purpose_filters[i]) + 1;
	}
	joined = mosquitto_malloc(len);
	if(!joined) return NULL;
	q = joined;
	for(uint32_t i = 0; i < sub->purpose_filter_count; i++){
		size_t n = strlen(sub->purpose_filters[i]);
		if(i > 0) *q++ = '|';
		memcpy(q, sub->purpose_filters[i], n);
		q += n;
	}
	*q = '\0';
	return joined;
}


int persist_sqlite__subscription_add_cb(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_persist_subscription *ed = event_data;
	struct mosquitto_sqlite *ms = userdata;
	int rc = MOSQ_ERR_UNKNOWN;
	char *purposes = purposes_join(&ed->data);

	UNUSED(event);

	if(sqlite3_bind_text(ms->subscription_add_stmt, 1,
			ed->data.clientid, (int)strlen(ed->data.clientid), SQLITE_STATIC) == SQLITE_OK){

		if(sqlite3_bind_text(ms->subscription_add_stmt, 2,
				ed->data.topic_filter, (int)strlen(ed->data.topic_filter), SQLITE_STATIC) == SQLITE_OK){

			if(sqlite3_bind_int(ms->subscription_add_stmt, 3,
					ed->data.options) == SQLITE_OK){

				if(sqlite3_bind_int(ms->subscription_add_stmt, 4,
						(int)ed->data.identifier) == SQLITE_OK
						&& (purposes
							? sqlite3_bind_text(ms->subscription_add_stmt, 5, purposes, (int)strlen(purposes), SQLITE_STATIC)
							: sqlite3_bind_null(ms->subscription_add_stmt, 5)) == SQLITE_OK){

					ms->event_count++;
					rc = sqlite3_step(ms->subscription_add_stmt);
					if(rc == SQLITE_DONE){
						rc = MOSQ_ERR_SUCCESS;
					}else{
						rc = MOSQ_ERR_UNKNOWN;
					}
				}
			}
		}
	}
	sqlite3_reset(ms->subscription_add_stmt);
	mosquitto_free(purposes);

	return rc;
}


int persist_sqlite__subscription_remove_cb(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_persist_subscription *ed = event_data;
	struct mosquitto_sqlite *ms = userdata;
	int rc = 1;

	UNUSED(event);

	if(sqlite3_bind_text(ms->subscription_remove_stmt, 1,
			ed->data.clientid, (int)strlen(ed->data.clientid), SQLITE_STATIC) == SQLITE_OK){

		if(sqlite3_bind_text(ms->subscription_remove_stmt, 2,
				ed->data.topic_filter, (int)strlen(ed->data.topic_filter), SQLITE_STATIC) == SQLITE_OK){

			ms->event_count++;
			rc = sqlite3_step(ms->subscription_remove_stmt);
			if(rc == SQLITE_DONE){
				rc = MOSQ_ERR_SUCCESS;
			}else{
				rc = MOSQ_ERR_UNKNOWN;
			}
		}
	}
	sqlite3_reset(ms->subscription_remove_stmt);

	return rc;
}
