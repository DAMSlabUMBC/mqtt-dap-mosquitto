/* MQTT-DAP pending-operation persistence. */

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sqlite3.h>

#include "mosquitto.h"
#include "mosquitto/broker.h"
#include "persist_sqlite.h"
#include "util.h"


int persist_sqlite__dap_init(struct mosquitto_sqlite *ms)
{
	int rc;

	rc = sqlite3_exec(ms->db,
			"CREATE TABLE IF NOT EXISTS dap_ops "
			"("
			"op_id INT64 PRIMARY KEY,"
			"publisher_id TEXT NOT NULL,"
			"op_type INTEGER,"
			"timestamp INT64,"
			"topic_filters TEXT,"
			"purpose_filters TEXT,"
			"client_filters TEXT,"
			"deadline INT64"
			");",
			NULL, NULL, NULL);
	if(rc){
		goto fail;
	}
	/* Tables created before deadlines were persisted lack the column. */
	sqlite3_exec(ms->db, "ALTER TABLE dap_ops ADD COLUMN deadline INT64", NULL, NULL, NULL);

	rc = sqlite3_exec(ms->db,
			"CREATE TABLE IF NOT EXISTS dap_tracked_ops "
			"("
			"op_id INT64 PRIMARY KEY,"
			"publisher_id TEXT NOT NULL,"
			"deadline INT64,"
			"settled INTEGER NOT NULL DEFAULT 0"
			");",
			NULL, NULL, NULL);
	if(rc){
		goto fail;
	}

	rc = sqlite3_exec(ms->db,
			"CREATE TABLE IF NOT EXISTS dap_flows "
			"("
			"publisher_id TEXT NOT NULL,"
			"topic TEXT NOT NULL,"
			"subscriber_id TEXT NOT NULL,"
			"purposes TEXT NOT NULL,"
			"first_time INT64,"
			"last_time INT64,"
			"PRIMARY KEY (publisher_id, topic, subscriber_id, purposes)"
			");",
			NULL, NULL, NULL);
	if(rc){
		goto fail;
	}

	rc = sqlite3_exec(ms->db,
			"CREATE TABLE IF NOT EXISTS dap_requests "
			"("
			"subscriber_id TEXT NOT NULL,"
			"op_id INT64 NOT NULL,"
			"deadline INT64,"
			"payload BLOB,"
			"properties TEXT,"
			"PRIMARY KEY (subscriber_id, op_id)"
			");",
			NULL, NULL, NULL);
	if(rc){
		goto fail;
	}

	rc = sqlite3_exec(ms->db,
			"CREATE TABLE IF NOT EXISTS dap_tracked_op_subs "
			"("
			"op_id INT64 NOT NULL,"
			"sub_id TEXT NOT NULL,"
			"responded INTEGER NOT NULL DEFAULT 0,"
			"status TEXT,"
			"reason TEXT,"
			"PRIMARY KEY (op_id, sub_id)"
			");",
			NULL, NULL, NULL);
	if(rc){
		goto fail;
	}
	/* Tables created before statuses were persisted lack the columns. */
	sqlite3_exec(ms->db, "ALTER TABLE dap_tracked_op_subs ADD COLUMN status TEXT", NULL, NULL, NULL);
	sqlite3_exec(ms->db, "ALTER TABLE dap_tracked_op_subs ADD COLUMN reason TEXT", NULL, NULL, NULL);

	rc = sqlite3_prepare_v3(ms->db,
			"INSERT OR REPLACE INTO dap_ops "
			"(op_id, publisher_id, op_type, timestamp, topic_filters, purpose_filters, client_filters, deadline) "
			"VALUES(?,?,?,?,?,?,?,?)",
			-1, SQLITE_PREPARE_PERSISTENT,
			&ms->dap_op_add_stmt, NULL);
	if(rc){
		goto fail;
	}

	rc = sqlite3_prepare_v3(ms->db,
			"INSERT OR REPLACE INTO dap_flows "
			"(publisher_id, topic, subscriber_id, purposes, first_time, last_time) "
			"VALUES(?,?,?,?,?,?)",
			-1, SQLITE_PREPARE_PERSISTENT,
			&ms->dap_flow_add_stmt, NULL);
	if(rc){
		goto fail;
	}

	rc = sqlite3_prepare_v3(ms->db,
			"INSERT OR REPLACE INTO dap_requests "
			"(subscriber_id, op_id, deadline, payload, properties) "
			"VALUES(?,?,?,?,?)",
			-1, SQLITE_PREPARE_PERSISTENT,
			&ms->dap_request_add_stmt, NULL);
	if(rc){
		goto fail;
	}

	rc = sqlite3_prepare_v3(ms->db,
			"DELETE FROM dap_requests WHERE subscriber_id=?",
			-1, SQLITE_PREPARE_PERSISTENT,
			&ms->dap_request_delete_sub_stmt, NULL);
	if(rc){
		goto fail;
	}

	rc = sqlite3_prepare_v3(ms->db,
			"DELETE FROM dap_requests WHERE deadline<=?",
			-1, SQLITE_PREPARE_PERSISTENT,
			&ms->dap_request_delete_expired_stmt, NULL);
	if(rc){
		goto fail;
	}

	rc = sqlite3_prepare_v3(ms->db,
			"DELETE FROM dap_ops WHERE op_id=?",
			-1, SQLITE_PREPARE_PERSISTENT,
			&ms->dap_op_delete_stmt, NULL);
	if(rc){
		goto fail;
	}

	rc = sqlite3_prepare_v3(ms->db,
			"INSERT OR REPLACE INTO dap_tracked_ops "
			"(op_id, publisher_id, deadline, settled) "
			"VALUES(?,?,?,0)",
			-1, SQLITE_PREPARE_PERSISTENT,
			&ms->dap_tracked_op_add_stmt, NULL);
	if(rc){
		goto fail;
	}

	rc = sqlite3_prepare_v3(ms->db,
			"INSERT OR REPLACE INTO dap_tracked_op_subs "
			"(op_id, sub_id, responded) "
			"VALUES(?,?,0)",
			-1, SQLITE_PREPARE_PERSISTENT,
			&ms->dap_tracked_op_sub_add_stmt, NULL);
	if(rc){
		goto fail;
	}

	rc = sqlite3_prepare_v3(ms->db,
			"UPDATE dap_tracked_op_subs SET responded=1, status=?, reason=? WHERE op_id=? AND sub_id=?",
			-1, SQLITE_PREPARE_PERSISTENT,
			&ms->dap_tracked_op_response_stmt, NULL);
	if(rc){
		goto fail;
	}

	rc = sqlite3_prepare_v3(ms->db,
			"UPDATE dap_tracked_ops SET settled=1 WHERE op_id=?",
			-1, SQLITE_PREPARE_PERSISTENT,
			&ms->dap_tracked_op_settle_stmt, NULL);
	if(rc){
		goto fail;
	}

	rc = sqlite3_prepare_v3(ms->db,
			"DELETE FROM dap_tracked_op_subs WHERE op_id=?",
			-1, SQLITE_PREPARE_PERSISTENT,
			&ms->dap_tracked_op_subs_clear_stmt, NULL);
	if(rc){
		goto fail;
	}

	return 0;
fail:
	mosquitto_log_printf(MOSQ_LOG_ERR, "Sqlite persistence: Error preparing DAP operation tables: %s %s",
			sqlite3_errstr(rc), sqlite3_errmsg(ms->db));
	return 1;
}


void persist_sqlite__dap_cleanup(struct mosquitto_sqlite *ms)
{
	sqlite3_finalize(ms->dap_op_add_stmt);
	sqlite3_finalize(ms->dap_op_delete_stmt);
	sqlite3_finalize(ms->dap_flow_add_stmt);
	sqlite3_finalize(ms->dap_request_add_stmt);
	sqlite3_finalize(ms->dap_request_delete_sub_stmt);
	sqlite3_finalize(ms->dap_request_delete_expired_stmt);
	sqlite3_finalize(ms->dap_tracked_op_add_stmt);
	sqlite3_finalize(ms->dap_tracked_op_sub_add_stmt);
	sqlite3_finalize(ms->dap_tracked_op_response_stmt);
	sqlite3_finalize(ms->dap_tracked_op_settle_stmt);
	sqlite3_finalize(ms->dap_tracked_op_subs_clear_stmt);
}


/* NULL is stored as SQL NULL. */
static int bind_text(sqlite3_stmt *stmt, int col, const char *value)
{
	if(value == NULL){
		return sqlite3_bind_null(stmt, col);
	}
	return sqlite3_bind_text(stmt, col, value, (int)strlen(value), SQLITE_STATIC);
}


static int step_and_reset(struct mosquitto_sqlite *ms, sqlite3_stmt *stmt)
{
	int rc;

	ms->event_count++;
	rc = sqlite3_step(stmt);
	sqlite3_reset(stmt);
	return rc == SQLITE_DONE ? MOSQ_ERR_SUCCESS : MOSQ_ERR_UNKNOWN;
}


int persist_sqlite__dap_op_add_cb(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_persist_dap_op *ed = event_data;
	struct mosquitto_sqlite *ms = userdata;
	sqlite3_stmt *stmt = ms->dap_op_add_stmt;

	UNUSED(event);

	if(sqlite3_bind_int64(stmt, 1, (int64_t)ed->data.op_id) == SQLITE_OK
			&& bind_text(stmt, 2, ed->data.publisher_id) == SQLITE_OK
			&& sqlite3_bind_int(stmt, 3, ed->data.op_type) == SQLITE_OK
			&& sqlite3_bind_int64(stmt, 4, (int64_t)ed->data.timestamp) == SQLITE_OK
			&& bind_text(stmt, 5, ed->data.topic_filters) == SQLITE_OK
			&& bind_text(stmt, 6, ed->data.purpose_filters) == SQLITE_OK
			&& bind_text(stmt, 7, ed->data.client_filters) == SQLITE_OK
			&& sqlite3_bind_int64(stmt, 8, (int64_t)ed->data.deadline) == SQLITE_OK
			){

		return step_and_reset(ms, stmt);
	}
	sqlite3_reset(stmt);
	return MOSQ_ERR_UNKNOWN;
}


int persist_sqlite__dap_flow_add_cb(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_persist_dap_flow *ed = event_data;
	struct mosquitto_sqlite *ms = userdata;
	sqlite3_stmt *stmt = ms->dap_flow_add_stmt;

	UNUSED(event);

	if(bind_text(stmt, 1, ed->data.publisher_id) == SQLITE_OK
			&& bind_text(stmt, 2, ed->data.topic) == SQLITE_OK
			&& bind_text(stmt, 3, ed->data.subscriber_id) == SQLITE_OK
			&& bind_text(stmt, 4, ed->data.purposes ? ed->data.purposes : "") == SQLITE_OK
			&& sqlite3_bind_int64(stmt, 5, (int64_t)ed->data.first_time) == SQLITE_OK
			&& sqlite3_bind_int64(stmt, 6, (int64_t)ed->data.last_time) == SQLITE_OK
			){

		return step_and_reset(ms, stmt);
	}
	sqlite3_reset(stmt);
	return MOSQ_ERR_UNKNOWN;
}


int persist_sqlite__dap_request_add_cb(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_persist_dap_request *ed = event_data;
	struct mosquitto_sqlite *ms = userdata;
	sqlite3_stmt *stmt = ms->dap_request_add_stmt;
	char *properties = ed->data.properties ? properties_to_json_str(ed->data.properties) : NULL;
	int rc = MOSQ_ERR_UNKNOWN;

	UNUSED(event);

	if(bind_text(stmt, 1, ed->data.subscriber_id) == SQLITE_OK
			&& sqlite3_bind_int64(stmt, 2, (int64_t)ed->data.op_id) == SQLITE_OK
			&& sqlite3_bind_int64(stmt, 3, (int64_t)ed->data.deadline) == SQLITE_OK
			&& (ed->data.payloadlen
				? sqlite3_bind_blob(stmt, 4, ed->data.payload, (int)ed->data.payloadlen, SQLITE_STATIC)
				: sqlite3_bind_null(stmt, 4)) == SQLITE_OK
			&& bind_text(stmt, 5, properties) == SQLITE_OK
			){

		rc = step_and_reset(ms, stmt);
	}else{
		sqlite3_reset(stmt);
	}
	free(properties);
	return rc;
}


int persist_sqlite__dap_request_delete_cb(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_persist_dap_request *ed = event_data;
	struct mosquitto_sqlite *ms = userdata;
	sqlite3_stmt *stmt;

	UNUSED(event);

	if(ed->data.subscriber_id){
		stmt = ms->dap_request_delete_sub_stmt;
		if(bind_text(stmt, 1, ed->data.subscriber_id) != SQLITE_OK){
			sqlite3_reset(stmt);
			return MOSQ_ERR_UNKNOWN;
		}
	}else{
		stmt = ms->dap_request_delete_expired_stmt;
		if(sqlite3_bind_int64(stmt, 1, (int64_t)ed->data.deadline) != SQLITE_OK){
			sqlite3_reset(stmt);
			return MOSQ_ERR_UNKNOWN;
		}
	}
	return step_and_reset(ms, stmt);
}


int persist_sqlite__dap_op_delete_cb(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_persist_dap_op *ed = event_data;
	struct mosquitto_sqlite *ms = userdata;

	UNUSED(event);

	if(sqlite3_bind_int64(ms->dap_op_delete_stmt, 1, (int64_t)ed->data.op_id) != SQLITE_OK){
		sqlite3_reset(ms->dap_op_delete_stmt);
		return MOSQ_ERR_UNKNOWN;
	}
	return step_and_reset(ms, ms->dap_op_delete_stmt);
}


int persist_sqlite__dap_tracked_op_add_cb(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_persist_dap_tracked_op *ed = event_data;
	struct mosquitto_sqlite *ms = userdata;
	sqlite3_stmt *stmt = ms->dap_tracked_op_add_stmt;
	int rc;

	UNUSED(event);

	if(sqlite3_bind_int64(stmt, 1, (int64_t)ed->data.op_id) != SQLITE_OK
			|| bind_text(stmt, 2, ed->data.publisher_id) != SQLITE_OK
			|| sqlite3_bind_int64(stmt, 3, (int64_t)ed->data.deadline) != SQLITE_OK
			){

		sqlite3_reset(stmt);
		return MOSQ_ERR_UNKNOWN;
	}
	rc = step_and_reset(ms, stmt);
	if(rc){
		return rc;
	}

	stmt = ms->dap_tracked_op_sub_add_stmt;
	for(size_t i=0; i<ed->data.num_expected; i++){
		if(sqlite3_bind_int64(stmt, 1, (int64_t)ed->data.op_id) != SQLITE_OK
				|| bind_text(stmt, 2, ed->data.expected_subs[i]) != SQLITE_OK
				){

			sqlite3_reset(stmt);
			return MOSQ_ERR_UNKNOWN;
		}
		rc = step_and_reset(ms, stmt);
		if(rc){
			return rc;
		}
	}
	return MOSQ_ERR_SUCCESS;
}


int persist_sqlite__dap_tracked_op_response_cb(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_persist_dap_tracked_op *ed = event_data;
	struct mosquitto_sqlite *ms = userdata;
	sqlite3_stmt *stmt = ms->dap_tracked_op_response_stmt;

	UNUSED(event);

	if(bind_text(stmt, 1, ed->status) == SQLITE_OK
			&& bind_text(stmt, 2, ed->reason) == SQLITE_OK
			&& sqlite3_bind_int64(stmt, 3, (int64_t)ed->data.op_id) == SQLITE_OK
			&& bind_text(stmt, 4, ed->subscriber_id) == SQLITE_OK
			){

		return step_and_reset(ms, stmt);
	}
	sqlite3_reset(stmt);
	return MOSQ_ERR_UNKNOWN;
}


int persist_sqlite__dap_tracked_op_delete_cb(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_persist_dap_tracked_op *ed = event_data;
	struct mosquitto_sqlite *ms = userdata;
	int rc;

	UNUSED(event);

	/* Keep the requester row for late responses, and the per-subscriber rows until
	 * the deadline so a settled op's status can still be requested. */
	if(sqlite3_bind_int64(ms->dap_tracked_op_settle_stmt, 1, (int64_t)ed->data.op_id) != SQLITE_OK){
		sqlite3_reset(ms->dap_tracked_op_settle_stmt);
		return MOSQ_ERR_UNKNOWN;
	}
	rc = step_and_reset(ms, ms->dap_tracked_op_settle_stmt);
	if(rc || ed->data.settled){
		return rc;
	}

	if(sqlite3_bind_int64(ms->dap_tracked_op_subs_clear_stmt, 1, (int64_t)ed->data.op_id) != SQLITE_OK){
		sqlite3_reset(ms->dap_tracked_op_subs_clear_stmt);
		return MOSQ_ERR_UNKNOWN;
	}
	return step_and_reset(ms, ms->dap_tracked_op_subs_clear_stmt);
}


static int dap_op_restore(struct mosquitto_sqlite *ms)
{
	sqlite3_stmt *stmt;
	struct mosquitto_dap_op op;
	int rc;
	long count = 0, failed = 0;

	rc = sqlite3_prepare_v2(ms->db,
			"SELECT op_id, publisher_id, op_type, timestamp, topic_filters, purpose_filters, client_filters, deadline "
			"FROM dap_ops ORDER BY op_id",
			-1, &stmt, NULL);
	if(rc != SQLITE_OK){
		mosquitto_log_printf(MOSQ_LOG_ERR, "sqlite: Error restoring DAP operations: %s", sqlite3_errstr(rc));
		return MOSQ_ERR_UNKNOWN;
	}

	/* Ascending id reproduces the broker's list order. */
	while(sqlite3_step(stmt) == SQLITE_ROW){
		memset(&op, 0, sizeof(op));
		op.op_id = (uint64_t)sqlite3_column_int64(stmt, 0);
		op.publisher_id = (const char *)sqlite3_column_text(stmt, 1);
		op.op_type = sqlite3_column_int(stmt, 2);
		op.timestamp = (time_t)sqlite3_column_int64(stmt, 3);
		op.topic_filters = (const char *)sqlite3_column_text(stmt, 4);
		op.purpose_filters = (const char *)sqlite3_column_text(stmt, 5);
		op.client_filters = (const char *)sqlite3_column_text(stmt, 6);
		op.deadline = (time_t)sqlite3_column_int64(stmt, 7);

		if(mosquitto_persist_dap_op_add(&op) == MOSQ_ERR_SUCCESS){
			count++;
		}else{
			failed++;
		}
	}
	sqlite3_finalize(stmt);

	mosquitto_log_printf(MOSQ_LOG_INFO, "sqlite: Restored %ld DAP pending operations (%ld failed)", count, failed);
	return MOSQ_ERR_SUCCESS;
}


struct dap_tracked_subs {
	char **subs;
	bool *responded;
	char **statuses;
	char **reasons;
	size_t n;
};


static void dap_tracked_subs_free(struct dap_tracked_subs *t)
{
	for(size_t i=0; i<t->n; i++){
		free(t->subs[i]);
		free(t->statuses[i]);
		free(t->reasons[i]);
	}
	free(t->subs);
	free(t->responded);
	free(t->statuses);
	free(t->reasons);
	memset(t, 0, sizeof(*t));
}


static char *column_strdup(sqlite3_stmt *stmt, int col)
{
	const char *value = (const char *)sqlite3_column_text(stmt, col);
	return value ? strdup(value) : NULL;
}


/* Load an op's expected subscribers and their responses (copied; sqlite reuses
 * column text). */
static int dap_tracked_op_load_subs(struct mosquitto_sqlite *ms, struct mosquitto_dap_tracked_op *op,
		struct dap_tracked_subs *t)
{
	sqlite3_stmt *stmt;
	int rc;

	rc = sqlite3_prepare_v2(ms->db,
			"SELECT sub_id, responded, status, reason FROM dap_tracked_op_subs WHERE op_id=? ORDER BY rowid",
			-1, &stmt, NULL);
	if(rc != SQLITE_OK){
		return MOSQ_ERR_UNKNOWN;
	}
	sqlite3_bind_int64(stmt, 1, (int64_t)op->op_id);

	rc = MOSQ_ERR_SUCCESS;
	while(sqlite3_step(stmt) == SQLITE_ROW){
		size_t n = t->n + 1;
		char **subs = realloc(t->subs, n*sizeof(char *));
		if(subs){
			t->subs = subs;
		}
		bool *responded = subs ? realloc(t->responded, n*sizeof(bool)) : NULL;
		if(responded){
			t->responded = responded;
		}
		char **statuses = responded ? realloc(t->statuses, n*sizeof(char *)) : NULL;
		if(statuses){
			t->statuses = statuses;
		}
		char **reasons = statuses ? realloc(t->reasons, n*sizeof(char *)) : NULL;
		if(reasons){
			t->reasons = reasons;
		}
		if(!reasons){
			rc = MOSQ_ERR_NOMEM;
			break;
		}
		t->subs[t->n] = strdup((const char *)sqlite3_column_text(stmt, 0));
		t->responded[t->n] = sqlite3_column_int(stmt, 1) != 0;
		t->statuses[t->n] = column_strdup(stmt, 2);
		t->reasons[t->n] = column_strdup(stmt, 3);
		t->n = n;
		if(!t->subs[t->n-1]){
			rc = MOSQ_ERR_NOMEM;
			break;
		}
	}
	sqlite3_finalize(stmt);

	op->expected_subs = (const char *const *)t->subs;
	op->responded = t->responded;
	op->statuses = (const char *const *)t->statuses;
	op->reasons = (const char *const *)t->reasons;
	op->num_expected = t->n;
	return rc;
}


static int dap_tracked_op_restore(struct mosquitto_sqlite *ms)
{
	sqlite3_stmt *stmt;
	struct mosquitto_dap_tracked_op op;
	time_t now = time(NULL);
	int rc;
	long count = 0, failed = 0;

	rc = sqlite3_prepare_v2(ms->db,
			"SELECT op_id, publisher_id, deadline, settled FROM dap_tracked_ops ORDER BY op_id",
			-1, &stmt, NULL);
	if(rc != SQLITE_OK){
		mosquitto_log_printf(MOSQ_LOG_ERR, "sqlite: Error restoring DAP tracked operations: %s", sqlite3_errstr(rc));
		return MOSQ_ERR_UNKNOWN;
	}

	while(sqlite3_step(stmt) == SQLITE_ROW){
		struct dap_tracked_subs subs;

		memset(&op, 0, sizeof(op));
		memset(&subs, 0, sizeof(subs));
		op.op_id = (uint64_t)sqlite3_column_int64(stmt, 0);
		op.publisher_id = (const char *)sqlite3_column_text(stmt, 1);
		op.deadline = (time_t)sqlite3_column_int64(stmt, 2);
		op.settled = sqlite3_column_int(stmt, 3) != 0;

		rc = MOSQ_ERR_SUCCESS;
		/* A settled op's statuses can be requested until its deadline. */
		if(!op.settled || op.deadline > now){
			rc = dap_tracked_op_load_subs(ms, &op, &subs);
		}
		if(rc == MOSQ_ERR_SUCCESS && mosquitto_persist_dap_tracked_op_add(&op) == MOSQ_ERR_SUCCESS){
			count++;
		}else{
			failed++;
		}
		dap_tracked_subs_free(&subs);
	}
	sqlite3_finalize(stmt);

	mosquitto_log_printf(MOSQ_LOG_INFO, "sqlite: Restored %ld DAP tracked operations (%ld failed)", count, failed);
	return MOSQ_ERR_SUCCESS;
}


static int dap_flow_restore(struct mosquitto_sqlite *ms)
{
	sqlite3_stmt *stmt;
	struct mosquitto_dap_flow flow;
	int rc;
	long count = 0, failed = 0;

	rc = sqlite3_prepare_v2(ms->db,
			"SELECT publisher_id, topic, subscriber_id, purposes, first_time, last_time FROM dap_flows",
			-1, &stmt, NULL);
	if(rc != SQLITE_OK){
		mosquitto_log_printf(MOSQ_LOG_ERR, "sqlite: Error restoring DAP flows: %s", sqlite3_errstr(rc));
		return MOSQ_ERR_UNKNOWN;
	}

	while(sqlite3_step(stmt) == SQLITE_ROW){
		memset(&flow, 0, sizeof(flow));
		flow.publisher_id = (const char *)sqlite3_column_text(stmt, 0);
		flow.topic = (const char *)sqlite3_column_text(stmt, 1);
		flow.subscriber_id = (const char *)sqlite3_column_text(stmt, 2);
		flow.purposes = (const char *)sqlite3_column_text(stmt, 3);
		flow.first_time = (time_t)sqlite3_column_int64(stmt, 4);
		flow.last_time = (time_t)sqlite3_column_int64(stmt, 5);

		if(mosquitto_persist_dap_flow_add(&flow) == MOSQ_ERR_SUCCESS){
			count++;
		}else{
			failed++;
		}
	}
	sqlite3_finalize(stmt);

	mosquitto_log_printf(MOSQ_LOG_INFO, "sqlite: Restored %ld DAP flows (%ld failed)", count, failed);
	return MOSQ_ERR_SUCCESS;
}


static int dap_request_restore(struct mosquitto_sqlite *ms)
{
	sqlite3_stmt *stmt;
	struct mosquitto_dap_request request;
	int rc;
	long count = 0, failed = 0;

	rc = sqlite3_prepare_v2(ms->db,
			"SELECT subscriber_id, op_id, deadline, payload, properties FROM dap_requests ORDER BY rowid",
			-1, &stmt, NULL);
	if(rc != SQLITE_OK){
		mosquitto_log_printf(MOSQ_LOG_ERR, "sqlite: Error restoring DAP requests: %s", sqlite3_errstr(rc));
		return MOSQ_ERR_UNKNOWN;
	}

	while(sqlite3_step(stmt) == SQLITE_ROW){
		mosquitto_property *properties = json_to_properties((const char *)sqlite3_column_text(stmt, 4));

		memset(&request, 0, sizeof(request));
		request.subscriber_id = (const char *)sqlite3_column_text(stmt, 0);
		request.op_id = (uint64_t)sqlite3_column_int64(stmt, 1);
		request.deadline = (time_t)sqlite3_column_int64(stmt, 2);
		request.payload = sqlite3_column_blob(stmt, 3);
		request.payloadlen = (uint32_t)sqlite3_column_bytes(stmt, 3);
		request.properties = properties;

		if(mosquitto_persist_dap_request_add(&request) == MOSQ_ERR_SUCCESS){
			count++;
		}else{
			failed++;
		}
		mosquitto_property_free_all(&properties);
	}
	sqlite3_finalize(stmt);

	mosquitto_log_printf(MOSQ_LOG_INFO, "sqlite: Restored %ld DAP requests (%ld failed)", count, failed);
	return MOSQ_ERR_SUCCESS;
}


int persist_sqlite__dap_restore(struct mosquitto_sqlite *ms)
{
	if(dap_op_restore(ms) || dap_flow_restore(ms) || dap_request_restore(ms)){
		return MOSQ_ERR_UNKNOWN;
	}
	return dap_tracked_op_restore(ms);
}
