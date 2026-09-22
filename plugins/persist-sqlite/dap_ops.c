/* MQTT-DAP pending-operation persistence. */

#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>

#include "mosquitto.h"
#include "mosquitto/broker.h"
#include "persist_sqlite.h"


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
			"client_filters TEXT"
			");",
			NULL, NULL, NULL);
	if(rc){
		goto fail;
	}

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
			"CREATE TABLE IF NOT EXISTS dap_tracked_op_subs "
			"("
			"op_id INT64 NOT NULL,"
			"sub_id TEXT NOT NULL,"
			"responded INTEGER NOT NULL DEFAULT 0,"
			"PRIMARY KEY (op_id, sub_id)"
			");",
			NULL, NULL, NULL);
	if(rc){
		goto fail;
	}

	rc = sqlite3_prepare_v3(ms->db,
			"INSERT OR REPLACE INTO dap_ops "
			"(op_id, publisher_id, op_type, timestamp, topic_filters, purpose_filters, client_filters) "
			"VALUES(?,?,?,?,?,?,?)",
			-1, SQLITE_PREPARE_PERSISTENT,
			&ms->dap_op_add_stmt, NULL);
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
			"UPDATE dap_tracked_op_subs SET responded=1 WHERE op_id=? AND sub_id=?",
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
			){

		return step_and_reset(ms, stmt);
	}
	sqlite3_reset(stmt);
	return MOSQ_ERR_UNKNOWN;
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

	if(sqlite3_bind_int64(stmt, 1, (int64_t)ed->data.op_id) == SQLITE_OK
			&& bind_text(stmt, 2, ed->subscriber_id) == SQLITE_OK
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

	/* Keep the requester row for late responses; drop the per-subscriber rows. */
	if(sqlite3_bind_int64(ms->dap_tracked_op_settle_stmt, 1, (int64_t)ed->data.op_id) != SQLITE_OK){
		sqlite3_reset(ms->dap_tracked_op_settle_stmt);
		return MOSQ_ERR_UNKNOWN;
	}
	rc = step_and_reset(ms, ms->dap_tracked_op_settle_stmt);
	if(rc){
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
			"SELECT op_id, publisher_id, op_type, timestamp, topic_filters, purpose_filters, client_filters "
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


/* Load an op's expected subscribers (copied; sqlite reuses column text). */
static int dap_tracked_op_load_subs(struct mosquitto_sqlite *ms, struct mosquitto_dap_tracked_op *op,
		char ***subs_out, bool **responded_out)
{
	sqlite3_stmt *stmt;
	char **subs = NULL;
	bool *responded = NULL;
	size_t n = 0;
	int rc;

	*subs_out = NULL;
	*responded_out = NULL;

	rc = sqlite3_prepare_v2(ms->db,
			"SELECT sub_id, responded FROM dap_tracked_op_subs WHERE op_id=? ORDER BY rowid",
			-1, &stmt, NULL);
	if(rc != SQLITE_OK){
		return MOSQ_ERR_UNKNOWN;
	}
	sqlite3_bind_int64(stmt, 1, (int64_t)op->op_id);

	rc = MOSQ_ERR_SUCCESS;
	while(sqlite3_step(stmt) == SQLITE_ROW){
		char **new_subs = realloc(subs, (n+1)*sizeof(char *));
		bool *new_responded = new_subs ? realloc(responded, (n+1)*sizeof(bool)) : NULL;
		if(new_subs){
			subs = new_subs;
		}
		if(new_responded){
			responded = new_responded;
		}
		if(!new_subs || !new_responded){
			rc = MOSQ_ERR_NOMEM;
			break;
		}
		subs[n] = strdup((const char *)sqlite3_column_text(stmt, 0));
		if(!subs[n]){
			rc = MOSQ_ERR_NOMEM;
			break;
		}
		responded[n] = sqlite3_column_int(stmt, 1) != 0;
		n++;
	}
	sqlite3_finalize(stmt);

	*subs_out = subs;
	*responded_out = responded;
	op->expected_subs = (const char *const *)subs;
	op->responded = responded;
	op->num_expected = n;
	return rc;
}


static int dap_tracked_op_restore(struct mosquitto_sqlite *ms)
{
	sqlite3_stmt *stmt;
	struct mosquitto_dap_tracked_op op;
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
		char **subs = NULL;
		bool *responded = NULL;

		memset(&op, 0, sizeof(op));
		op.op_id = (uint64_t)sqlite3_column_int64(stmt, 0);
		op.publisher_id = (const char *)sqlite3_column_text(stmt, 1);
		op.deadline = (time_t)sqlite3_column_int64(stmt, 2);
		op.settled = sqlite3_column_int(stmt, 3) != 0;

		rc = MOSQ_ERR_SUCCESS;
		if(!op.settled){
			rc = dap_tracked_op_load_subs(ms, &op, &subs, &responded);
		}
		if(rc == MOSQ_ERR_SUCCESS && mosquitto_persist_dap_tracked_op_add(&op) == MOSQ_ERR_SUCCESS){
			count++;
		}else{
			failed++;
		}

		for(size_t i=0; i<op.num_expected; i++){
			free(subs[i]);
		}
		free(subs);
		free(responded);
	}
	sqlite3_finalize(stmt);

	mosquitto_log_printf(MOSQ_LOG_INFO, "sqlite: Restored %ld DAP tracked operations (%ld failed)", count, failed);
	return MOSQ_ERR_SUCCESS;
}


int persist_sqlite__dap_restore(struct mosquitto_sqlite *ms)
{
	if(dap_op_restore(ms)){
		return MOSQ_ERR_UNKNOWN;
	}
	return dap_tracked_op_restore(ms);
}
