/* Standalone isolation test for the output-side priority queue
 * (src/dap/output_priority.h: db__queued_insert_prioritized), MQTT-DAP paper 5.2(ii).
 *
 * Verifies that op/PBMR control-plane messages are ordered ahead of data on a
 * client's outgoing queue (msgs_out.queued) while FIFO order is preserved within
 * each class. Uses stack-allocated client_msg / base_msg nodes; no broker runtime,
 * no allocator, no CUnit. The real header-defined helper is exercised directly.
 *
 * Build and run on its own:
 *   cc -DWITH_BROKER -I../../.. -I../../../src -I../../../lib -I../../../include \
 *      -I../../../common -I../../../libcommon -I../../../deps -std=gnu99 \
 *      queued_priority_test.c ../../../lib/dap/dap_topics.c -o queued_priority_test \
 *      && ./queued_priority_test
 */

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "mosquitto_broker_internal.h"
#include "dap/output_priority.h"

/* A data topic (never matches dap_is_op_system_topic) and several op/PBMR topics
 * spanning the control-plane namespaces the classifier recognises. */
#define DATA_TOPIC   "sensors/room1/temp"
#define OP_ORS       MOSQ_DAP_TOPIC_ORS "/sub1"   /* OP_REQ/sub1   - op request   */
#define OP_ONP       MOSQ_DAP_TOPIC_ONP "/pub1"   /* OP_NOTIF/pub1 - op notify    */
#define OP_OSYS      MOSQ_DAP_TOPIC_OSYS          /* $OP_SYS       - status bus   */
#define OP_PBMR      "$DAP/MP_reg/pub1"           /* $DAP/...      - PBMR control */

/* Node pool: each base_msg carries a topic, each client_msg a tag in data.mid. */
static struct mosquitto__base_msg   g_base[16];
static struct mosquitto__client_msg g_cmsg[16];
static int g_used;

static void reset_pool(void)
{
	memset(g_base, 0, sizeof(g_base));
	memset(g_cmsg, 0, sizeof(g_cmsg));
	g_used = 0;
}

/* Build a client_msg tagged with `id`, on `topic`, and insert it via the helper. */
static void insert(struct mosquitto_msg_data *q, uint16_t id, char *topic)
{
	struct mosquitto__base_msg   *bm = &g_base[g_used];
	struct mosquitto__client_msg *cm = &g_cmsg[g_used];
	g_used++;

	bm->data.topic = topic;
	cm->base_msg   = bm;
	cm->data.mid   = id;
	cm->prev = NULL;
	cm->next = NULL;

	db__queued_insert_prioritized(q, cm, topic);
}

/* Assert the queued list drains as exactly `expected` (by tag), in order. */
static void assert_order(struct mosquitto_msg_data *q, const uint16_t *expected, size_t n)
{
	struct mosquitto__client_msg *cur;
	size_t i = 0;
	DL_FOREACH(q->queued, cur){
		assert(i < n && "queue longer than expected");
		assert(cur->data.mid == expected[i] && "wrong message at this position");
		i++;
	}
	assert(i == n && "queue shorter than expected");
}

/* The headline case: interleaved (data, op, data, op) drains as (op, op, data, data),
 * preserving FIFO within each class. */
static void test_interleaved_op_ahead_of_data(void)
{
	struct mosquitto_msg_data q;
	memset(&q, 0, sizeof(q));
	reset_pool();

	insert(&q, 10, DATA_TOPIC); /* data d0 */
	insert(&q, 20, OP_ORS);     /* op   o0 */
	insert(&q, 11, DATA_TOPIC); /* data d1 */
	insert(&q, 21, OP_ONP);     /* op   o1 */

	const uint16_t expected[] = {20, 21, 10, 11}; /* o0, o1, d0, d1 */
	assert_order(&q, expected, 4);
	printf("  ok: interleaved (data,op,data,op) -> (op,op,data,data)\n");
}

/* All-data input keeps plain FIFO. */
static void test_all_data_fifo(void)
{
	struct mosquitto_msg_data q;
	memset(&q, 0, sizeof(q));
	reset_pool();

	insert(&q, 1, DATA_TOPIC);
	insert(&q, 2, DATA_TOPIC);
	insert(&q, 3, DATA_TOPIC);

	const uint16_t expected[] = {1, 2, 3};
	assert_order(&q, expected, 3);
	printf("  ok: all-data preserves FIFO\n");
}

/* All-op input keeps plain FIFO across mixed op/PBMR namespaces. */
static void test_all_op_fifo(void)
{
	struct mosquitto_msg_data q;
	memset(&q, 0, sizeof(q));
	reset_pool();

	insert(&q, 1, OP_ORS);
	insert(&q, 2, OP_OSYS);
	insert(&q, 3, OP_PBMR);
	insert(&q, 4, OP_ONP);

	const uint16_t expected[] = {1, 2, 3, 4};
	assert_order(&q, expected, 4);
	printf("  ok: all-op (incl. PBMR/$OP_SYS) preserves FIFO\n");
}

/* An op message arriving after a block of data jumps ahead of all data but stays
 * behind earlier ops. */
static void test_op_jumps_ahead_of_data_block(void)
{
	struct mosquitto_msg_data q;
	memset(&q, 0, sizeof(q));
	reset_pool();

	insert(&q, 20, OP_ORS);     /* op   o0 */
	insert(&q, 10, DATA_TOPIC); /* data d0 */
	insert(&q, 11, DATA_TOPIC); /* data d1 */
	insert(&q, 21, OP_PBMR);    /* op   o1 - must land after o0, before d0/d1 */

	const uint16_t expected[] = {20, 21, 10, 11};
	assert_order(&q, expected, 4);
	printf("  ok: late op jumps the data block, stays behind earlier op\n");
}

/* Op into an empty queue, then data after it. */
static void test_op_then_data(void)
{
	struct mosquitto_msg_data q;
	memset(&q, 0, sizeof(q));
	reset_pool();

	insert(&q, 20, OP_ONP);     /* op into empty queue (DL_PREPEND_ELEM NULL -> tail) */
	insert(&q, 10, DATA_TOPIC); /* data appends after op */

	const uint16_t expected[] = {20, 10};
	assert_order(&q, expected, 2);
	printf("  ok: op into empty queue, data appends behind it\n");
}

int main(void)
{
	printf("output_priority (paper 5.2(ii)) queued-insert ordering:\n");
	test_interleaved_op_ahead_of_data();
	test_all_data_fifo();
	test_all_op_fifo();
	test_op_jumps_ahead_of_data_block();
	test_op_then_data();
	printf("ALL PASS\n");
	return 0;
}
