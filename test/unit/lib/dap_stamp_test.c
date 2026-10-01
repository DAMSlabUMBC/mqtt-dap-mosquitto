/* Integration check for the Case 4 step 1-2 stamping helper (lib/dap_stamp.c).
 *
 * This is the unit-level stand-in for "a publish results in a stamped message in
 * the topic queue": it drives the exact MP-version-lookup + enqueue path the broker
 * hook in subs__process calls, using the real mp registry and the real
 * subscription queues, with dummy (never-
 * dereferenced) stored-message pointers. The SP version is supplied by the caller
 * (it now lives on the subscription leaf), so the tests pass it directly. The full
 * broker is not exercised here. Build and run on its own:
 *
 *   cc -I../../.. -I../../../lib -I../../../include -I../../../libcommon \
 *      -I../../../src -I../../../common -I../../../deps -I/opt/homebrew/include \
 *      dap_stamp_test.c ../../../lib/dap_stamp.c ../../../lib/dap_subscription_queues.c \
 *      ../../../lib/mp_registry.c \
 *      ../../../libcommon/memory_common.c -o dap_stamp_test && ./dap_stamp_test
 */

#include <assert.h>
#include <stdio.h>

#include "dap_stamp.h"
#include "dap_subscription_queues.h"
#include "mp_registry.h"

/* Distinct, non-NULL stand-ins for real stored messages. Contents are never read. */
static char msg_slots[16];
#define MSG(i) ((struct mosquitto__base_msg *)&msg_slots[(i)])

/* A publish matched to a subscription lands in that subscription's topic queue,
 * stamped with the MP version registered for it, the SP version passed in, and the
 * cmsg_id passed in. Pending operations are applied at the send-path gate, so no op
 * ids are stamped here. */
static void test_stamp_enqueues_with_current_versions(void)
{
    mp_registry_init();
    struct dap_subscription_queues q;
    assert(dap_subscription_queues_init(&q) == 0);

    /* Publisher's MP for the topic is now at version 2 (registered, then updated). */
    mp__register_topic("pub/x", "sensors/temp", "ads/targeted");
    mp__register_topic("pub/x", "sensors/temp", "billing/electricity");

    /* Subscriber's SP (from the leaf) is at version 1; the client message is cmsg_id 0xABCD. */
    assert(dap_stamp_and_enqueue(&q, "pub/x", "sensors/temp", 0xABCD, 1, MSG(0), 4242) == 0);

    /* One stamped message, queued under its topic, carrying the versions and cmsg_id. */
    assert(dap_subscription_queues_total_size(&q) == 1);
    struct dap_stamped_msg *m = dap_subscription_queues_peek_front(&q, "sensors/temp");
    assert(m != NULL);
    assert(m->base_msg == MSG(0));
    assert(m->cmsg_id == 0xABCD);
    assert(m->mp_version == 2);
    assert(m->sp_version == 1);
    assert(m->enqueue_time == 4242);
    /* No pending op applied. */
    assert(m->num_applied_op_ids == 0);
    assert(m->applied_op_ids == NULL);

    dap_subscription_queues_destroy(&q);
    mp_registry_cleanup();
    printf("ok - stamp enqueues the message with the current MP version, given SP version and cmsg_id\n");
}

/* With no MP registered and SP version 0, both versions stamp as 0 and the message
 * still queues. */
static void test_stamp_with_unregistered_versions(void)
{
    mp_registry_init();
    struct dap_subscription_queues q;
    dap_subscription_queues_init(&q);

    assert(dap_stamp_and_enqueue(&q, "pub/x", "sensors/temp", 0, 0, MSG(1), 7) == 0);
    struct dap_stamped_msg *m = dap_subscription_queues_peek_front(&q, "sensors/temp");
    assert(m != NULL);
    assert(m->mp_version == 0);
    assert(m->sp_version == 0);

    dap_subscription_queues_destroy(&q);
    mp_registry_cleanup();
    printf("ok - unregistered MP and SP version 0 still enqueue\n");
}

/* Repeated publishes to the same topic accumulate in FIFO order. */
static void test_stamp_accumulates_fifo(void)
{
    mp_registry_init();
    struct dap_subscription_queues q;
    dap_subscription_queues_init(&q);

    assert(dap_stamp_and_enqueue(&q, "pub/x", "t/a", 0, 0, MSG(0), 1) == 0);
    assert(dap_stamp_and_enqueue(&q, "pub/x", "t/a", 0, 0, MSG(1), 2) == 0);
    assert(dap_subscription_queues_topic_size(&q, "t/a") == 2);

    struct dap_stamped_msg *m;
    m = dap_subscription_queues_dequeue_front(&q, "t/a"); assert(m->base_msg == MSG(0)); dap_stamped_msg_free(m);
    m = dap_subscription_queues_dequeue_front(&q, "t/a"); assert(m->base_msg == MSG(1)); dap_stamped_msg_free(m);

    dap_subscription_queues_destroy(&q);
    mp_registry_cleanup();
    printf("ok - repeated stamps accumulate FIFO under the topic\n");
}

/* A NULL queue is rejected rather than crashing. */
static void test_stamp_bad_args(void)
{
    assert(dap_stamp_and_enqueue(NULL, "pub/x", "t/a", 0, 0, MSG(0), 1) != 0);
    printf("ok - stamp rejects a NULL queue\n");
}

int main(void)
{
    test_stamp_enqueues_with_current_versions();
    test_stamp_with_unregistered_versions();
    test_stamp_accumulates_fifo();
    test_stamp_bad_args();
    printf("\nAll dap_stamp tests passed.\n");
    return 0;
}
