/* Standalone isolation test for the per-subscription topic queues
 * (lib/dap_subscription_queues.c).
 *
 * Uses dummy (never-dereferenced) stored-message pointers and checks FIFO ordering per
 * topic, topic independence, stamping, push-to-front and cleanup. Build and run
 * on its own (CUnit is not required here):
 *
 *   cc -I../../.. -I../../../lib -I../../../include -I../../../libcommon \
 *      -I../../../src -I../../../common -I../../../deps -I/opt/homebrew/include \
 *      dap_subscription_queues_test.c ../../../lib/dap_subscription_queues.c \
 *      ../../../libcommon/memory_common.c -o dap_subscription_queues_test && ./dap_subscription_queues_test
 */

#include <assert.h>
#include <stdio.h>

#include "dap_subscription_queues.h"

/* Distinct, non-NULL stand-ins for real stored messages. Contents are never read. */
static char msg_slots[16];
#define MSG(i) ((struct mosquitto__base_msg *)&msg_slots[(i)])

static void test_empty_queues(void)
{
    struct dap_subscription_queues q;
    assert(dap_subscription_queues_init(&q) == 0);
    assert(dap_subscription_queues_total_size(&q) == 0);
    assert(dap_subscription_queues_topic_size(&q, "t/a") == 0);
    assert(dap_subscription_queues_peek_front(&q, "t/a") == NULL);
    assert(dap_subscription_queues_dequeue_front(&q, "t/a") == NULL);
    dap_subscription_queues_destroy(&q);
    printf("ok - empty queues report nothing\n");
}

static void test_fifo_within_topic(void)
{
    struct dap_subscription_queues q;
    dap_subscription_queues_init(&q);

    assert(dap_subscription_queues_enqueue(&q, "t/a", MSG(0), 0, 0, 0, NULL, 0, 0) == 0);
    assert(dap_subscription_queues_enqueue(&q, "t/a", MSG(1), 0, 0, 0, NULL, 0, 0) == 0);
    assert(dap_subscription_queues_enqueue(&q, "t/a", MSG(2), 0, 0, 0, NULL, 0, 0) == 0);
    assert(dap_subscription_queues_topic_size(&q, "t/a") == 3);
    assert(dap_subscription_queues_total_size(&q) == 3);

    /* Dequeue returns in insertion order. */
    struct dap_stamped_msg *m;
    m = dap_subscription_queues_dequeue_front(&q, "t/a"); assert(m->base_msg == MSG(0)); dap_stamped_msg_free(m);
    m = dap_subscription_queues_dequeue_front(&q, "t/a"); assert(m->base_msg == MSG(1)); dap_stamped_msg_free(m);
    m = dap_subscription_queues_dequeue_front(&q, "t/a"); assert(m->base_msg == MSG(2)); dap_stamped_msg_free(m);
    assert(dap_subscription_queues_dequeue_front(&q, "t/a") == NULL);
    assert(dap_subscription_queues_total_size(&q) == 0);

    dap_subscription_queues_destroy(&q);
    printf("ok - a topic queue is FIFO\n");
}

static void test_topics_are_independent(void)
{
    struct dap_subscription_queues q;
    dap_subscription_queues_init(&q);

    dap_subscription_queues_enqueue(&q, "t/a", MSG(0), 0, 0, 0, NULL, 0, 0);
    dap_subscription_queues_enqueue(&q, "t/b", MSG(1), 0, 0, 0, NULL, 0, 0);
    dap_subscription_queues_enqueue(&q, "t/a", MSG(2), 0, 0, 0, NULL, 0, 0);

    assert(dap_subscription_queues_topic_size(&q, "t/a") == 2);
    assert(dap_subscription_queues_topic_size(&q, "t/b") == 1);
    assert(dap_subscription_queues_total_size(&q) == 3);

    /* Draining t/a leaves t/b untouched. */
    struct dap_stamped_msg *m;
    m = dap_subscription_queues_dequeue_front(&q, "t/a"); assert(m->base_msg == MSG(0)); dap_stamped_msg_free(m);
    assert(dap_subscription_queues_topic_size(&q, "t/a") == 1);
    assert(dap_subscription_queues_topic_size(&q, "t/b") == 1);
    assert(dap_subscription_queues_peek_front(&q, "t/b")->base_msg == MSG(1));

    dap_subscription_queues_destroy(&q);
    printf("ok - separate topics keep separate FIFO queues\n");
}

static void test_stamping_fields_stored(void)
{
    struct dap_subscription_queues q;
    dap_subscription_queues_init(&q);

    uint64_t ids[] = {10, 20, 30};
    assert(dap_subscription_queues_enqueue(&q, "t/a", MSG(5), 0x1BEEF00000001ULL, 5, 7, ids, 3, 4242) == 0);

    /* peek is borrowed - do not free. */
    struct dap_stamped_msg *m = dap_subscription_queues_peek_front(&q, "t/a");
    assert(m != NULL);
    assert(m->base_msg == MSG(5));
    assert(m->cmsg_id == 0x1BEEF00000001ULL);
    assert(m->mp_version == 5);
    assert(m->sp_version == 7);
    assert(m->enqueue_time == 4242);
    assert(m->num_applied_op_ids == 3);
    assert(m->applied_op_ids[0] == 10 && m->applied_op_ids[1] == 20 && m->applied_op_ids[2] == 30);

    /* The op-id array is copied, not aliased to the caller's storage. */
    assert(m->applied_op_ids != ids);

    /* A different cmsg_id on a different topic is recorded independently; no op ids. */
    dap_subscription_queues_enqueue(&q, "t/b", MSG(6), 7, 1, 1, NULL, 0, 9);
    struct dap_stamped_msg *m2 = dap_subscription_queues_peek_front(&q, "t/b");
    assert(m2->cmsg_id == 7);
    assert(m2->num_applied_op_ids == 0 && m2->applied_op_ids == NULL);

    dap_subscription_queues_destroy(&q);
    printf("ok - stamping fields (incl. cmsg_id) are stored and the op-id list is copied\n");
}

static void test_find_and_remove(void)
{
    struct dap_subscription_queues q;
    dap_subscription_queues_init(&q);

    dap_subscription_queues_enqueue(&q, "t/a", MSG(0), 1, 0, 0, NULL, 0, 0);
    dap_subscription_queues_enqueue(&q, "t/a", MSG(1), 2, 0, 0, NULL, 0, 0);
    dap_subscription_queues_enqueue(&q, "t/a", MSG(2), 3, 0, 0, NULL, 0, 0);

    /* Found anywhere in the queue, but only when both base_msg and cmsg_id match. */
    struct dap_stamped_msg *m = dap_subscription_queues_find(&q, "t/a", MSG(1), 2);
    assert(m != NULL && m->cmsg_id == 2);
    assert(dap_subscription_queues_find(&q, "t/a", MSG(0), 2) == NULL);
    assert(dap_subscription_queues_find(&q, "t/b", MSG(1), 2) == NULL);

    dap_subscription_queues_remove(&q, "t/a", m);
    dap_stamped_msg_free(m);
    assert(dap_subscription_queues_topic_size(&q, "t/a") == 2);
    assert(dap_subscription_queues_total_size(&q) == 2);

    /* The others keep their order. */
    m = dap_subscription_queues_dequeue_front(&q, "t/a");
    assert(m->cmsg_id == 1);
    dap_stamped_msg_free(m);
    m = dap_subscription_queues_dequeue_front(&q, "t/a");
    assert(m->cmsg_id == 3);
    dap_stamped_msg_free(m);

    dap_subscription_queues_destroy(&q);
    printf("ok - a stamp is found and removed by base_msg and cmsg_id anywhere in its queue\n");
}

static void test_push_to_front(void)
{
    struct dap_subscription_queues q;
    dap_subscription_queues_init(&q);

    dap_subscription_queues_enqueue(&q, "t/a", MSG(0), 0, 0, 0, NULL, 0, 0);
    dap_subscription_queues_enqueue(&q, "t/a", MSG(1), 0, 0, 0, NULL, 0, 0);

    /* Take the front off, then put it back at the front (holding-list pattern). */
    struct dap_stamped_msg *held = dap_subscription_queues_dequeue_front(&q, "t/a");
    assert(held->base_msg == MSG(0));
    assert(dap_subscription_queues_topic_size(&q, "t/a") == 1);

    assert(dap_subscription_queues_push_front(&q, "t/a", held) == 0);
    assert(dap_subscription_queues_topic_size(&q, "t/a") == 2);

    /* Order is restored: the pushed-back message comes out first again. */
    struct dap_stamped_msg *m;
    m = dap_subscription_queues_dequeue_front(&q, "t/a"); assert(m->base_msg == MSG(0)); dap_stamped_msg_free(m);
    m = dap_subscription_queues_dequeue_front(&q, "t/a"); assert(m->base_msg == MSG(1)); dap_stamped_msg_free(m);

    dap_subscription_queues_destroy(&q);
    printf("ok - push_front places a message ahead of the rest\n");
}

static void test_destroy_cleans_everything(void)
{
    struct dap_subscription_queues q;
    dap_subscription_queues_init(&q);

    uint64_t ids[] = {1, 2};
    dap_subscription_queues_enqueue(&q, "t/a", MSG(0), 0, 1, 1, ids, 2, 1);
    dap_subscription_queues_enqueue(&q, "t/a", MSG(1), 0, 1, 1, NULL, 0, 2);
    dap_subscription_queues_enqueue(&q, "t/b", MSG(2), 0, 1, 1, ids, 2, 3);

    dap_subscription_queues_destroy(&q);

    /* Destroy leaves the structure empty and reusable. */
    assert(dap_subscription_queues_total_size(&q) == 0);
    assert(dap_subscription_queues_peek_front(&q, "t/a") == NULL);
    assert(dap_subscription_queues_enqueue(&q, "t/c", MSG(3), 0, 0, 0, NULL, 0, 0) == 0);
    assert(dap_subscription_queues_total_size(&q) == 1);
    dap_subscription_queues_destroy(&q);

    printf("ok - destroy frees all topics and messages and stays reusable\n");
}

int main(void)
{
    test_empty_queues();
    test_fifo_within_topic();
    test_topics_are_independent();
    test_stamping_fields_stored();
    test_find_and_remove();
    test_push_to_front();
    test_destroy_cleans_everything();
    printf("\nAll dap_subscription_queues tests passed.\n");
    return 0;
}
