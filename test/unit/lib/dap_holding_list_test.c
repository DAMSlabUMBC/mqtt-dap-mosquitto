/* Standalone isolation test for the per-subscription holding list
 * (lib/dap_holding_list.c).
 *
 * Uses dummy (never-dereferenced) stamped-message pointers and checks the holding
 * lifecycle, FIFO ordering across add/flush, subscription independence, that flush
 * clears the holding state, that a double start is harmless, and that the pending
 * re-verify mid is recorded. Build and run on its own (CUnit is not required here):
 *
 *   cc -I../../.. -I../../../lib -I../../../include -I../../../libcommon \
 *      -I../../../src -I../../../common -I../../../deps -I/opt/homebrew/include \
 *      dap_holding_list_test.c ../../../lib/dap_holding_list.c \
 *      ../../../libcommon/memory_common.c -o dap_holding_list_test && ./dap_holding_list_test
 */

#include <assert.h>
#include <stdio.h>

#include "dap_holding_list.h"

/* Distinct, non-NULL stand-ins for real stamped messages. Never dereferenced. */
static char msg_slots[32];
#define MSG(i) ((struct dap_stamped_msg *)&msg_slots[(i)])

/* Count a flushed FIFO list and confirm it holds the expected messages in order. */
static size_t flushed_len(struct dap_held_msg *list)
{
    size_t n = 0;
    for(struct dap_held_msg *h = list; h; h = h->next) n++;
    return n;
}

static void test_holding_lifecycle(void)
{
    struct dap_holding_list h;
    assert(dap_holding_list_init(&h) == 0);

    assert(dap_holding_list_is_holding(&h, "sub/a") == false);
    assert(dap_holding_list_start_holding(&h, "sub/a", 0) == 0);
    assert(dap_holding_list_is_holding(&h, "sub/a") == true);

    /* Flushing with nothing held still clears the holding state. */
    struct dap_held_msg *flushed = dap_holding_list_flush(&h, "sub/a");
    assert(flushed_len(flushed) == 0);
    dap_holding_list_free_held(flushed);
    assert(dap_holding_list_is_holding(&h, "sub/a") == false);

    dap_holding_list_destroy(&h);
    printf("ok - holding state goes on with start and off with flush\n");
}

static void test_pending_id_recorded(void)
{
    struct dap_holding_list h;
    dap_holding_list_init(&h);

    /* A subscription that is not holding reports a pending id of 0. */
    assert(dap_holding_list_pending_id(&h, "sub/a") == 0);

    /* start_holding records the bumped message's cmsg_id for the re-verify candidate. */
    assert(dap_holding_list_start_holding(&h, "sub/a", 0x10000CAFEULL) == 0);
    assert(dap_holding_list_pending_id(&h, "sub/a") == 0x10000CAFEULL);

    dap_holding_list_destroy(&h);
    printf("ok - start_holding records the pending re-verify cmsg_id\n");
}

static void test_add_flush_ordering(void)
{
    struct dap_holding_list h;
    dap_holding_list_init(&h);

    dap_holding_list_start_holding(&h, "sub/a", 0);
    assert(dap_holding_list_add_held(&h, "sub/a", MSG(0)) == 0);
    assert(dap_holding_list_add_held(&h, "sub/a", MSG(1)) == 0);
    assert(dap_holding_list_add_held(&h, "sub/a", MSG(2)) == 0);

    /* Flush returns the held messages front-first, in arrival order. */
    struct dap_held_msg *flushed = dap_holding_list_flush(&h, "sub/a");
    assert(flushed_len(flushed) == 3);
    assert(flushed->msg == MSG(0));
    assert(flushed->next->msg == MSG(1));
    assert(flushed->next->next->msg == MSG(2));
    dap_holding_list_free_held(flushed);

    assert(dap_holding_list_is_holding(&h, "sub/a") == false);

    dap_holding_list_destroy(&h);
    printf("ok - add then flush preserves arrival order\n");
}

static void test_add_requires_holding(void)
{
    struct dap_holding_list h;
    dap_holding_list_init(&h);

    /* Adding to a subscription that is not holding is rejected. */
    assert(dap_holding_list_add_held(&h, "sub/a", MSG(0)) != 0);
    assert(dap_holding_list_is_holding(&h, "sub/a") == false);

    dap_holding_list_destroy(&h);
    printf("ok - add_held is rejected unless the subscription is holding\n");
}

static void test_subscriptions_independent(void)
{
    struct dap_holding_list h;
    dap_holding_list_init(&h);

    dap_holding_list_start_holding(&h, "sub/a", 0);
    dap_holding_list_start_holding(&h, "sub/b", 0);
    dap_holding_list_add_held(&h, "sub/a", MSG(0));
    dap_holding_list_add_held(&h, "sub/b", MSG(1));
    dap_holding_list_add_held(&h, "sub/a", MSG(2));

    /* Flushing one subscription leaves the other holding and untouched. */
    struct dap_held_msg *fa = dap_holding_list_flush(&h, "sub/a");
    assert(flushed_len(fa) == 2);
    assert(fa->msg == MSG(0) && fa->next->msg == MSG(2));
    dap_holding_list_free_held(fa);

    assert(dap_holding_list_is_holding(&h, "sub/a") == false);
    assert(dap_holding_list_is_holding(&h, "sub/b") == true);

    struct dap_held_msg *fb = dap_holding_list_flush(&h, "sub/b");
    assert(flushed_len(fb) == 1);
    assert(fb->msg == MSG(1));
    dap_holding_list_free_held(fb);

    dap_holding_list_destroy(&h);
    printf("ok - separate subscriptions hold and flush independently\n");
}

static void test_flush_clears_state(void)
{
    struct dap_holding_list h;
    dap_holding_list_init(&h);

    dap_holding_list_start_holding(&h, "sub/a", 0);
    dap_holding_list_add_held(&h, "sub/a", MSG(0));

    struct dap_held_msg *first = dap_holding_list_flush(&h, "sub/a");
    assert(flushed_len(first) == 1);
    dap_holding_list_free_held(first);

    /* A second flush with no active hold returns nothing. */
    assert(dap_holding_list_is_holding(&h, "sub/a") == false);
    struct dap_held_msg *again = dap_holding_list_flush(&h, "sub/a");
    assert(again == NULL);

    /* And add_held is rejected now that the hold has been flushed. */
    assert(dap_holding_list_add_held(&h, "sub/a", MSG(1)) != 0);

    dap_holding_list_destroy(&h);
    printf("ok - flush clears the holding state for the subscription\n");
}

static void test_double_start_is_harmless(void)
{
    struct dap_holding_list h;
    dap_holding_list_init(&h);

    dap_holding_list_start_holding(&h, "sub/a", 0xCAFE);
    dap_holding_list_add_held(&h, "sub/a", MSG(0));

    /* Starting again while already holding keeps the messages already held and the
     * original pending mid (a later bump does not displace the first candidate). */
    assert(dap_holding_list_start_holding(&h, "sub/a", 0x9999) == 0);
    assert(dap_holding_list_is_holding(&h, "sub/a") == true);
    assert(dap_holding_list_pending_id(&h, "sub/a") == 0xCAFE);
    dap_holding_list_add_held(&h, "sub/a", MSG(1));

    struct dap_held_msg *flushed = dap_holding_list_flush(&h, "sub/a");
    assert(flushed_len(flushed) == 2);
    assert(flushed->msg == MSG(0) && flushed->next->msg == MSG(1));
    dap_holding_list_free_held(flushed);

    dap_holding_list_destroy(&h);
    printf("ok - starting an already-holding subscription keeps its held messages and pending mid\n");
}

static void test_destroy_cleans_everything(void)
{
    struct dap_holding_list h;
    dap_holding_list_init(&h);

    dap_holding_list_start_holding(&h, "sub/a", 0);
    dap_holding_list_start_holding(&h, "sub/b", 0);
    dap_holding_list_add_held(&h, "sub/a", MSG(0));
    dap_holding_list_add_held(&h, "sub/a", MSG(1));
    dap_holding_list_add_held(&h, "sub/b", MSG(2));

    dap_holding_list_destroy(&h);

    /* Destroy leaves the structure empty and reusable. */
    assert(dap_holding_list_is_holding(&h, "sub/a") == false);
    assert(dap_holding_list_start_holding(&h, "sub/c", 0) == 0);
    assert(dap_holding_list_is_holding(&h, "sub/c") == true);
    dap_holding_list_destroy(&h);

    printf("ok - destroy frees all subscriptions and wrappers and stays reusable\n");
}

int main(void)
{
    test_holding_lifecycle();
    test_pending_id_recorded();
    test_add_flush_ordering();
    test_add_requires_holding();
    test_subscriptions_independent();
    test_flush_clears_state();
    test_double_start_is_harmless();
    test_destroy_cleans_everything();
    printf("\nAll dap_holding_list tests passed.\n");
    return 0;
}
