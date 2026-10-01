/* Standalone isolation test for subscription relevance tracking in the data
 * relationship registry (lib/dr_registry.c).
 *
 * Seeds a set of recorded pub->sub deliveries (each with an SP at receipt time)
 * and checks dr__find_relevant_subscribers against the paper's four relevance
 * conditions. Build and run on its own (CUnit is not required here):
 *
 *   cc -I../../.. -I../../../lib -I../../../lib/dap -I../../../include -I../../../libcommon \
 *      -I../../../src -I../../../common -I../../../deps -I/opt/homebrew/include \
 *      dr_relevance_test.c ../../../lib/dap/dr_registry.c \
 *      ../../../libcommon/memory_common.c -o dr_relevance_test && ./dr_relevance_test

 */

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>

#include "dr_registry.h"

/* Wrap the filter args into the dap__op_property the API now takes. */
static struct dr_sublist *relevant(const char *pub_id, const char *topic_filters,
                                   const char *purpose_filters, const char *client_filters,
                                   time_t before, time_t after)
{
    struct dap__op_property props;
    memset(&props, 0, sizeof(props));
    props.op_topic_filters = (char *)topic_filters;
    props.op_purpose_filters = (char *)purpose_filters;
    props.op_client_filters = (char *)client_filters;
    props.op_before = before;
    props.op_after = after;
    return dr__find_relevant_subscribers(pub_id, &props);
}

static int sublist_count(struct dr_sublist *l)
{
    int n = 0;
    for(; l; l = l->next) n++;
    return n;
}

static bool sublist_has(struct dr_sublist *l, const char *sub_id)
{
    for(; l; l = l->next){
        if(!strcmp(l->sub_id, sub_id)) return true;
    }
    return false;
}

/* Recorded deliveries shared by the tests. The trailing argument is the receipt
 * time (DAP-Timestamp) for the flow, used by the OpBefore/OpAfter bounds test. */
static void seed(void)
{
    dr__record_recipient_with_sp("pubA", "sensors/temp", "subX", "billing", 1000);
    dr__record_recipient_with_sp("pubA", "sensors/humidity", "subY", "research", 2000);
    dr__record_recipient_with_sp("pubA", "sensors/temp", "subZ", "ads,research", 3000);
    dr__record_recipient_with_sp("pubA", "events/x", "subX", "billing", 4000); /* subX again, dedupe */
    dr__record_recipient_with_sp("pubA", "telemetry", "subW", NULL, 5000);     /* no SP recorded */
    dr__record_recipient_with_sp("pubB", "sensors/temp", "subX", "billing", 6000); /* other publisher */
}

static void test_all_recipients_when_no_filters(void)
{
    dr_registry_init();
    seed();

    /* "*" everywhere: every distinct subscriber that received from pubA, once. */
    struct dr_sublist *r = relevant("pubA", "*", "*", "*", 0, 0);
    assert(sublist_count(r) == 4);
    assert(sublist_has(r, "subX") && sublist_has(r, "subY")
        && sublist_has(r, "subZ") && sublist_has(r, "subW"));
    dr__free_sublist(r);

    dr_registry_cleanup();
    printf("ok - with no filters, every distinct recipient is returned once\n");
}

static void test_publisher_isolation(void)
{
    dr_registry_init();
    seed();

    /* Only subscribers that received from pubB. */
    struct dr_sublist *r = relevant("pubB", "*", "*", "*", 0, 0);
    assert(sublist_count(r) == 1);
    assert(sublist_has(r, "subX"));
    dr__free_sublist(r);

    /* A publisher nobody received from yields nothing. */
    r = relevant("pubC", "*", "*", "*", 0, 0);
    assert(r == NULL);

    dr_registry_cleanup();
    printf("ok - relevance is scoped to the requesting publisher\n");
}

static void test_topic_filters(void)
{
    dr_registry_init();
    seed();

    /* Single topic filter: only recipients on sensors/temp. */
    struct dr_sublist *r = relevant("pubA", "sensors/temp", "*", "*", 0, 0);
    assert(sublist_count(r) == 2);
    assert(sublist_has(r, "subX") && sublist_has(r, "subZ"));
    dr__free_sublist(r);

    /* A topic-filter list matches deliveries on any listed topic. */
    r = relevant("pubA", "sensors/temp,sensors/humidity", "*", "*", 0, 0);
    assert(sublist_count(r) == 3);
    assert(sublist_has(r, "subX") && sublist_has(r, "subY") && sublist_has(r, "subZ"));
    dr__free_sublist(r);

    /* A topic nobody received on yields nothing. */
    r = relevant("pubA", "sensors/pressure", "*", "*", 0, 0);
    assert(r == NULL);

    dr_registry_cleanup();
    printf("ok - topic filters select by receipt topic, any element may match\n");
}

static void test_purpose_filters(void)
{
    dr_registry_init();
    seed();

    /* SP at receipt time must share a purpose with the purpose filters. */
    struct dr_sublist *r = relevant("pubA", "*", "research", "*", 0, 0);
    /* subY (SP "research") and subZ (SP "ads,research"); not subX (billing). */
    assert(sublist_count(r) == 2);
    assert(sublist_has(r, "subY") && sublist_has(r, "subZ"));
    dr__free_sublist(r);

    /* A subscriber with no recorded SP can never satisfy a provided purpose filter. */
    r = relevant("pubA", "*", "billing", "*", 0, 0);
    assert(sublist_count(r) == 1);     /* only subX */
    assert(sublist_has(r, "subX"));
    assert(!sublist_has(r, "subW"));   /* subW had a NULL SP */
    dr__free_sublist(r);

    /* No SP described by these filters. */
    r = relevant("pubA", "*", "nonexistent", "*", 0, 0);
    assert(r == NULL);

    dr_registry_cleanup();
    printf("ok - purpose filters match against the SP recorded at receipt time\n");
}

static void test_client_filters(void)
{
    dr_registry_init();
    seed();

    struct dr_sublist *r = relevant("pubA", "*", "*", "subX,subZ", 0, 0);
    assert(sublist_count(r) == 2);
    assert(sublist_has(r, "subX") && sublist_has(r, "subZ"));
    assert(!sublist_has(r, "subY"));
    dr__free_sublist(r);

    dr_registry_cleanup();
    printf("ok - client filters restrict the result to listed subscriber ids\n");
}

static void test_all_four_conditions_combined(void)
{
    dr_registry_init();
    seed();

    /* Topic sensors/temp -> {subX, subZ}; purpose research -> only subZ qualifies;
     * client list {subZ, subX} keeps subZ. */
    struct dr_sublist *r = relevant("pubA", "sensors/temp", "research", "subZ,subX", 0, 0);
    assert(sublist_count(r) == 1);
    assert(sublist_has(r, "subZ"));
    dr__free_sublist(r);

    dr_registry_cleanup();
    printf("ok - all four conditions are ANDed together\n");
}

static void test_time_bounds(void)
{
    dr_registry_init();
    seed();

    /* DAP-OpAfter only (after=3500, before unbounded): recipients with a flow at or
     * after 3500. subX via events/x@4000 and subW@5000 qualify; subY@2000, subZ@3000
     * and subX@1000 do not (but subX is still kept once, via its 4000 flow). */
    struct dr_sublist *r = relevant("pubA", "*", "*", "*", 0, 3500);
    assert(sublist_count(r) == 2);
    assert(sublist_has(r, "subX") && sublist_has(r, "subW"));
    dr__free_sublist(r);

    /* DAP-OpBefore only (before=3500): flows at or before 3500. subX@1000, subY@2000,
     * subZ@3000 qualify; subW@5000 does not. */
    r = relevant("pubA", "*", "*", "*", 3500, 0);
    assert(sublist_count(r) == 3);
    assert(sublist_has(r, "subX") && sublist_has(r, "subY") && sublist_has(r, "subZ"));
    assert(!sublist_has(r, "subW"));
    dr__free_sublist(r);

    /* Both bounds: a window [2500, 4500] keeps subZ@3000 and subX (via events/x@4000). */
    r = relevant("pubA", "*", "*", "*", 4500, 2500);
    assert(sublist_count(r) == 2);
    assert(sublist_has(r, "subZ") && sublist_has(r, "subX"));
    dr__free_sublist(r);

    dr_registry_cleanup();
    printf("ok - OpBefore/OpAfter bound recipients by receipt time (0 = unbounded)\n");
}

int main(void)
{
    test_all_recipients_when_no_filters();
    test_publisher_isolation();
    test_topic_filters();
    test_purpose_filters();
    test_client_filters();
    test_all_four_conditions_combined();
    test_time_bounds();
    printf("\nAll dr relevance tests passed.\n");
    return 0;
}
