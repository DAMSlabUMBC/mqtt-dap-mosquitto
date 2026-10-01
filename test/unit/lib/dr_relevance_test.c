/* Standalone isolation test for subscription relevance tracking in the data
 * relationship registry (lib/dr_registry.c).
 *
 * Seeds a set of recorded pub->sub deliveries (each with an SP at receipt time)
 * and checks dr__find_relevant_subscribers against the paper's four relevance
 * conditions. Build and run on its own (CUnit is not required here):
 *
 *   cc -I../../.. -I../../../lib -I../../../lib/dap -I../../../include -I../../../libcommon \
 *      -I../../../src -I../../../common -I../../../deps -I/opt/homebrew/include \
 *      dr_relevance_test.c ../../../lib/dap/dr_registry.c ../../../lib/dap/purpose_filters.c \
 *      ../../../libcommon/topic_common.c ../../../libcommon/memory_common.c \
 *      -o dr_relevance_test && ./dr_relevance_test

 */

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>

#include "dr_registry.h"
#include "purpose_filters.h"

/* Record a delivery to a subscription whose SP is the collection sp_filters. */
static void flow(const char *pub, const char *topic, const char *sub, const char *sp_filters, time_t t)
{
    char **sp = NULL;
    uint32_t n = 0;
    assert(purpose_set_expand(sp_filters, &sp, &n) == 0);
    const struct dr_sublist *changed;
    assert(dr__record_flow(pub, topic, sub, sp, n, t, &changed) == 0);
    purpose_set_free(sp, n);
}

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
    flow("pubA", "sensors/temp", "subX", "billing", 1000);
    flow("pubA", "sensors/humidity", "subY", "research", 2000);
    flow("pubA", "sensors/temp", "subZ", "ads|research", 3000);
    flow("pubA", "events/x", "subX", "billing", 4000); /* subX again, dedupe */
    flow("pubA", "telemetry", "subW", NULL, 5000);     /* no SP recorded */
    flow("pubB", "sensors/temp", "subX", "billing", 6000); /* other publisher */
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

    /* Topic filters are MQTT topic filters. */
    r = relevant("pubA", "sensors/#", "*", "*", 0, 0);
    assert(sublist_count(r) == 3);
    assert(sublist_has(r, "subX") && sublist_has(r, "subY") && sublist_has(r, "subZ"));
    dr__free_sublist(r);
    r = relevant("pubA", "+/x", "*", "*", 0, 0);
    assert(sublist_count(r) == 1 && sublist_has(r, "subX"));
    dr__free_sublist(r);

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

    /* DAP-OpPFs is a collection of purpose filters ('|' or ',', braces expanded). */
    r = relevant("pubA", "*", "{billing,ads}", "*", 0, 0);
    assert(sublist_count(r) == 2);
    assert(sublist_has(r, "subX") && sublist_has(r, "subZ"));
    dr__free_sublist(r);

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

/* Paper 6.1: relevance uses the SP in force at delivery, so a subscriber stays
 * relevant for data it received under an SP it has since replaced. */
static void test_delivery_time_sp(void)
{
    dr_registry_init();
    flow("pubA", "t/a", "subV", "maintenance", 1000);
    flow("pubA", "t/a", "subV", "maintenance", 1500);
    flow("pubA", "t/a", "subV", "quality", 2000); /* the SP changed */

    struct dr_sublist *r = relevant("pubA", "*", "maintenance", "*", 0, 0);
    assert(sublist_count(r) == 1 && sublist_has(r, "subV"));
    dr__free_sublist(r);
    r = relevant("pubA", "*", "quality", "*", 0, 0);
    assert(sublist_count(r) == 1 && sublist_has(r, "subV"));
    dr__free_sublist(r);

    /* Each (publisher, topic, subscriber, SP) is one flow, spanning its first and
     * last receipt. */
    int flows = 0;
    for(struct dr_entry *e = dr_head; e; e = e->next){
        for(struct dr_sublist *s = e->sub_list; s; s = s->next){
            flows++;
            if(!strcmp(s->sp, "maintenance")){
                assert(s->first_time == 1000 && s->last_time == 1500);
            }else{
                assert(!strcmp(s->sp, "quality"));
                assert(s->first_time == 2000 && s->last_time == 2000);
            }
        }
    }
    assert(flows == 2);

    /* The maintenance flow ran from 1000 to 1500, so a window inside it overlaps. */
    r = relevant("pubA", "*", "maintenance", "*", 1300, 1200);
    assert(sublist_count(r) == 1);
    dr__free_sublist(r);
    r = relevant("pubA", "*", "maintenance", "*", 0, 1600);
    assert(r == NULL);

    dr_registry_cleanup();
    printf("ok - a flow keeps the SP in force at delivery\n");
}

/* A flow reports a change when it is new or its receipt times move, so it can be
 * persisted; a restored flow is relevant like a recorded one. */
static void test_flow_changes_and_restore(void)
{
    char **sp = NULL;
    uint32_t n = 0;
    const struct dr_sublist *changed;

    dr_registry_init();
    assert(purpose_set_expand("qa", &sp, &n) == 0);
    assert(dr__record_flow("pubA", "t/a", "subA", sp, n, 100, &changed) == 0 && changed
           && changed->first_time == 100 && changed->last_time == 100 && !strcmp(changed->sp, "qa"));
    assert(dr__record_flow("pubA", "t/a", "subA", sp, n, 100, &changed) == 0 && changed == NULL);
    assert(dr__record_flow("pubA", "t/a", "subA", sp, n, 101, &changed) == 0 && changed && changed->last_time == 101);
    purpose_set_free(sp, n);

    assert(dr__restore_flow("pubA", "t/b", "subB", "qb|qc", 50, 60) == 0);
    struct dr_sublist *r = relevant("pubA", "t/b", "qc", "*", 0, 0);
    assert(sublist_count(r) == 1 && sublist_has(r, "subB"));
    dr__free_sublist(r);
    r = relevant("pubA", "*", "*", "*", 70, 0);
    assert(sublist_count(r) == 1 && sublist_has(r, "subB"));
    dr__free_sublist(r);

    dr_registry_cleanup();
    printf("ok - flows report their changes and restored flows are relevant\n");
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
    test_delivery_time_sp();
    test_flow_changes_and_restore();
    printf("\nAll dr relevance tests passed.\n");
    return 0;
}
