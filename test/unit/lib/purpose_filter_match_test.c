/* Unit test for the purpose algebra (lib/dap/purpose_filters.c), paper section 4.
 *
 * A collection of purpose filters ('|' or ',' separated, outside braces) expands to
 * the set of purposes it describes. A message's MP permits a subscription's SP when
 * every purpose of the SP is in the MP (Pi(SP) subset of Pi(MP)), or the MP is "*".
 */
#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include "purpose_filters.h"

extern void mosquitto_free(void *mem);

/* Expand a collection and check it describes exactly the expected purposes, in sorted order. */
static void expect_set(const char *filters, const char **expected, uint32_t n)
{
    char **set = NULL;
    uint32_t count = 99;
    assert(purpose_set_expand(filters, &set, &count) == 0);
    assert(count == n);
    for(uint32_t i = 0; i < n; i++){
        assert(strcmp(set[i], expected[i]) == 0);
    }
    purpose_set_free(set, count);
}

static char *canonical(const char *filters)
{
    char *canon = NULL;
    assert(purpose_filter_canonical(filters, &canon) == 0);
    assert(canon != NULL);
    return canon;
}

static void expect_canonical(const char *filters, const char *expected)
{
    char *canon = canonical(filters);
    assert(strcmp(canon, expected) == 0);
    mosquitto_free(canon);
}

static bool permits(const char *mp_filters, const char *sp_filters)
{
    char *mp = canonical(mp_filters);
    char **sp = NULL;
    uint32_t sp_n = 0;
    assert(purpose_set_expand(sp_filters, &sp, &sp_n) == 0);
    bool r = purpose_mp_permits(mp, sp, sp_n);
    purpose_set_free(sp, sp_n);
    mosquitto_free(mp);
    return r;
}

static bool permits_unrevoked(const char *mp_filters, const char *revoked_filters, const char *sp_filters)
{
    char *mp = canonical(mp_filters);
    char *revoked = canonical(revoked_filters);
    char **sp = NULL;
    uint32_t sp_n = 0;
    assert(purpose_set_expand(sp_filters, &sp, &sp_n) == 0);
    bool r = purpose_mp_permits_unrevoked(mp, revoked, sp, sp_n);
    purpose_set_free(sp, sp_n);
    mosquitto_free(mp);
    mosquitto_free(revoked);
    return r;
}

static bool intersect(const char *a_filters, const char *b_filters)
{
    char *a = canonical(a_filters);
    char *b = canonical(b_filters);
    bool r = purpose_sets_intersect(a, b);
    mosquitto_free(a);
    mosquitto_free(b);
    return r;
}

static void test_table1_descriptions(void)
{
    const char *t1[] = {"maintenance", "quality"};
    expect_set("{quality,maintenance}", t1, 2);

    const char *t2[] = {"partner", "partner/billing", "partner/logistics"};
    expect_set("partner/{.,logistics,billing}", t2, 3);

    const char *t3[] = {"maintenance/predictive/report", "maintenance/routine/report"};
    expect_set("maintenance/{predictive,routine}/report", t3, 2);

    const char *t4[] = {"maintenance/forecast", "maintenance/planning",
                        "operations/forecast", "operations/planning"};
    expect_set("{operations,maintenance}/{forecast,planning}", t4, 4);
    printf("ok - the paper's Table 1 filters describe the listed purposes\n");
}

static void test_collections(void)
{
    /* '|' and ',' both separate filters; braces keep their ',' and duplicates collapse. */
    const char *c1[] = {"maintenance/predictive", "maintenance/routine", "quality/assurance"};
    expect_set("quality/assurance|maintenance/{predictive,routine}", c1, 3);
    expect_set("maintenance/{predictive,routine},quality/assurance,quality/assurance", c1, 3);

    /* An empty collection (a withdrawn SP) describes nothing. */
    expect_set("", NULL, 0);
    expect_set(NULL, NULL, 0);

    expect_canonical("quality/assurance|maintenance/{predictive,routine}",
                     "maintenance/predictive|maintenance/routine|quality/assurance");
    expect_canonical("quality/assurance|*", "*");

    char **set = NULL;
    uint32_t n = 0;
    assert(purpose_set_expand("b|a", &set, &n) == 0);
    char *joined = purpose_set_join(set, n);
    assert(!strcmp(joined, "a|b"));
    assert(purpose_set_is(joined, set, n));
    assert(!purpose_set_is("a", set, n));
    assert(!purpose_set_is("a|b|c", set, n));
    assert(!purpose_set_is("a|bb", set, n));
    assert(purpose_set_is("", set, 0) && purpose_set_is(NULL, NULL, 0) && !purpose_set_is("a", NULL, 0));
    mosquitto_free(joined);
    purpose_set_free(set, n);
    printf("ok - collections expand, sort and deduplicate\n");
}

static void test_matching(void)
{
    /* Section 4.2 example. */
    assert(permits("maintenance/{predictive,calibration}", "maintenance/predictive"));
    assert(!permits("quality/assurance", "maintenance/predictive"));

    /* Every SP purpose must be permitted; the MP may permit more. */
    assert(permits("quality/assurance|operations/forecast", "quality/assurance"));
    assert(permits("quality/assurance|operations/forecast", "operations/forecast|quality/assurance"));
    assert(!permits("quality/assurance", "quality/assurance|operations/forecast"));

    /* An MP of "*" permits any SP; "*" in an SP is not a wildcard. */
    assert(permits("*", "anything/at/all"));
    assert(!permits("quality/assurance", "*"));

    /* Purposes are explicit: a parent does not imply its children, nor the reverse. */
    assert(!permits("maintenance", "maintenance/predictive"));
    assert(!permits("maintenance/predictive", "maintenance"));

    /* An empty SP (consent withdrawn) matches nothing. */
    assert(!permits("quality/assurance", ""));
    assert(!permits("*", ""));
    printf("ok - an MP permits an SP only when it permits every SP purpose\n");
}

static void test_restrict(void)
{
    /* RESTRICT revokes the OpPFs purposes from the MP. */
    assert(permits_unrevoked("quality/assurance|operations/forecast", "operations/forecast",
                             "quality/assurance"));
    assert(!permits_unrevoked("quality/assurance|operations/forecast", "operations/forecast",
                              "operations/forecast"));
    assert(!permits_unrevoked("quality/assurance|operations/forecast", "operations/forecast",
                              "quality/assurance|operations/forecast"));
    assert(!permits_unrevoked("*", "operations/forecast", "operations/forecast"));
    assert(permits_unrevoked("*", "operations/forecast", "quality/assurance"));
    assert(!permits_unrevoked("quality/assurance", "*", "quality/assurance"));
    printf("ok - a RESTRICT takes its purposes out of the MP\n");
}

static void test_intersection(void)
{
    assert(intersect("quality/assurance|operations/forecast", "operations/forecast"));
    assert(!intersect("quality/assurance", "operations/forecast"));
    assert(intersect("*", "operations/forecast"));
    assert(intersect("maintenance/{predictive,routine}", "maintenance/routine,vendor/x"));
    assert(!intersect("", "quality/assurance"));

    char *ops = canonical("maintenance/{predictive,routine}");
    char **sp = NULL;
    uint32_t n = 0;
    assert(purpose_set_expand("quality|maintenance/routine", &sp, &n) == 0);
    assert(purpose_set_intersects(ops, sp, n));
    assert(purpose_set_intersects("*", sp, n));
    assert(!purpose_set_intersects("vendor", sp, n));
    assert(!purpose_set_intersects(ops, sp, 0));
    purpose_set_free(sp, n);
    mosquitto_free(ops);
    printf("ok - purpose sets intersect when they share a purpose\n");
}

static void test_recognized(void)
{
    char *recognized = canonical("quality/assurance|maintenance/{predictive,routine}");
    char **set = NULL;
    uint32_t n = 0;

    assert(purpose_set_expand("maintenance/routine|quality/assurance", &set, &n) == 0);
    assert(purpose_set_recognized(recognized, set, n));
    assert(purpose_set_recognized(NULL, set, n));
    purpose_set_free(set, n);
    assert(purpose_set_expand("maintenance|quality/assurance", &set, &n) == 0);
    assert(!purpose_set_recognized(recognized, set, n));
    purpose_set_free(set, n);
    /* A set that names no purpose names no unrecognized one. */
    assert(purpose_set_recognized(recognized, NULL, 0));
    mosquitto_free(recognized);
    printf("ok - a purpose set is recognized when every purpose is in the recognized set\n");
}

static void test_limit(void)
{
    /* Eleven two-way sets describe 2048 purposes, past PURPOSE_SET_MAX. */
    const char *big = "{a,b}/{a,b}/{a,b}/{a,b}/{a,b}/{a,b}/{a,b}/{a,b}/{a,b}/{a,b}/{a,b}";
    char **set = NULL;
    uint32_t count = 99;
    assert(purpose_set_expand(big, &set, &count) != 0);
    assert(set == NULL && count == 0);
    char *canon = (char *)"unset";
    assert(purpose_filter_canonical(big, &canon) != 0);
    assert(canon == NULL);

    /* Ten describe 1024, which is allowed. */
    assert(purpose_set_expand(big + 6, &set, &count) == 0);
    assert(count == PURPOSE_SET_MAX);
    purpose_set_free(set, count);
    printf("ok - a collection may describe at most PURPOSE_SET_MAX purposes\n");
}

static void test_invalid_terms(void)
{
    /* A term may not contain a separator or a brace: "x/{a|y}" must not describe "y". */
    const char *bad[] = {"x/{a|y}", "{z|a}", "a{b}", "x/{a,{b}}", "x/a}", "{a"};
    for(size_t i = 0; i < sizeof(bad)/sizeof(bad[0]); i++){
        char **set = NULL;
        uint32_t count = 99;
        char *canon = (char *)"unset";
        assert(purpose_set_expand(bad[i], &set, &count) != 0);
        assert(set == NULL && count == 0);
        assert(purpose_filter_canonical(bad[i], &canon) != 0 && canon == NULL);
    }
    printf("ok - a term containing a separator or a brace is invalid\n");
}

int main(void)
{
    test_table1_descriptions();
    test_collections();
    test_matching();
    test_restrict();
    test_intersection();
    test_limit();
    test_invalid_terms();
    test_recognized();
    printf("\nAll purpose_filter_match tests passed.\n");
    return 0;
}
