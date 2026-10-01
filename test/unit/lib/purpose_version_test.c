/* Standalone isolation test for MP purpose-filter version tracking
 * (lib/mp_registry.c).
 *
 * Checks that a stored MP starts at version 1 and bumps by one on every update of
 * the same id/topic pair, while a fresh topic versions independently. (SP version
 * tracking was retired from the registry in paper v2 and now lives on the
 * subscription leaf, covered by the broker subscribe path and dap_stamp_test.)
 * Build and run on its own (CUnit is not required here):
 *
 *   cc -I../../.. -I../../../lib -I../../../include -I../../../libcommon \
 *      -I../../../src -I../../../common -I/opt/homebrew/include \
 *      purpose_version_test.c ../../../lib/mp_registry.c \
 *      ../../../libcommon/memory_common.c -o purpose_version_test && ./purpose_version_test
 */

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "mosquitto/defs.h" /* MOSQ_DAP_MP_REG_TOPIC prefix */
#include "mp_registry.h"

static void test_mp_version_increments_on_update(void)
{
    mp_registry_init();

    /* Unknown topic has no version yet. */
    assert(mp__lookup("pub1", "sensors/temp") == NULL);

    /* First registration starts at 1. */
    mp__register_topic("pub1", "sensors/temp", "ads/targeted");

    struct mp_entry *stored = mp__lookup("pub1", "sensors/temp");
    assert(stored->version == 1);
    assert(!strcmp(stored->purpose_filter, "ads/targeted"));

    /* Each update of the same pub/topic pair bumps the version by one... */
    mp__register_topic("pub1", "sensors/temp", "billing/electricity");
    stored = mp__lookup("pub1", "sensors/temp");
    assert(stored->version == 2);
    /* ...and the stored value still tracks the latest write. */
    assert(!strcmp(stored->purpose_filter, "billing/electricity"));

    mp__register_topic("pub1", "sensors/temp", "ads/targeted");
    stored = mp__lookup("pub1", "sensors/temp");
    assert(stored->version == 3);

    /* A different topic versions independently of the first. */
    mp__register_topic("pub1", "sensors/humidity", "research");
    stored = mp__lookup("pub1", "sensors/humidity");
    assert(stored->version == 1);
    stored = mp__lookup("pub1", "sensors/temp");
    assert(stored->version == 3);

    mp_registry_cleanup();
    printf("ok - MP version starts at 1 and increments on each update\n");
}

/*
 * Mirror of the bracketed parse handle_publish.c performs for a registration
 * PUBLISH on the registration-by-topic path: split "<prefix><real_topic>[<value>]"
 * into the real topic and the MP/SP value, exactly as src/handle_publish.c does
 * (the MP_reg branch around line 309, the SP_reg branch around line 342). This is
 * a copy of that parse so the standalone test can drive the realistic topic-string
 * format without linking the whole broker; it does not exercise handle_publish.c
 * itself, nor the SP_reg HASH/PLUS wildcard substitution.
 */
static void parse_reg_topic(const char *full, const char *prefix, char *rt_out, char *val_out)
{
    const char *rest = full + strlen(prefix);
    const char *b = strchr(rest, '[');
    assert(b != NULL);
    const char *eb = strrchr(b, ']');
    assert(eb != NULL);

    size_t rlen = (size_t)(b - rest);
    memcpy(rt_out, rest, rlen);
    rt_out[rlen] = '\0';

    size_t vlen = (size_t)(eb - (b + 1));
    memcpy(val_out, b + 1, vlen);
    val_out[vlen] = '\0';
}

/* The real MP registration call site (handle_publish.c:335) feeds the parsed real
 * topic and MP into mp__register_topic(context->id, rt, mp); confirm a re-published
 * registration for the same topic bumps the version the Case 4 send path will read. */
static void test_mp_registration_topic_path_bumps_version(void)
{
    mp_registry_init();

    const char *pub = "publisherA";
    char rt[256], val[256];

    /* First "$DAP/MP_reg/sensors/temp[ads/targeted]" PUBLISH -> version 1. */
    parse_reg_topic(MOSQ_DAP_MP_REG_TOPIC "sensors/temp[ads/targeted]",
                    MOSQ_DAP_MP_REG_TOPIC, rt, val);
    assert(!strcmp(rt, "sensors/temp"));
    assert(!strcmp(val, "ads/targeted"));
    mp__register_topic(pub, rt, val);
    struct mp_entry *stored = mp__lookup(pub, rt);
    assert(stored->version == 1);
    assert(!strcmp(stored->purpose_filter, "ads/targeted"));

    /* Re-registration for the same real topic -> version 2, latest value stored. */
    parse_reg_topic(MOSQ_DAP_MP_REG_TOPIC "sensors/temp[billing/electricity]",
                    MOSQ_DAP_MP_REG_TOPIC, rt, val);
    mp__register_topic(pub, rt, val);
    stored = mp__lookup(pub, rt);
    assert(stored->version == 2);
    assert(!strcmp(stored->purpose_filter, "billing/electricity"));

    mp_registry_cleanup();
    printf("ok - MP_reg topic path parses and bumps version on re-registration\n");
}

/* Publishers sharing a topic each keep their own MP, even when their entries share a
 * hash bucket (2000 entries in the table make that all but certain). */
static void test_mp_entries_are_per_publisher(void)
{
    char id[32], mp[32];

    mp_registry_init();
    for(int i = 0; i < 2000; i++){
        snprintf(id, sizeof(id), "pub%d", i);
        snprintf(mp, sizeof(mp), "purpose/%d", i);
        mp__register_topic(id, "shared/topic", mp);
    }
    for(int i = 0; i < 2000; i++){
        snprintf(id, sizeof(id), "pub%d", i);
        snprintf(mp, sizeof(mp), "purpose/%d", i);
        struct mp_entry *stored = mp__lookup(id, "shared/topic");
        assert(stored != NULL);
        assert(!strcmp(stored->purpose_filter, mp));
        assert(stored->version == 1);
    }
    /* The id and topic do not run together: "pub1" + "2/t" is not "pub12" + "/t". */
    mp__register_topic("pub12", "/t", "a");
    assert(mp__lookup("pub1", "2/t") == NULL);
    mp_registry_cleanup();
    printf("ok - publishers on the same topic keep separate MPs\n");
}

int main(void)
{
    test_mp_version_increments_on_update();
    test_mp_registration_topic_path_bumps_version();
    test_mp_entries_are_per_publisher();
    printf("\nAll purpose version tests passed.\n");
    return 0;
}
