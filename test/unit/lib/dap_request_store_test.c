/* Standalone isolation test for the store of operation requests held for offline
 * subscribers (lib/dap/dap_request_store.c), paper 6.3: "for a currently offline
 * relevant subscriber, the broker persists the request for the deadline's duration
 * and delivers it on reconnection".
 */
#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "mosquitto.h"
#include "dap_request_store.h"

static mosquitto_property *props(const char *op)
{
    mosquitto_property *p = NULL;
    assert(mosquitto_property_add_string_pair(&p, MQTT_PROP_USER_PROPERTY, "DAP-OpType", op) == 0);
    return p;
}

static int count(struct dap_stored_request *list)
{
    int n = 0;
    for(; list; list = list->next) n++;
    return n;
}

static void test_take_returns_requests_in_order(void)
{
    struct dap_request_store store;
    dap_request_store_init(&store);

    assert(dap_request_store_add(&store, "subA", 1, 100, "h1", 2, props("HISTORY")) == 0);
    assert(dap_request_store_add(&store, "subA", 2, 100, NULL, 0, props("DELETE")) == 0);
    assert(dap_request_store_add(&store, "subB", 3, 100, "x", 1, NULL) == 0);
    assert(dap_request_store_has(&store, "subA") && dap_request_store_has(&store, "subB"));
    assert(!dap_request_store_has(&store, "subC"));

    struct dap_stored_request *list = dap_request_store_take(&store, "subA", 50);
    assert(count(list) == 2);
    assert(list->op_id == 1 && list->payloadlen == 2 && !memcmp(list->payload, "h1", 2));
    assert(list->properties != NULL);
    assert(list->next->op_id == 2 && list->next->payload == NULL);
    dap_request_store_free_list(list);

    /* Taking empties the subscriber's inbox and leaves the others. */
    assert(!dap_request_store_has(&store, "subA"));
    assert(dap_request_store_take(&store, "subA", 50) == NULL);
    assert(dap_request_store_has(&store, "subB"));

    dap_request_store_destroy(&store);
    printf("ok - a subscriber's stored requests are taken once, oldest first\n");
}

static void test_deadline_bounds_storage(void)
{
    struct dap_request_store store;
    dap_request_store_init(&store);

    assert(dap_request_store_add(&store, "subA", 1, 100, NULL, 0, props("HISTORY")) == 0);
    assert(dap_request_store_add(&store, "subA", 2, 200, NULL, 0, NULL) == 0);
    assert(dap_request_store_add(&store, "subB", 3, 100, NULL, 0, NULL) == 0);

    /* Past a request's deadline it is no longer delivered. */
    struct dap_stored_request *list = dap_request_store_take(&store, "subA", 150);
    assert(count(list) == 1 && list->op_id == 2);
    dap_request_store_free_list(list);

    /* Expiry reclaims what was never taken. */
    dap_request_store_expire(&store, 150);
    assert(!dap_request_store_has(&store, "subB"));

    dap_request_store_destroy(&store);
    printf("ok - requests are dropped once their deadline passes\n");
}

static void test_remove_operation(void)
{
    struct dap_request_store store;
    dap_request_store_init(&store);

    assert(dap_request_store_add(&store, "subA", 1, 100, NULL, 0, NULL) == 0);
    assert(dap_request_store_add(&store, "subA", 2, 100, NULL, 0, NULL) == 0);
    assert(dap_request_store_add(&store, "subB", 1, 100, NULL, 0, NULL) == 0);

    dap_request_store_remove_operation(&store, 1);
    assert(!dap_request_store_has(&store, "subB"));
    struct dap_stored_request *list = dap_request_store_take(&store, "subA", 50);
    assert(count(list) == 1 && list->op_id == 2);
    dap_request_store_free_list(list);

    dap_request_store_destroy(&store);
    printf("ok - an operation's requests can be withdrawn from every inbox\n");
}

int main(void)
{
    test_take_returns_requests_in_order();
    test_deadline_bounds_storage();
    test_remove_operation();
    printf("\nAll dap_request_store tests passed.\n");
    return 0;
}
