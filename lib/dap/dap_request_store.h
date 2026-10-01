/* dap_request_store.h */
#ifndef DAP_REQUEST_STORE_H
#define DAP_REQUEST_STORE_H

#include <stdbool.h>
#include <stdint.h>
#include <time.h>

#include "config.h"
#include "mosquitto/defs.h"
#include "uthash.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Operation requests held for relevant subscribers that were offline when the
 * request was forwarded (paper 6.3). A subscriber receives them when it next
 * subscribes to its request topic, unless the request's deadline has passed.
 */
struct dap_stored_request {
    uint64_t op_id;
    time_t deadline;
    uint8_t qos;
    void *payload;
    uint32_t payloadlen;
    mosquitto_property *properties;
    struct dap_stored_request *next;
};

struct dap_request_inbox {
    char *sub_id;                         /* hash key */
    struct dap_stored_request *requests;  /* oldest first */
    UT_hash_handle hh;
};

struct dap_request_store {
    struct dap_request_inbox *inboxes;
};

void dap_request_store_init(struct dap_request_store *store);

/* Hold a request for sub_id. The payload is copied; properties are taken on success.
 * Returns 0, or non-zero on a bad argument or allocation failure. */
int dap_request_store_add(struct dap_request_store *store, const char *sub_id, uint64_t op_id,
                          time_t deadline, uint8_t qos, const void *payload, uint32_t payloadlen,
                          mosquitto_property *properties);

bool dap_request_store_has(struct dap_request_store *store, const char *sub_id);

/* Detach sub_id's requests whose deadline is still ahead of now, oldest first, freeing
 * the rest. Free the result with dap_request_store_free_list. */
struct dap_stored_request *dap_request_store_take(struct dap_request_store *store, const char *sub_id, time_t now);

/* Drop every request whose deadline has passed. */
void dap_request_store_expire(struct dap_request_store *store, time_t now);

/* Drop every request of op_id. */
void dap_request_store_remove_operation(struct dap_request_store *store, uint64_t op_id);

void dap_request_store_free_list(struct dap_stored_request *list);
void dap_request_store_destroy(struct dap_request_store *store);

#ifdef __cplusplus
}
#endif

#endif /* DAP_REQUEST_STORE_H */
