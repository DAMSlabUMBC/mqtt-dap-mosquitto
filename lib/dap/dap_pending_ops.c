/* dap_pending_ops.c */

#include <string.h>
#include <stdlib.h>

/* mosquitto_internal.h pulls in config.h, which points uthash at the mosquitto
 * allocators. Include it before dap_pending_ops.h so uthash.h picks up those
 * macros rather than its own malloc/free defaults. */
#include "mosquitto_internal.h"
#include "util_mosq.h"
#include "dap_pending_ops.h"
#include "purpose_filters.h"

/* Initialize an empty map. */
int dap_pending_ops_init(struct dap_pending_ops *map)
{
    if(!map) return 1;
    map->publishers = NULL;
    map->next_op_id = 1; /* op ids are broker-assigned starting at 1 */
    return 0;
}

/* Free a list of duplicated filter strings. */
static void dap__free_filter_list(char **list, size_t n)
{
    for(size_t i = 0; i < n; i++){
        mosquitto_FREE(list[i]);
    }
    mosquitto_FREE(list);
}

/* Free a single operation node along with its filter lists. */
static void dap__free_op(struct dap_pending_op *op)
{
    dap__free_filter_list(op->topic_filters, op->num_topic_filters);
    mosquitto_FREE(op->purposes);
    dap__free_filter_list(op->subscriber_filters, op->num_subscriber_filters);
    mosquitto_FREE(op);
}

/*
 * Split a comma-separated filter value into a list of duplicated strings. Empty
 * tokens are skipped, and a NULL or otherwise empty value yields a single "*"
 * element so a missing filter matches anything. Returns 0 on success and writes
 * the list and its length to *out_list / *out_n; returns non-zero on allocation
 * failure (leaving nothing allocated).
 */
static int dap__parse_filter_list(const char *value, char ***out_list, size_t *out_n)
{
    char **list = NULL;
    size_t n = 0;

    if(value){
        const char *p = value;
        while(*p){
            const char *comma = strchr(p, ',');
            size_t len = comma ? (size_t)(comma - p) : strlen(p);
            if(len > 0){
                char **grown = mosquitto_realloc(list, sizeof(char*) * (n + 1));
                if(!grown) goto fail;
                list = grown;
                list[n] = mosquitto_malloc(len + 1);
                if(!list[n]) goto fail;
                memcpy(list[n], p, len);
                list[n][len] = '\0';
                n++;
            }
            if(!comma) break;
            p = comma + 1;
        }
    }

    /* A missing or all-empty value behaves as "match anything". */
    if(n == 0){
        list = mosquitto_realloc(list, sizeof(char*));
        if(!list) return 1;
        list[0] = mosquitto_strdup("*");
        if(!list[0]){
            mosquitto_FREE(list);
            return 1;
        }
        n = 1;
    }

    *out_list = list;
    *out_n = n;
    return 0;

fail:
    dap__free_filter_list(list, n);
    return 1;
}

/* Look up the hash entry for a publisher, or NULL if there is none. */
static struct dap_pub_entry *dap__find_publisher(struct dap_pending_ops *map, const char *pub_id)
{
    struct dap_pub_entry *entry = NULL;
    HASH_FIND_STR(map->publishers, pub_id, entry);
    return entry;
}

/* Add an op with the given id to the head of pub_id's list. */
static int dap__add_operation(struct dap_pending_ops *map,
                              uint64_t op_id,
                              const char *pub_id,
                              enum dap_op_type type,
                              uint64_t order,
                              const char *topic_filters,
                              const char *purpose_filters,
                              const char *subscriber_filters)
{
    struct dap_pending_op *op = mosquitto_calloc(1, sizeof(*op));
    if(!op) return 1;
    op->op_id     = op_id;
    op->order     = order;
    op->type      = type;
    if(dap__parse_filter_list(topic_filters, &op->topic_filters, &op->num_topic_filters)
            || dap__parse_filter_list(subscriber_filters, &op->subscriber_filters, &op->num_subscriber_filters)){
        dap__free_op(op);
        return 1;
    }
    /* An absent or empty DAP-OpPFs scopes the operation to every purpose. */
    if(purpose_filters && purpose_filters[0] != '\0' && strcmp(purpose_filters, "*")){
        if(purpose_filter_canonical(purpose_filters, &op->purposes)){
            dap__free_op(op);
            return 1;
        }
    }

    /* Find or create the publisher's entry. */
    struct dap_pub_entry *entry = dap__find_publisher(map, pub_id);
    if(!entry){
        entry = mosquitto_calloc(1, sizeof(*entry));
        if(!entry){
            dap__free_op(op);
            return 1;
        }
        entry->pub_id = mosquitto_strdup(pub_id);
        if(!entry->pub_id){
            mosquitto_FREE(entry);
            dap__free_op(op);
            return 1;
        }
        entry->ops = NULL;
        HASH_ADD_KEYPTR(hh, map->publishers, entry->pub_id, strlen(entry->pub_id), entry);
    }

    /* Link at the head of the publisher's op list. */
    op->next = entry->ops;
    entry->ops = op;
    return 0;
}

static bool dap__op_id_present(struct dap_pending_ops *map, uint64_t op_id)
{
    struct dap_pub_entry *entry, *tmp;
    HASH_ITER(hh, map->publishers, entry, tmp){
        for(struct dap_pending_op *op = entry->ops; op; op = op->next){
            if(op->op_id == op_id) return true;
        }
    }
    return false;
}

int dap_pending_ops_insert_operation(struct dap_pending_ops *map,
                                     const char *pub_id,
                                     enum dap_op_type type,
                                     uint64_t order,
                                     const char *topic_filters,
                                     const char *purpose_filters,
                                     const char *subscriber_filters,
                                     uint64_t *op_id_out)
{
    if(!map || !pub_id) return 1;

    uint64_t op_id = map->next_op_id;
    if(dap__add_operation(map, op_id, pub_id, type, order,
                          topic_filters, purpose_filters, subscriber_filters)){
        return 1;
    }
    map->next_op_id++;

    if(op_id_out) *op_id_out = op_id;
    return 0;
}

int dap_pending_ops_restore_operation(struct dap_pending_ops *map,
                                      uint64_t op_id,
                                      const char *pub_id,
                                      enum dap_op_type type,
                                      uint64_t order,
                                      const char *topic_filters,
                                      const char *purpose_filters,
                                      const char *subscriber_filters)
{
    if(!map || !pub_id || op_id == 0) return 1;
    if(dap__op_id_present(map, op_id)) return 1;

    if(dap__add_operation(map, op_id, pub_id, type, order,
                          topic_filters, purpose_filters, subscriber_filters)){
        return 1;
    }
    dap_pending_ops_reserve_op_id(map, op_id);
    return 0;
}

void dap_pending_ops_reserve_op_id(struct dap_pending_ops *map, uint64_t op_id)
{
    if(!map) return;
    if(op_id >= map->next_op_id){
        map->next_op_id = op_id + 1;
    }
}

uint64_t dap_pending_ops_allocate_op_id(struct dap_pending_ops *map)
{
    if(!map) return 0;
    return map->next_op_id++;
}

struct dap_pending_op *dap_pending_ops_lookup_operations_for_publisher(struct dap_pending_ops *map,
                                                                       const char *pub_id)
{
    if(!map || !pub_id) return NULL;
    struct dap_pub_entry *entry = dap__find_publisher(map, pub_id);
    return entry ? entry->ops : NULL;
}

int dap_pending_ops_remove_operation_by_id(struct dap_pending_ops *map, uint64_t op_id)
{
    if(!map) return 1;

    struct dap_pub_entry *entry, *tmp;
    HASH_ITER(hh, map->publishers, entry, tmp){
        struct dap_pending_op *prev = NULL;
        struct dap_pending_op *op = entry->ops;
        while(op){
            if(op->op_id == op_id){
                /* Unlink the operation from this publisher's list. */
                if(prev){
                    prev->next = op->next;
                }else{
                    entry->ops = op->next;
                }
                dap__free_op(op);

                /* Drop the publisher entry once it has no operations left. */
                if(!entry->ops){
                    HASH_DEL(map->publishers, entry);
                    mosquitto_FREE(entry->pub_id);
                    mosquitto_FREE(entry);
                }
                return 0;
            }
            prev = op;
            op = op->next;
        }
    }
    return 1; /* not found */
}

void dap_pending_ops_destroy(struct dap_pending_ops *map)
{
    if(!map) return;

    struct dap_pub_entry *entry, *tmp;
    HASH_ITER(hh, map->publishers, entry, tmp){
        struct dap_pending_op *op = entry->ops;
        while(op){
            struct dap_pending_op *next = op->next;
            dap__free_op(op);
            op = next;
        }
        HASH_DEL(map->publishers, entry);
        mosquitto_FREE(entry->pub_id);
        mosquitto_FREE(entry);
    }
    map->publishers = NULL;
    map->next_op_id = 1;
}

/* A filter list matches when any element is the "*" wildcard or equals the field. */
static bool dap__filter_list_matches(char **list, size_t n, const char *value)
{
    for(size_t i = 0; i < n; i++){
        if(!strcmp(list[i], "*")) return true;
        if(value && !strcmp(list[i], value)) return true;
    }
    return false;
}

/* A topic filter list matches when any element is "*" or an MQTT filter matching the topic. */
static bool dap__topic_list_matches(char **list, size_t n, const char *topic)
{
    for(size_t i = 0; i < n; i++){
        bool result = false;
        if(!strcmp(list[i], "*")) return true;
        if(topic && mosquitto_topic_matches_sub(list[i], topic, &result) == MOSQ_ERR_SUCCESS && result){
            return true;
        }
    }
    return false;
}

/* True when every filter matches and the message was received before the op. */
static bool dap__op_applies(const struct dap_pending_op *op, const char *topic,
                            char *const *sp, uint32_t sp_count,
                            const char *subscriber_id, uint64_t msg_order)
{
    if(msg_order > op->order) return false;
    if(!dap__topic_list_matches(op->topic_filters, op->num_topic_filters, topic)) return false;
    if(op->purposes && sp && !purpose_set_intersects(op->purposes, sp, sp_count)) return false;
    if(!dap__filter_list_matches(op->subscriber_filters, op->num_subscriber_filters, subscriber_id)) return false;
    return true;
}

enum dap_op_action dap_pending_ops_match(struct dap_pending_ops *map,
                                         const char *pub_id,
                                         const char *topic,
                                         char *const *sp,
                                         uint32_t sp_count,
                                         const char *subscriber_id,
                                         uint64_t msg_order,
                                         uint64_t *op_id_out,
                                         const char **revoked_out)
{
    if(op_id_out) *op_id_out = 0;
    if(revoked_out) *revoked_out = NULL;
    if(!map || !pub_id) return DAP_OP_ACTION_NONE;

    struct dap_pub_entry *entry = dap__find_publisher(map, pub_id);
    if(!entry) return DAP_OP_ACTION_NONE;

    /* Scan the publisher's ops, letting a DELETE win outright and otherwise
     * keeping the most recent matching RESTRICT. */
    struct dap_pending_op *best_restrict = NULL;
    for(struct dap_pending_op *op = entry->ops; op; op = op->next){
        if(!dap__op_applies(op, topic, sp, sp_count, subscriber_id, msg_order)){
            continue;
        }
        if(op->type == DAP_OP_DELETE){
            if(op_id_out) *op_id_out = op->op_id;
            return DAP_OP_ACTION_DROP;
        }
        if(!best_restrict || op->order > best_restrict->order){
            best_restrict = op;
        }
    }

    if(best_restrict){
        if(op_id_out) *op_id_out = best_restrict->op_id;
        if(revoked_out) *revoked_out = best_restrict->purposes ? best_restrict->purposes : "*";
        return DAP_OP_ACTION_RESTRICT;
    }
    return DAP_OP_ACTION_NONE;
}
