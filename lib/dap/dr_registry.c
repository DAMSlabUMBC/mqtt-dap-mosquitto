#include <string.h>
#include "dr_registry.h"
#include "mosquitto_internal.h"
#include "util_mosq.h"
#include "purpose_filters.h"


struct dr_entry *dr_head = NULL;
/* The entries of dr_head, hashed by publisher and topic. */
static struct dr_entry *dr_index = NULL;

void dr_registry_init(void)
{
    dr_head = NULL;
    dr_index = NULL;
}

void dr_registry_cleanup(void)
{
    HASH_CLEAR(hh, dr_index);
    while(dr_head){
        struct dr_entry *e = dr_head;
        dr_head = dr_head->next;
        mosquitto_FREE(e->pub_id);
        mosquitto_FREE(e->topic);
        mosquitto_FREE(e->key);
        dr__free_sublist(e->sub_list);
        mosquitto_FREE(e);
    }
}

static struct dr_entry *dr__find_or_create(const char *pub_id, const char *topic)
{
    size_t plen = strlen(pub_id);
    size_t len = plen + 1 + strlen(topic);
    char stack_key[256];
    char *key = len <= sizeof(stack_key) ? stack_key : mosquitto_malloc(len);
    struct dr_entry *e = NULL;

    if(!key) return NULL;
    memcpy(key, pub_id, plen + 1);
    memcpy(key + plen + 1, topic, len - plen - 1);
    HASH_FIND(hh, dr_index, key, len, e);
    if(!e){
        e = mosquitto_calloc(1, sizeof(*e));
        if(e){
            e->pub_id = mosquitto_strdup(pub_id);
            e->topic = mosquitto_strdup(topic);
            e->key = mosquitto_malloc(len);
        }
        if(e && (!e->pub_id || !e->topic || !e->key)){
            mosquitto_FREE(e->pub_id);
            mosquitto_FREE(e->topic);
            mosquitto_FREE(e->key);
            mosquitto_FREE(e);
        }
        if(e){
            memcpy(e->key, key, len);
            HASH_ADD_KEYPTR(hh, dr_index, e->key, len, e);
            e->next = dr_head;
            dr_head = e;
        }
    }
    if(key != stack_key) mosquitto_FREE(key);
    return e;
}

/* Add a flow with the given SP and receipt times to an entry. */
static struct dr_sublist *dr__add_flow(struct dr_entry *entry, const char *sub_id, char *sp,
                                       time_t first_time, time_t last_time)
{
    struct dr_sublist *s = mosquitto_calloc(1, sizeof(*s));

    if(s) s->sub_id = mosquitto_strdup(sub_id);
    if(!s || !s->sub_id || !sp){
        mosquitto_FREE(sp);
        dr__free_sublist(s);
        return NULL;
    }
    s->sp = sp;
    s->first_time = first_time;
    s->last_time = last_time;
    s->next = entry->sub_list;
    entry->sub_list = s;
    return s;
}

int dr__record_flow(const char *pub_id, const char *topic, const char *sub_id,
                    char *const *sp, uint32_t sp_count, time_t recv_time,
                    const struct dr_sublist **changed)
{
    struct dr_entry *entry = dr__find_or_create(pub_id, topic);
    struct dr_sublist *s;

    *changed = NULL;
    if(!entry) return MOSQ_ERR_NOMEM;
    for(s = entry->sub_list; s; s = s->next){
        if(!strcmp(s->sub_id, sub_id) && purpose_set_is(s->sp, sp, sp_count)){
            if(recv_time < s->first_time){
                s->first_time = recv_time;
                *changed = s;
            }
            if(recv_time > s->last_time){
                s->last_time = recv_time;
                *changed = s;
            }
            return MOSQ_ERR_SUCCESS;
        }
    }
    s = dr__add_flow(entry, sub_id, purpose_set_join(sp, sp_count), recv_time, recv_time);
    if(!s) return MOSQ_ERR_NOMEM;
    *changed = s;
    return MOSQ_ERR_SUCCESS;
}

int dr__restore_flow(const char *pub_id, const char *topic, const char *sub_id,
                     const char *sp, time_t first_time, time_t last_time)
{
    struct dr_entry *entry = dr__find_or_create(pub_id, topic);

    if(!entry) return MOSQ_ERR_NOMEM;
    return dr__add_flow(entry, sub_id, mosquitto_strdup(sp ? sp : ""), first_time, last_time)
            ? MOSQ_ERR_SUCCESS : MOSQ_ERR_NOMEM;
}

void dr__free_sublist(struct dr_sublist *list)
{
    while(list){
        struct dr_sublist *tmp = list;
        list = list->next;
        mosquitto_FREE(tmp->sub_id);
        mosquitto_FREE(tmp->sp);
        mosquitto_FREE(tmp);
    }
}

/* True when (token, tlen) appears as an element of the comma-separated csv. */
static bool dr__csv_has_token(const char *csv, const char *token, size_t tlen)
{
    if(!csv) return false;
    const char *p = csv;
    while(*p){
        const char *comma = strchr(p, ',');
        size_t len = comma ? (size_t)(comma - p) : strlen(p);
        if(len == tlen && !strncmp(p, token, tlen)) return true;
        if(!comma) break;
        p = comma + 1;
    }
    return false;
}

/* A filter list counts as "not provided / any" when NULL, empty or holding "*". */
static bool dr__filter_is_any(const char *filter_csv)
{
    return !filter_csv || filter_csv[0] == '\0' || dr__csv_has_token(filter_csv, "*", 1);
}

/* Condition for the subscriber id. */
static bool dr__field_matches(const char *filter_csv, const char *value)
{
    if(dr__filter_is_any(filter_csv)) return true;
    if(!value) return false;
    return dr__csv_has_token(filter_csv, value, strlen(value));
}

/* Condition for the topic: any element of the list is an MQTT filter matching it. */
static bool dr__topic_matches(const char *filter_csv, const char *topic)
{
    if(dr__filter_is_any(filter_csv)) return true;

    const char *p = filter_csv;
    while(*p){
        const char *comma = strchr(p, ',');
        size_t len = comma ? (size_t)(comma - p) : strlen(p);
        if(len > 0){
            char *filter = mosquitto_strndup(p, len);
            bool result = false;
            if(filter && mosquitto_topic_matches_sub(filter, topic, &result) != MOSQ_ERR_SUCCESS){
                result = false;
            }
            mosquitto_FREE(filter);
            if(result) return true;
        }
        if(!comma) break;
        p = comma + 1;
    }
    return false;
}

/* True when sub_id is already in the result list (used to keep it deduplicated). */
static bool dr__result_has(struct dr_sublist *list, const char *sub_id)
{
    for(; list; list = list->next){
        if(!strcmp(list->sub_id, sub_id)) return true;
    }
    return false;
}

struct dr_sublist *dr__find_relevant_subscribers(const char *pub_id, struct dap__op_property *dap_op_properties)
{
    struct dr_sublist *result = NULL;
    char *purposes = NULL;

    if(!pub_id) return NULL;
    if(!dr__filter_is_any(dap_op_properties->op_purpose_filters)
            && purpose_filter_canonical(dap_op_properties->op_purpose_filters, &purposes)){
        return NULL;
    }

    /* Walk every recorded (pub_id, topic) flow for this publisher. */
    for(struct dr_entry *e = dr_head; e; e = e->next){
        if(strcmp(e->pub_id, pub_id)) continue;

        /* The topic must match a topic filter. */
        if(!dr__topic_matches(dap_op_properties->op_topic_filters, e->topic)) continue;

        for(struct dr_sublist *s = e->sub_list; s; s = s->next){
            /* The subscriber id must be in the client filters. */
            if(!dr__field_matches(dap_op_properties->op_client_filters, s->sub_id)) continue;
            /* The SP at delivery must share a purpose with the filters. */
            if(purposes && !purpose_sets_intersect(s->sp, purposes)) continue;
            /* The flow must overlap the DAP-OpAfter/OpBefore bounds; a 0 bound is
             * unbounded on that side. */
            if(dap_op_properties->op_after && s->last_time < dap_op_properties->op_after) continue;
            if(dap_op_properties->op_before && s->first_time > dap_op_properties->op_before) continue;
            /* Receipt itself is implicit: only recorded recipients are walked. */

            /* A subscriber relevant via several flows is still returned once. */
            if(dr__result_has(result, s->sub_id)) continue;

            struct dr_sublist *node = mosquitto_calloc(1, sizeof(*node));
            if(node) node->sub_id = mosquitto_strdup(s->sub_id);
            if(!node || !node->sub_id){
                mosquitto_FREE(node);
                dr__free_sublist(result);
                mosquitto_FREE(purposes);
                return NULL;
            }
            node->next = result;
            result = node;
        }
    }
    mosquitto_FREE(purposes);
    return result;
}
