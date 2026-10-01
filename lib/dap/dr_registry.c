#include <string.h>
#include "dr_registry.h"
#include "mosquitto_internal.h"
#include "util_mosq.h"


struct dr_entry *dr_head = NULL;
struct dr_retained_entry *dr_retained_head = NULL;

void dr_registry_init(void)
{
    dr_head = NULL;
    dr_retained_head = NULL;
}

void dr_registry_cleanup(void)
{
    while(dr_head){
        struct dr_entry *e = dr_head;
        dr_head = dr_head->next;
        mosquitto_FREE(e->pub_id);
        mosquitto_FREE(e->topic);
        while(e->sub_list){
            struct dr_sublist *s = e->sub_list;
            e->sub_list = s->next;
            mosquitto_FREE(s->sub_id);
            mosquitto_FREE(s->sp);
            mosquitto_FREE(s);
        }
        mosquitto_FREE(e);
    }

    while(dr_retained_head)
    {
        struct dr_retained_entry *e = dr_retained_head;
        dr_retained_head = dr_retained_head->next;
        mosquitto_FREE(e->pub_id);
        mosquitto_FREE(e->topic);
        mosquitto_FREE(e);
    }
}

static struct dr_entry *dr__find_or_create(const char *pub_id, const char *topic)
{
    struct dr_entry *cur = dr_head;
    while(cur){
        if(!strcmp(cur->pub_id, pub_id) && !strcmp(cur->topic, topic)){
            return cur;
        }
        cur = cur->next;
    }
    struct dr_entry *e = mosquitto_calloc(1, sizeof(*e));
    if(!e) return NULL;
    e->pub_id = mosquitto_strdup(pub_id);
    e->topic  = mosquitto_strdup(topic);
    e->sub_list = NULL;
    e->next = dr_head;
    dr_head = e;
    return e;
}

void dr__record_recipient(const char *pub_id, const char *topic, const char *sub_id, time_t recv_time)
{
    dr__record_recipient_with_sp(pub_id, topic, sub_id, NULL, recv_time);
}

void dr__record_recipient_with_sp(const char *pub_id, const char *topic, const char *sub_id, const char *sp, time_t recv_time)
{
    struct dr_entry *entry = dr__find_or_create(pub_id, topic);
    if(!entry) return;
    /* Check if sub_id is already in sub_list */
    struct dr_sublist *s = entry->sub_list;
    while(s){
        if(!strcmp(s->sub_id, sub_id)){
            /* Already stored; refresh the SP when a newer one is supplied, and
             * advance the receipt time to the most recent flow. */
            if(sp){
                mosquitto_FREE(s->sp);
                s->sp = mosquitto_strdup(sp);
            }
            if(recv_time) s->recv_time = recv_time;
            return;
        }
        s = s->next;
    }
    s = mosquitto_calloc(1, sizeof(*s));
    if(!s) return;
    s->sub_id = mosquitto_strdup(sub_id);
    s->sp = sp ? mosquitto_strdup(sp) : NULL;
    s->recv_time = recv_time;
    s->next = entry->sub_list;
    entry->sub_list = s;
}

struct dr_sublist *dr__get_recipients(const char *pub_id, const char *topic)
{
    struct dr_entry *cur = dr_head;
    while(cur){
        if(!strcmp(cur->pub_id, pub_id) && !strcmp(cur->topic, topic)){
            /* Build a shallow copy of sub_list so the caller can iterate. */
            struct dr_sublist *copy_head = NULL;
            struct dr_sublist *orig = cur->sub_list;
            while(orig){
                struct dr_sublist *tmp = mosquitto_calloc(1, sizeof(*tmp));
                if(!tmp){
                    dr__free_sublist(copy_head);
                    return NULL;
                }
                tmp->sub_id = mosquitto_strdup(orig->sub_id);
                tmp->next = copy_head;
                copy_head = tmp;
                orig = orig->next;
            }
            return copy_head;
        }
        cur = cur->next;
    }
    return NULL;
}

void dr__record_retained_publisher(const char* pub_id, const char * topic)
{
    struct dr_retained_entry *cur = dr_retained_head;
    while(cur){
        if(!strcmp(cur->topic, topic)){
            mosquitto_FREE(cur->pub_id);
            cur->pub_id = mosquitto_strdup(pub_id);
            return;
        }
        cur = cur->next;
    }

    struct dr_retained_entry *e = mosquitto_calloc(1, sizeof(*e));
    if(!e) return;
    e->pub_id = mosquitto_strdup(pub_id);
    e->topic  = mosquitto_strdup(topic);
    e->next = dr_retained_head;
    dr_retained_head = e;
    return;
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

/* Condition for a single-valued field (receipt topic, subscriber id). */
static bool dr__field_matches(const char *filter_csv, const char *value)
{
    if(dr__filter_is_any(filter_csv)) return true;
    if(!value) return false;
    return dr__csv_has_token(filter_csv, value, strlen(value));
}

/*
 * Condition for the SP recorded at receipt time against the purpose filters: a
 * "*" SP matches any filter; otherwise at least one purpose named in the SP
 * must also appear among the purpose filters.
 */
static bool dr__purpose_matches(const char *pf_csv, const char *sp_csv)
{
    if(dr__filter_is_any(pf_csv)) return true;
    if(!sp_csv) return false;
    if(dr__filter_is_any(sp_csv)) return true;

    const char *p = sp_csv;
    while(*p){
        const char *comma = strchr(p, ',');
        size_t len = comma ? (size_t)(comma - p) : strlen(p);
        if(len > 0 && dr__csv_has_token(pf_csv, p, len)) return true;
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

    if(!pub_id) return NULL;

    /* Walk every recorded (pub_id, topic) flow for this publisher. */
    for(struct dr_entry *e = dr_head; e; e = e->next){
        if(strcmp(e->pub_id, pub_id)) continue;

        /* The receipt topic must match a topic filter. */
        if(!dr__field_matches(dap_op_properties->op_topic_filters, e->topic)) continue;

        for(struct dr_sublist *s = e->sub_list; s; s = s->next){
            /* The subscriber id must be in the client filters. */
            if(!dr__field_matches(dap_op_properties->op_client_filters, s->sub_id)) continue;
            /* The SP at receipt must share a purpose with the filters. */
            if(!dr__purpose_matches(dap_op_properties->op_purpose_filters, s->sp)) continue;
            /* The receipt time must fall within the DAP-OpAfter/OpBefore bounds;
             * a 0 bound is unbounded on that side. */
            if(dap_op_properties->op_after && s->recv_time < dap_op_properties->op_after) continue;
            if(dap_op_properties->op_before && s->recv_time > dap_op_properties->op_before) continue;
            /* Receipt itself is implicit: only recorded recipients are walked. */

            /* A subscriber relevant via several flows is still returned once. */
            if(dr__result_has(result, s->sub_id)) continue;

            struct dr_sublist *node = mosquitto_calloc(1, sizeof(*node));
            if(!node){
                dr__free_sublist(result);
                return NULL;
            }
            node->sub_id = mosquitto_strdup(s->sub_id);
            node->next = result;
            result = node;
        }
    }
    return result;
}
