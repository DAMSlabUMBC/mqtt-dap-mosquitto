/* mp_registry.c */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "mosquitto_internal.h"  
#include "util_mosq.h"                
#include "mp_registry.h"

#define MPREG_HASH_SIZE 65535

/* The bucket array is for our hash table. */
static struct mp_entry *g_mp_buckets[MPREG_HASH_SIZE];


/* djb2 over the id, a NUL separator and the topic, so "ab"+"c" and "a"+"bc" differ. */
static unsigned int mp__hash(const char *id, const char *topic)
{
    unsigned long hash = 5381;
    const char *p;

    for(p = id; *p; p++){
        hash = ((hash << 5) + hash) + (unsigned char)*p;
    }
    hash = (hash << 5) + hash;
    for(p = topic; *p; p++){
        hash = ((hash << 5) + hash) + (unsigned char)*p;
    }
    return (unsigned int)(hash % MPREG_HASH_SIZE);
}

void mp_registry_init(void)
{
    /* Zero out the bucket array  */
    memset(g_mp_buckets, 0, sizeof(g_mp_buckets));
}

void mp_registry_cleanup(void)
{
    /* Free all the entries in all buckets. */
    for(int i=0; i<MPREG_HASH_SIZE; i++){
        struct mp_entry *curr = g_mp_buckets[i];
        while(curr){
            struct mp_entry *tmp = curr;
            curr = curr->next;
            mosquitto_FREE(tmp->id);
            mosquitto_FREE(tmp->topic);
            mosquitto_FREE(tmp->purpose_filter);
            mosquitto_FREE(tmp);
        }
        g_mp_buckets[i] = NULL;
    }
}

/* Store or overwrite the purpose filter of a publisher's topic */
int mp__register_topic(const char *id, const char *topic, const char *mp_value)
{
    unsigned int bucket_index = mp__hash(id, topic);
    struct mp_entry *entry = mp__lookup(id, topic);
    char *mp = mosquitto_strdup(mp_value);

    if(!mp) return MOSQ_ERR_NOMEM;
    if(entry){
        /* Overwrite the purpose filter and bump the version */
        mosquitto_FREE(entry->purpose_filter);
        entry->purpose_filter = mp;
        entry->version++;
        return MOSQ_ERR_SUCCESS;
    }

    /* Not found so create a new entry and link to head of chain */
    entry = mosquitto_calloc(1, sizeof(*entry));
    if(entry){
        entry->id = mosquitto_strdup(id);
        entry->topic = mosquitto_strdup(topic);
    }
    if(!entry || !entry->id || !entry->topic){
        if(entry){
            mosquitto_FREE(entry->id);
            mosquitto_FREE(entry->topic);
            mosquitto_FREE(entry);
        }
        mosquitto_FREE(mp);
        return MOSQ_ERR_NOMEM;
    }
    entry->purpose_filter = mp;
    entry->version = 1; /* first registration starts at version 1 */
    entry->next = g_mp_buckets[bucket_index];
    g_mp_buckets[bucket_index] = entry;
    return MOSQ_ERR_SUCCESS;
}

/* Look up the purpose filter of a publisher's topic */
struct mp_entry *mp__lookup(const char *id, const char *topic)
{
    struct mp_entry *curr;

    if(!id || !topic) return NULL;
    for(curr = g_mp_buckets[mp__hash(id, topic)]; curr; curr = curr->next){
        if(!strcmp(curr->id, id) && !strcmp(curr->topic, topic)){
            return curr;
        }
    }
    return NULL;
}
