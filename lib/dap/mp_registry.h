#ifndef MP_REGISTRY_H
#define MP_REGISTRY_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* A single entry in the hash chain. */
struct mp_entry {
    char *id;              /* publisher client id */
    char *topic;           /* "sensors/temp" */
    char *purpose_filter;  /* "ads/targeted" */
    uint32_t version;      /* MP version, starts at 1 and bumps on every update */
    struct mp_entry *next; /* pointer to next in the chain */
};

/* Initializes the MP registry */
void mp_registry_init(void);

/* Cleans up all the stored entries */
void mp_registry_cleanup(void);

/* Registers or overwrites the purpose filter of a publisher's topic. Returns
 * MOSQ_ERR_SUCCESS or MOSQ_ERR_NOMEM. */
int mp__register_topic(const char *id, const char *topic, const char *mp_value);

/* Looks up the stored purpose filter of a publisher's topic. */
struct mp_entry *mp__lookup(const char *id, const char *topic);

#ifdef __cplusplus
}
#endif

#endif 
