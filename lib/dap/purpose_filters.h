#ifndef PURPOSE_FILTERS_H
#define PURPOSE_FILTERS_H

#include <stdbool.h>
#include <stdint.h>

#include "config.h"

/*
 * Purpose algebra (MQTT-DAP paper, section 4). A purpose filter describes a set of
 * purposes: each '/'-separated level is a term or a {a,b} set, and a '.' in a set
 * permits the parent level itself. A collection separates filters with '|' or ','
 * (outside braces). Purpose sets are kept sorted and free of duplicates; a canonical
 * set is one '|'-joined string of them, or "*" for an MP that permits everything.
 */

/* Upper bound on the purposes one collection may describe. */
#define PURPOSE_SET_MAX 1024

/* Expand a collection into the sorted purposes it describes. An empty or NULL
 * collection describes none. Returns MOSQ_ERR_SUCCESS, MOSQ_ERR_NOMEM, or
 * MOSQ_ERR_INVAL when a term contains a separator or a brace, or the collection
 * describes more than PURPOSE_SET_MAX purposes. Free the result with purpose_set_free. */
int purpose_set_expand(const char *filters, char ***purposes, uint32_t *count);
void purpose_set_free(char **purposes, uint32_t count);

/* Sort a purpose set and free its duplicates. */
void purpose_set_normalize(char **set, uint32_t *count);

/* A sorted set as one '|'-joined string ("" when empty). NULL on allocation failure. */
char *purpose_set_join(char *const *set, uint32_t n);

/* True when joined is the '|'-joined form of the sorted set. */
bool purpose_set_is(const char *joined, char *const *set, uint32_t n);

/* The canonical form of a collection, with purpose_set_expand's return codes. */
int purpose_filter_canonical(const char *filters, char **canonical);

/* Section 4.2 matching: true when the canonical MP permits every purpose of the sorted
 * SP set (Pi(SP) subset of Pi(MP)), or the MP is "*". An empty SP is permitted nothing. */
bool purpose_mp_permits(const char *mp, char *const *sp, uint32_t sp_count);

/* As purpose_mp_permits, with the canonical revoked purposes (a RESTRICT) taken out of the MP. */
bool purpose_mp_permits_unrevoked(const char *mp, const char *revoked, char *const *sp, uint32_t sp_count);

/* True when a canonical set and a sorted set share a purpose; "*" shares one with any
 * non-empty set. */
bool purpose_set_intersects(const char *canonical, char *const *sorted, uint32_t n);

/* True when two canonical sets share a purpose; "*" shares one with any non-empty set. */
bool purpose_sets_intersect(const char *a, const char *b);

/* purpose_set_expand for one DAP-SP value. Returns NULL with *num_results 0 for an
 * empty value or on failure. */
char **parse_purpose_filter(const char *filter, uint32_t *num_results);

/* Duplicate a single parsed purpose string for long-term storage in a subscription's
 * purpose-filter list. Returns a newly-allocated, NUL-terminated copy (caller owns) or
 * NULL on allocation failure. */
char *purpose_filter_store_dup(const char *purpose);

#endif
