#ifndef PURPOSE_FILTERS_H
#define PURPOSE_FILTERS_H

#include <stdint.h>

#include "config.h"

typedef struct 
{
    char *str;
    bool ended;
} mosquitto_pf_expansion;

uint32_t parse_one_level(const char *level, char ***out_terms);
mosquitto_pf_expansion *combine_expansions(mosquitto_pf_expansion *old_list, uint32_t old_count,
                                       char **terms, uint32_t term_count,
                                       uint32_t *out_count);
char **parse_purpose_filter(const char *filter, uint32_t *num_results);

/* Duplicate a single parsed purpose string for long-term storage in a subscription's
 * purpose-filter list. Returns a newly-allocated, NUL-terminated copy (caller owns) or
 * NULL on allocation failure. Split out of handle__subscribe so the allocation sizing is
 * unit-testable: the inline version undersized the buffer by one byte (strlen instead of
 * strlen+1) and overflowed when copying the terminator. */
char *purpose_filter_store_dup(const char *purpose);

/* §4 purpose compatibility under match-any semantics. Both the message-purpose mp
 * and each subscription purpose-filter in sp_filters may carry several alternative
 * filters joined by '|'. Returns true when some '|'-separated filter of mp equals
 * some '|'-separated filter of any sp_filters entry, or when an sp_filters entry is
 * "*" (allow-all). Returns false for a NULL/empty mp or an empty SP set. */
bool purpose_filter_mp_matches_sp(const char *mp, char *const *sp_filters, uint32_t sp_count);

#endif
