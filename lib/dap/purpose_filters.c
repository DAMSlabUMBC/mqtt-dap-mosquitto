#include "config.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "util_mosq.h"

#include "purpose_filters.h"

/* A partially expanded purpose; ended once a '.' term closes it at its parent level. */
struct pf_expansion {
    char *str;
    bool ended;
};

static void pf__free_expansions(struct pf_expansion *list, uint32_t count)
{
    for(uint32_t i = 0; i < count; i++){
        mosquitto_FREE(list[i].str);
    }
    mosquitto_FREE(list);
}

/* Append str (owned) to a growable string array. Returns 0 on success; str is freed on failure. */
static int pf__push(char ***set, uint32_t *count, char *str)
{
    char **grown;

    if(!str) return 1;
    grown = mosquitto_realloc(*set, (*count + 1) * sizeof(char *));
    if(!grown){
        mosquitto_FREE(str);
        return 1;
    }
    *set = grown;
    (*set)[(*count)++] = str;
    return 0;
}

/* The terms of one level: the members of a {a,b} set, or the level itself. */
static int pf__level_terms(const char *level, size_t len, char ***terms, uint32_t *count)
{
    *terms = NULL;
    *count = 0;
    if(len >= 2 && level[0] == '{' && level[len-1] == '}'){
        const char *p = level + 1;
        const char *end = level + len - 1;
        while(p <= end){
            const char *comma = memchr(p, ',', (size_t)(end - p));
            const char *term_end = comma ? comma : end;
            if(term_end > p && pf__push(terms, count, mosquitto_strndup(p, (size_t)(term_end - p)))){
                return 1;
            }
            p = term_end + 1;
        }
        return 0;
    }
    return pf__push(terms, count, mosquitto_strndup(level, len));
}

static void pf__free_terms(char **terms, uint32_t count)
{
    for(uint32_t i = 0; i < count; i++){
        mosquitto_FREE(terms[i]);
    }
    mosquitto_FREE(terms);
}

/* Expand one filter (no top-level separators) into the purposes it describes,
 * appending them to out. */
static int pf__expand_filter(const char *filter, size_t len, char ***out, uint32_t *out_count)
{
    struct pf_expansion *exp = mosquitto_calloc(1, sizeof(*exp));
    uint32_t exp_count = 1;
    struct pf_expansion *next = NULL;
    uint32_t next_count = 0;
    char **terms = NULL;
    uint32_t term_count = 0;
    const char *p = filter;
    const char *end = filter + len;
    int rc = MOSQ_ERR_NOMEM;

    if(!exp) return MOSQ_ERR_NOMEM;
    exp[0].str = mosquitto_strdup("");
    if(!exp[0].str) goto cleanup;

    while(p < end){
        const char *slash = memchr(p, '/', (size_t)(end - p));
        const char *level_end = slash ? slash : end;

        if(level_end == p){
            p = level_end + 1;
            continue; /* empty level */
        }
        if(pf__level_terms(p, (size_t)(level_end - p), &terms, &term_count)) goto cleanup;
        for(uint32_t i = 0; i < exp_count; i++){
            for(uint32_t j = 0; j < (exp[i].ended ? 1 : term_count); j++){
                struct pf_expansion *grown;
                char *str;

                if(*out_count + next_count >= PURPOSE_SET_MAX){
                    rc = MOSQ_ERR_INVAL;
                    goto cleanup;
                }
                grown = mosquitto_realloc(next, (next_count + 1) * sizeof(*next));
                if(!grown) goto cleanup;
                next = grown;
                if(exp[i].ended || !strcmp(terms[j], ".")){
                    /* '.' permits the parent itself; an ended purpose is carried through. */
                    str = mosquitto_strdup(exp[i].str);
                    next[next_count].ended = true;
                }else if(exp[i].str[0] == '\0'){
                    str = mosquitto_strdup(terms[j]);
                    next[next_count].ended = false;
                }else{
                    size_t n = strlen(exp[i].str) + 1 + strlen(terms[j]) + 1;
                    str = mosquitto_malloc(n);
                    if(str) snprintf(str, n, "%s/%s", exp[i].str, terms[j]);
                    next[next_count].ended = false;
                }
                if(!str) goto cleanup;
                next[next_count++].str = str;
            }
        }
        pf__free_terms(terms, term_count);
        terms = NULL;
        term_count = 0;
        pf__free_expansions(exp, exp_count);
        exp = next;
        exp_count = next_count;
        next = NULL;
        next_count = 0;
        p = level_end + 1;
    }

    for(uint32_t i = 0; i < exp_count; i++){
        if(exp[i].str[0] != '\0'){
            char *str = exp[i].str;
            exp[i].str = NULL;
            if(pf__push(out, out_count, str)) goto cleanup;
        }
    }
    rc = MOSQ_ERR_SUCCESS;

cleanup:
    pf__free_terms(terms, term_count);
    pf__free_expansions(next, next_count);
    pf__free_expansions(exp, exp_count);
    return rc;
}

static int pf__strcmp(const void *a, const void *b)
{
    return strcmp(*(char *const *)a, *(char *const *)b);
}

void purpose_set_normalize(char **set, uint32_t *count)
{
    uint32_t kept = 0;

    if(!set || *count == 0) return;
    qsort(set, *count, sizeof(char *), pf__strcmp);
    for(uint32_t i = 0; i < *count; i++){
        if(kept > 0 && !strcmp(set[kept-1], set[i])){
            mosquitto_FREE(set[i]);
        }else{
            set[kept++] = set[i];
        }
    }
    *count = kept;
}

int purpose_set_expand(const char *filters, char ***purposes, uint32_t *count)
{
    const char *p = filters;
    int depth = 0;

    *purposes = NULL;
    *count = 0;
    if(!filters) return 0;

    /* Split on '|' or ',' outside braces; ',' inside braces separates set members. */
    for(const char *c = filters; ; c++){
        if(*c == '{'){
            depth++;
        }else if(*c == '}' && depth > 0){
            depth--;
        }else if(*c == '\0' || (depth == 0 && (*c == '|' || *c == ','))){
            int rc = (c > p) ? pf__expand_filter(p, (size_t)(c - p), purposes, count) : MOSQ_ERR_SUCCESS;
            if(rc){
                purpose_set_free(*purposes, *count);
                *purposes = NULL;
                *count = 0;
                return rc;
            }
            if(*c == '\0') break;
            p = c + 1;
        }
    }
    purpose_set_normalize(*purposes, count);
    return 0;
}

void purpose_set_free(char **purposes, uint32_t count)
{
    if(!purposes) return;
    for(uint32_t i = 0; i < count; i++){
        mosquitto_FREE(purposes[i]);
    }
    mosquitto_FREE(purposes);
}

int purpose_filter_canonical(const char *filters, char **canonical)
{
    char **set;
    uint32_t count;
    size_t len = 1;
    char *q;
    int rc;

    *canonical = NULL;
    rc = purpose_set_expand(filters, &set, &count);
    if(rc) return rc;
    for(uint32_t i = 0; i < count; i++){
        if(!strcmp(set[i], "*")){
            purpose_set_free(set, count);
            *canonical = mosquitto_strdup("*");
            return *canonical ? MOSQ_ERR_SUCCESS : MOSQ_ERR_NOMEM;
        }
        len += strlen(set[i]) + 1;
    }
    *canonical = mosquitto_malloc(len);
    if(!*canonical){
        purpose_set_free(set, count);
        return MOSQ_ERR_NOMEM;
    }
    q = *canonical;
    for(uint32_t i = 0; i < count; i++){
        size_t n = strlen(set[i]);
        if(i > 0) *q++ = '|';
        memcpy(q, set[i], n);
        q += n;
    }
    *q = '\0';
    purpose_set_free(set, count);
    return MOSQ_ERR_SUCCESS;
}

char **parse_purpose_filter(const char *filter, uint32_t *num_results)
{
    char **set;

    if(purpose_set_expand(filter, &set, num_results)){
        *num_results = 0;
        return NULL;
    }
    return set;
}

char *purpose_filter_store_dup(const char *purpose)
{
    /* +1 for the NUL terminator: strcpy writes strlen(purpose)+1 bytes. Omitting it
     * overflowed the allocation by one byte (the original handle__subscribe defect). */
    char *filter = mosquitto_malloc(strlen(purpose) + 1);
    if(!filter)
    {
        return NULL;
    }
    strcpy(filter, purpose);
    return filter;
}

/* A cursor over the '|'-separated purposes of a canonical set. */
struct pf_cursor {
    const char *p;
    const char *tok;
    size_t len;
};

static bool pf__next(struct pf_cursor *c)
{
    const char *bar;

    if(!c->p || *c->p == '\0') return false;
    bar = strchr(c->p, '|');
    c->tok = c->p;
    c->len = bar ? (size_t)(bar - c->p) : strlen(c->p);
    c->p = bar ? bar + 1 : NULL;
    return true;
}

/* strcmp order between a canonical token and a purpose. */
static int pf__tokcmp(const struct pf_cursor *c, const char *purpose)
{
    size_t plen = strlen(purpose);
    int r = memcmp(c->tok, purpose, c->len < plen ? c->len : plen);
    if(r) return r;
    return (c->len > plen) - (c->len < plen);
}

/* True when every purpose of the sorted set is in the canonical set (or the canonical
 * set is "*"); with exclude, also none of them may be in exclude. */
static bool pf__subset(const char *canonical, char *const *sorted, uint32_t n, bool exclude)
{
    struct pf_cursor c = {canonical, NULL, 0};
    bool have = pf__next(&c);

    for(uint32_t i = 0; i < n; i++){
        while(have && pf__tokcmp(&c, sorted[i]) < 0){
            have = pf__next(&c);
        }
        if(have && pf__tokcmp(&c, sorted[i]) == 0){
            if(exclude) return false;
        }else if(!exclude){
            return false;
        }
    }
    return true;
}

bool purpose_mp_permits(const char *mp, char *const *sp, uint32_t sp_count)
{
    if(!mp || !sp || sp_count == 0) return false;
    if(!strcmp(mp, "*")) return true;
    return pf__subset(mp, sp, sp_count, false);
}

bool purpose_mp_permits_unrevoked(const char *mp, const char *revoked, char *const *sp, uint32_t sp_count)
{
    if(!purpose_mp_permits(mp, sp, sp_count)) return false;
    if(!revoked || revoked[0] == '\0') return true;
    if(!strcmp(revoked, "*")) return false;
    return pf__subset(revoked, sp, sp_count, true);
}

bool purpose_set_intersects(const char *canonical, char *const *sorted, uint32_t n)
{
    if(!canonical || canonical[0] == '\0' || !sorted || n == 0) return false;
    if(!strcmp(canonical, "*")) return true;
    return !pf__subset(canonical, sorted, n, true);
}

bool purpose_sets_intersect(const char *a, const char *b)
{
    struct pf_cursor ca = {a, NULL, 0};
    struct pf_cursor cb = {b, NULL, 0};
    bool have_a, have_b;

    if(!a || !b || a[0] == '\0' || b[0] == '\0') return false;
    if(!strcmp(a, "*") || !strcmp(b, "*")) return true;

    have_a = pf__next(&ca);
    have_b = pf__next(&cb);
    while(have_a && have_b){
        size_t n = ca.len < cb.len ? ca.len : cb.len;
        int r = memcmp(ca.tok, cb.tok, n);
        if(!r) r = (ca.len > cb.len) - (ca.len < cb.len);
        if(!r) return true;
        if(r < 0){
            have_a = pf__next(&ca);
        }else{
            have_b = pf__next(&cb);
        }
    }
    return false;
}
