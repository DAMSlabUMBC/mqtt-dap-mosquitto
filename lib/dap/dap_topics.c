/* dap_topics.c */

#include <string.h>

#include "dap_topics.h"
#include "mosquitto/defs.h"

/* True when topic equals base, or begins with "base/" (a keyed sub-topic).
 * A bare prefix that continues with other characters (e.g. "ORchard" vs "OR")
 * does not match. */
static bool topic_in_namespace(const char *topic, const char *base)
{
    size_t bl = strlen(base);
    if(strncmp(topic, base, bl) != 0) return false;
    return topic[bl] == '\0' || topic[bl] == '/';
}

bool dap_is_op_system_topic(const char *topic)
{
    if(!topic) return false;

    /* Purpose-management control plane: $DAP/MP_reg/, $DAP/SP_reg/,
     * $DAP/purpose_management all share the $DAP/ prefix. */
    if(!strncmp(topic, "$DAP/", 5)) return true;

    /* Operation status bus. */
    if(topic_in_namespace(topic, MOSQ_DAP_TOPIC_OSYS)) return true; /* $OSYS */

    /* Operation request/notification topics, bare or keyed as <base>/<id>. */
    if(topic_in_namespace(topic, MOSQ_DAP_TOPIC_ORS)) return true;  /* ORS */
    if(topic_in_namespace(topic, MOSQ_DAP_TOPIC_ONP)) return true;  /* ONP */

    return false;
}
