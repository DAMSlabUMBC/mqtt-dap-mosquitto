/* dap_timestamp.c */

#include <stdio.h>

#include "dap_timestamp.h"

int dap_timestamp_format(time_t t, char *buf, size_t buflen)
{
    if(!buf || buflen == 0) return 1;

    /* DAP-Timestamp is decimal seconds since the epoch. time_t is signed and may
     * be wider than long, so widen to long long for a portable format. */
    int n = snprintf(buf, buflen, "%lld", (long long)t);
    if(n < 0 || (size_t)n >= buflen) return 1;
    return 0;
}
