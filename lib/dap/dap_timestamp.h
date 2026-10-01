/* dap_timestamp.h */
#ifndef DAP_TIMESTAMP_H
#define DAP_TIMESTAMP_H

#include <stddef.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Render a receipt time as the DAP-Timestamp property value: decimal seconds since
 * the epoch, NUL-terminated, written into buf. This is the reference timestamp the
 * broker stamps on every received message and exposes to subscribers as the
 * MOSQ_DAP_TIMESTAMP_KEY user property.
 *
 * Returns 0 on success, non-zero on a bad argument (NULL buf or zero length) or
 * when buf is too small. On failure the buffer contents are unspecified, but never
 * written past buflen.
 */
int dap_timestamp_format(time_t t, char *buf, size_t buflen);

#ifdef __cplusplus
}
#endif

#endif /* DAP_TIMESTAMP_H */
