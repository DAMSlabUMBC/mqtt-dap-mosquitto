/* dap_metrics.h */
#ifndef DAP_METRICS_H
#define DAP_METRICS_H

#ifdef __cplusplus
extern "C" {
#endif

struct mosquitto__base_msg;

/*
 * Open the broker-side per-message metrics CSV at /tmp/dap_broker_metrics.csv,
 * write the header row, and retain the FILE* for subsequent log calls.
 * Returns 0 on success, non-zero on fopen failure.
 */
int dap_metrics_init(void);

/*
 * Write one CSV row describing a fully-resolved base_msg: receipt wall-clock
 * nanoseconds, publisher id, topic, matched leaf count, processing time
 * (CLOCK_MONOTONIC now minus the receipt monotonic stamp), and bump count.
 * Flushes after each row. No-op if init has not run or the file is closed.
 */
void dap_metrics_log_message(const struct mosquitto__base_msg *base_msg);

/*
 * Close the metrics file. Subsequent log calls become no-ops.
 */
void dap_metrics_close(void);

#ifdef __cplusplus
}
#endif

#endif /* DAP_METRICS_H */
