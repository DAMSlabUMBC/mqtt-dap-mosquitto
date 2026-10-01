/* dap_metrics.c */

#include <stdio.h>
#include <time.h>

#include "../../src/mosquitto_broker_internal.h"
#include "dap_metrics.h"

static FILE *metrics_file = NULL;

int dap_metrics_init(void)
{
    if(metrics_file) return 0;
    metrics_file = fopen("/tmp/dap_broker_metrics.csv", "w");
    if(!metrics_file) return 1;
    fprintf(metrics_file,
            "recv_time_ns,publisher_id,topic,num_subs_matched,processing_time_ns,bump_count\n");
    fflush(metrics_file);
    return 0;
}

void dap_metrics_log_message(const struct mosquitto__base_msg *base_msg)
{
    if(!metrics_file || !base_msg) return;

    struct timespec now_mono;
    clock_gettime(CLOCK_MONOTONIC, &now_mono);
    uint64_t now_ns = (uint64_t)now_mono.tv_sec * 1000000000ULL + (uint64_t)now_mono.tv_nsec;
    uint64_t processing_ns = (base_msg->dap_recv_time_ns_mono > 0
                              && now_ns >= base_msg->dap_recv_time_ns_mono)
                             ? (now_ns - base_msg->dap_recv_time_ns_mono)
                             : 0;

    const char *publisher_id = base_msg->data.source_id ? base_msg->data.source_id : "";
    const char *topic = base_msg->data.topic ? base_msg->data.topic : "";

    fprintf(metrics_file, "%llu,%s,%s,%d,%llu,%d\n",
            (unsigned long long)base_msg->dap_recv_time_ns_wall,
            publisher_id,
            topic,
            base_msg->dap_subs_matched,
            (unsigned long long)processing_ns,
            base_msg->dap_bump_count);
    fflush(metrics_file);
}

void dap_metrics_close(void)
{
    if(!metrics_file) return;
    fclose(metrics_file);
    metrics_file = NULL;
}
