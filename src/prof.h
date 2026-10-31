/* Timing profile shared by the DAP and baseline brokers (temporary, for comparison runs). */
#ifndef PROF_H
#define PROF_H

#include <stdint.h>
#include <time.h>

enum prof_section {
	PROF_FANOUT,        /* sub__messages_queue */
	PROF_WRITE_SINGLE,  /* db__message_write_inflight_out_single */
	PROF_SEND_PUBLISH,  /* send__publish */
	PROF_PURPOSE,       /* DAP: purpose check per subscriber */
	PROF_RI,            /* DAP: register-info check per subscriber */
	PROF_STAMP,         /* DAP: stamp into subscription topic queue */
	PROF_CHECK_SEND,    /* DAP: send-path gate */
	PROF_DAP_LOOP,      /* DAP: per-loop deadline and op resend checks */
	PROF_ACK_RTT,       /* write to PUBACK for QoS 1 */
	PROF_MAX,
};

static inline uint64_t prof_now(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

void prof_add(enum prof_section s, uint64_t ns);
void prof_msg(uint64_t id, uint64_t recv, uint64_t handle, uint64_t fan_start, uint64_t fan_end, int subs);
void prof_delivery(uint64_t id, uint64_t recv, uint64_t enq, int during_fanout, uint64_t now);

extern int prof_in_fanout;     /* set while sub__messages_queue runs */
extern int prof_fanout_subs;   /* client messages queued by the current fan-out */
void prof_ack(const char *client_id, uint64_t rtt_ns);
void prof_tick(void);
void prof_close(void);

#endif
