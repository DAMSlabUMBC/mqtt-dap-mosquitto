#ifndef DAP_INTAKE_H
#define DAP_INTAKE_H

#include <stdbool.h>

struct mosquitto;
struct dap_receipt;

/*
 * Prioritized intake (paper 5.2(ii)). While a pass of the event loop reads
 * packets, data PUBLISHes are set aside and the packets that change state -
 * SUBSCRIBE, PUBLISH to $MP_REG and to $OP_SYS - are handled as they are read.
 * The data is handled when the pass ends, in the order it was read, under the
 * receipt stamp it was given on reading. A client's own packets keep their order,
 * except that a QoS 0 PUBLISH to $MP_REG or $OP_SYS without a topic alias goes
 * ahead of the client's own data. The epoll loop reads clients showing a state
 * change first, finishes their hold, then handles other clients' data as it is read.
 */

/* Stamp a packet's receipt: the broker's total order over the PUBLISHes it reads. */
void dap_receipt__stamp(struct dap_receipt *receipt);

/* Start and finish a pass; finishing handles the PUBLISHes set aside. */
void dap_intake__begin(void);
void dap_intake__end(void);

/* True if the client's next read may complete a state-changing packet. */
bool dap_intake__peek_state(struct mosquitto *context);

/* Handle the PUBLISH in context->in_packet, or set it aside during a pass. */
int dap_intake__publish(struct mosquitto *context);

/* Handle a client's set-aside PUBLISHes now, before its next packet or its
 * disconnection. Returns the error of the first that fails; the rest are dropped. */
int dap_intake__flush(struct mosquitto *context);

/* Drop everything set aside, at shutdown. */
void dap_intake__cleanup(void);

#endif
