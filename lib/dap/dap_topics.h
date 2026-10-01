/* dap_topics.h */
#ifndef DAP_TOPICS_H
#define DAP_TOPICS_H

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * True when topic belongs to the MQTT-DAP operation-system / control plane:
 * the purpose-management topics ($DAP/MP_reg, $DAP/SP_reg, $DAP/purpose_management),
 * the operation status bus ($OSYS), and the operation request/notification topics
 * (OR, ON, ORS, ONP) either bare or keyed as "<base>/<client-id>".
 *
 * These topics carry the framework's own traffic, so they are exempt from the
 * subscribe-time SP requirement (paper 4.3) and the publish-time MP requirement
 * (paper 4.3): a client must be able to use them without declaring an SP/MP.
 * NULL is not a system topic.
 */
bool dap_is_op_system_topic(const char *topic);

#ifdef __cplusplus
}
#endif

#endif /* DAP_TOPICS_H */
