#include <stdio.h>
#include <stdbool.h>/* dap_send_verify.c */

#include <mosquitto/defs.h>
#include "dap/dap_send_verify.h"

#include "dap/dap_subscription_queues.h" /* struct dap_stamped_msg */

enum dap_send_verdict dap_verify_for_send(const struct dap_stamped_msg *stamped,
                                          uint32_t current_mp_version,
                                          uint32_t current_sp_version,
                                          enum dap_op_action current_op_action,
                                          bool is_op,
                                          const char *topic,
                                          const char *recv_client_id)
{
    // TODO: Handle purposes in will/retained
    if(stamped != NULL)
    {
        if(!is_op)
        {
            /* A retained message that was never stamped flows through untouched. */
            if (stamped == NULL) {
                return DAP_SEND_PASS;
            }

            /* A DELETE drops the message outright, regardless of version freshness. */
            if (current_op_action == DAP_OP_ACTION_DROP) {
                return DAP_SEND_DROP_DELETE;
            }

            /* A stale version means policy changed since stamping: re-verify (MP first). */
            if (stamped->mp_version != current_mp_version) {
                return DAP_SEND_FAIL_MP_VER;
            }
            if (stamped->sp_version != current_sp_version) {
                return DAP_SEND_FAIL_SP_VER;
            }

            /* A RESTRICT re-verifies the message against the purposes it revoked. */
            if (current_op_action == DAP_OP_ACTION_RESTRICT) {
                return DAP_SEND_FAIL_RESTRICT;
            }
        }
        else
        {
            char* temp = (char*)malloc(strlen(MOSQ_DAP_TOPIC_ONP) + strlen(recv_client_id) + 2);
            strcpy(temp, MOSQ_DAP_TOPIC_ONP);
            strcat(temp, "/");
            strcat(temp, recv_client_id);
            bool is_notif_topic = (strcmp(topic, temp) == 0);
            free(temp);

            temp = (char*)malloc(strlen(MOSQ_DAP_TOPIC_ORS) + strlen(recv_client_id) + 2);
            strcpy(temp, MOSQ_DAP_TOPIC_ORS);
            strcat(temp, "/");
            strcat(temp, recv_client_id);
            bool is_req_topic = (strcmp(topic, temp) == 0);
            free(temp);

            if(!is_notif_topic && !is_req_topic)
            {
                return DAP_SEND_DROP_INVALID_OP_TOPIC;
            }
        }
    }

    return DAP_SEND_PASS;
}

enum dap_send_disposition dap_send_decide(bool stamped_msg,
                                          bool is_holding,
                                          uint64_t pending_id,
                                          uint64_t this_id,
                                          enum dap_send_verdict verdict,
                                          bool perform_full_check)
{
    enum dap_send_disposition disp = DAP_DISP_UNKOWN;

    /* If the quick verification shows delete, we don't need to process anything else */
	if(verdict == DAP_SEND_DROP_DELETE || verdict == DAP_SEND_DROP_INVALID_OP_TOPIC)
	{
		disp = DAP_DISP_DROP;
	}

    else if(!stamped_msg)
    {
        disp = DAP_DISP_DELIVER;
    }

	/* Otherwise if we're not allowed to send this message right now, skip it*/
	else if(is_holding && (this_id != pending_id))
	{
		disp = DAP_DISP_SKIP;
	}

	/* If the quick verification did NOT pass, we need to do full verification,
	do this now if requested, otherwise we bump and delay the check */
	else if(verdict == DAP_SEND_FAIL_MP_VER || verdict == DAP_SEND_FAIL_SP_VER || verdict == DAP_SEND_FAIL_RESTRICT)
	{
        disp = perform_full_check ? DAP_DISP_RECALCULATE : DAP_DISP_BUMP;
	}
	
	else if(verdict == DAP_SEND_PASS)
	{
		disp = DAP_DISP_DELIVER;
	}

    return disp;
}
