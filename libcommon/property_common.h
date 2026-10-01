/*
Copyright (c) 2018-2021 Roger Light <roger@atchoo.org>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License 2.0
and Eclipse Distribution License v1.0 which accompany this distribution.

The Eclipse Public License is available at
   https://www.eclipse.org/legal/epl-2.0/
and the Eclipse Distribution License is available at
  http://www.eclipse.org/org/documents/edl-v10.php.

SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause

Contributors:
   Roger Light - initial implementation and documentation.
*/
#ifndef PROPERTY_COMMON_H
#define PROPERTY_COMMON_H

#include <stdbool.h>
#include <stdint.h>

#include "mosquitto.h"

struct mqtt__string {
	char *v;
	uint16_t len;
};

struct mqtt5__property {
	struct mqtt5__property *next;
	union {
		uint8_t i8;
		uint16_t i16;
		uint32_t i32;
		uint32_t varint;
		struct mqtt__string bin;
		struct mqtt__string s;
	} value;
	struct mqtt__string name;
	int32_t identifier;
	uint8_t property_type;
	bool client_generated;
};

struct dap__op_property {
	bool op_present;
	char *op_id;

	/* DAP-OpTFs / DAP-OpPFs / DAP-OpClients: the operation's comma-separated topic,
	 * purpose and client filter lists. */
	char *op_topic_filters;
	char *op_purpose_filters;
	char *op_client_filters;
	/* DAP-OpBefore / DAP-OpAfter: the operation's receipt-time bounds (decimal
	 * seconds; 0 = unbounded), used by the relevance query. */
	time_t op_before;
	time_t op_after;
	char *correlation_data;
	uint16_t correlation_data_len;
	char *response_topic;

	/* Inbound operation status notification (subscriber -> broker). DAP-Status marks a
	 * publish on $OSYS as a notification rather than a request; DAP-OpId names the
	 * operation being responded to, DAP-Reason is optional, DAP-ClientID is the
	 * responding subscriber. */
	char *op_status;
	char *op_reason;
	char *op_client_id ;
	uint64_t op_id_num;
	bool found_op_id_num;
};

#endif
