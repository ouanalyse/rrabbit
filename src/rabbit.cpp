/*
 * Version: MIT
 *
 * Portions created by Alan Antonuk are Copyright (c) 2012-2013
 * Alan Antonuk. All Rights Reserved.
 *
 * Portions created by VMware are Copyright (c) 2007-2012 VMware, Inc.
 * All Rights Reserved.
 *
 * Portions created by Tony Garnock-Jones are Copyright (c) 2009-2010
 * VMware, Inc. and Tony Garnock-Jones. All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <stdint.h>
#include "Rrabbit_types.h"

#include <R.h>
#include <Rinternals.h>
#include <Rcpp.h>

void die_on_error(int x, char const *context) {
	if (x < 0) {
		Rcpp::stop("%s: %s", context, amqp_error_string2(x));
	}
}

void die_on_amqp_error(amqp_rpc_reply_t x, char const *context) {
	switch (x.reply_type) {
	case AMQP_RESPONSE_NORMAL:
		return;
	case AMQP_RESPONSE_NONE:
		Rcpp::stop("%s: missing RPC reply type!", context);
		break;
	case AMQP_RESPONSE_LIBRARY_EXCEPTION:
		Rcpp::stop("%s: %s", context, amqp_error_string2(x.library_error));
		break;
	case AMQP_RESPONSE_SERVER_EXCEPTION:
		switch (x.reply.id) {
		case AMQP_CONNECTION_CLOSE_METHOD: {
			amqp_connection_close_t *m = (amqp_connection_close_t *) x.reply.decoded;
			Rcpp::stop("%s: server connection error %uh, message: %.*s",
				context,
				m->reply_code,
				(int) m->reply_text.len, (char *) m->reply_text.bytes);
			break;
		}
		case AMQP_CHANNEL_CLOSE_METHOD: {
			amqp_channel_close_t *m = (amqp_channel_close_t *) x.reply.decoded;
			Rcpp::stop("%s: server channel error %uh, message: %.*s",
				context,
				m->reply_code,
				(int) m->reply_text.len, (char *) m->reply_text.bytes);
			break;
		}
		default:
			Rcpp::stop("%s: unknown server error, method id 0x%08X", context, x.reply.id);
			break;
		}
		break;
	}
	fprintf(stderr, "rabbitmq-c: unknown error");
	exit(1);
}

// [[Rcpp::export]]
Rcpp::XPtr<amqp_connection_state_t_> open_conn(std::string hostname, int port) {
	int status;
	amqp_socket_t *socket = NULL;
	amqp_connection_state_t conn;

	conn = amqp_new_connection();

	socket = amqp_tcp_socket_new(conn);
	if (!socket) {
		Rcpp::stop("failed to create a TCP socket");
	}

	status = amqp_socket_open(socket, hostname.c_str(), port);
	if (status) {
		Rcpp::stop("failed to open a TCP socket");
	}

	die_on_amqp_error(amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest"), "logging in");
	amqp_channel_open(conn, 1);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "opening channel");

	// Note that amqp_connection_state_t is really amqp_connection_state_t_*. As the Rcpp::XPtr
	// template needs a pointer, cast (amqp_connection_state_t( to (amqp_connection_state_t_*).
	return Rcpp::XPtr<amqp_connection_state_t_>((amqp_connection_state_t_*) conn, true);
}

// [[Rcpp::export]]
void send_string(Rcpp::XPtr<amqp_connection_state_t_> conn, std::string body) {
	int st;
	char const *exchange;
	char const *routingkey;

	exchange = "amq.direct";
	routingkey = "test";

	amqp_basic_properties_t props;
	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
	props.content_type = amqp_cstring_bytes("text/plain");
	props.delivery_mode = 2; /* persistent delivery mode */

	st = amqp_basic_publish(
		(amqp_connection_state_t) conn,
		1,
		amqp_cstring_bytes(exchange),
		amqp_cstring_bytes(routingkey),
		0,
		0,
		&props,
		amqp_cstring_bytes(body.c_str())
	);
	die_on_error(st, "publishing message");
}

// [[Rcpp::export]]
void close_conn(Rcpp::XPtr<amqp_connection_state_t_> conn) {
	die_on_amqp_error(amqp_channel_close((amqp_connection_state_t) conn, 1, AMQP_REPLY_SUCCESS), "closing channel");
	die_on_amqp_error(amqp_connection_close((amqp_connection_state_t) conn, AMQP_REPLY_SUCCESS), "closing connection");
	die_on_error(amqp_destroy_connection((amqp_connection_state_t) conn), "ending connection");
}
