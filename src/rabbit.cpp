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
Rcpp::XPtr<amqp_connection_state_t_> open_conn(std::string hostname, int port, std::string username, std::string password) {
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

	die_on_amqp_error(amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, username.c_str(), username.c_str()), "logging in");

	// Note that amqp_connection_state_t is really amqp_connection_state_t_*. As the Rcpp::XPtr
	// template needs a pointer, cast (amqp_connection_state_t( to (amqp_connection_state_t_*).
	return Rcpp::XPtr<amqp_connection_state_t_>((amqp_connection_state_t_*) conn, true);
}

// [[Rcpp::export]]
void close_conn(Rcpp::XPtr<amqp_connection_state_t_> conn) {
	die_on_amqp_error(amqp_connection_close((amqp_connection_state_t) conn, AMQP_REPLY_SUCCESS), "closing connection");
	die_on_error(amqp_destroy_connection((amqp_connection_state_t) conn), "ending connection");
}

// [[Rcpp::export]]
void open_channel(Rcpp::XPtr<amqp_connection_state_t_> conn, int id) {
	amqp_channel_open(conn, id);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "opening channel");
}

// [[Rcpp::export]]
void close_channel(Rcpp::XPtr<amqp_connection_state_t_> conn, int chan_id) {
	die_on_amqp_error(amqp_channel_close((amqp_connection_state_t) conn, chan_id, AMQP_REPLY_SUCCESS), "closing channel");
}

// [[Rcpp::export]]
void publish_string(Rcpp::XPtr<amqp_connection_state_t_> conn, int chan_id,
	std::string exchange, std::string key, std::string body, int deliveryMode, bool mandatory, bool immediate) {

	// deliveryMode:
	//   1 - non-persistent message
	//   2 - persistent message

	int st;

	amqp_basic_properties_t props;
	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
	props.content_type = amqp_cstring_bytes("text/plain");
	props.delivery_mode = (amqp_delivery_mode_enum) deliveryMode;

	st = amqp_basic_publish(
		(amqp_connection_state_t) conn,
		chan_id,
		amqp_cstring_bytes(exchange.c_str()),
		amqp_cstring_bytes(key.c_str()),
		mandatory,
		immediate,
		&props,
		amqp_cstring_bytes(body.c_str())
	);
	die_on_error(st, "publishing message");
}

// [[Rcpp::export]]
void declare_exchange(Rcpp::XPtr<amqp_connection_state_t_> conn, int chan_id, std::string name, std::string type, bool durable, bool auto_delete) {
	amqp_exchange_declare(/*(amqp_connection_state_t)*/ conn, chan_id, amqp_cstring_bytes(name.c_str()), amqp_cstring_bytes(type.c_str()),
		0 /* passive */, durable, auto_delete, 0 /* internal */, amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "declaring exchange");
}

// [[Rcpp::export]]
std::string declare_queue(Rcpp::XPtr<amqp_connection_state_t_> conn, int chan_id, std::string queuename, bool durable, bool exclusive, bool auto_delete) {
	amqp_bytes_t name = amqp_empty_bytes;
	if (queuename != "") {
		name = amqp_cstring_bytes(queuename.c_str());
	}
	// Declaring a passive queue means that the queue must already exist. Useful for testing whether a particular
	// queue was declared already. This is considered unnecessary for now.
	amqp_queue_declare_ok_t *r = amqp_queue_declare((amqp_connection_state_t) conn, chan_id, name, 0 /* passive */, durable, exclusive, auto_delete, amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "declaring queue");

	if (queuename == "") {
		name = amqp_bytes_malloc_dup(r->queue);
		if (name.bytes == NULL) {
			Rcpp::stop("out of memory while copying queue name");
		}
		return std::string ((char *) name.bytes, name.len);
	}
	return queuename;
}

// [[Rcpp::export]]
void bind_queue(Rcpp::XPtr<amqp_connection_state_t_> conn, int chan_id, std::string queue, std::string exchange, std::string bindkey) {
	amqp_queue_bind((amqp_connection_state_t) conn, chan_id, amqp_cstring_bytes(queue.c_str()), amqp_cstring_bytes(exchange.c_str()),
		amqp_cstring_bytes(bindkey.c_str()), amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "binding queue");
}

// [[Rcpp::export]]
void start_consuming(Rcpp::XPtr<amqp_connection_state_t_> conn, int chan_id, std::string queuename) {
	amqp_basic_consume((amqp_connection_state_t) conn, chan_id, amqp_cstring_bytes(queuename.c_str()), amqp_empty_bytes, 0, 0, 0, amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply((amqp_connection_state_t) conn), "consuming");
}

// void cancel_consuming() -> amqp_basic_cancel

// [[Rcpp::export]]
std::string consume_message(Rcpp::XPtr<amqp_connection_state_t_> conn) {
	amqp_rpc_reply_t res;
	amqp_envelope_t envelope;

	amqp_maybe_release_buffers((amqp_connection_state_t) conn);
	res = amqp_consume_message((amqp_connection_state_t) conn, &envelope, NULL, 0);
	if (AMQP_RESPONSE_NORMAL != res.reply_type) {
		Rcpp::stop("failed to consume a message");
	}

	std::string msg ((char *) envelope.message.body.bytes, envelope.message.body.len);
	amqp_destroy_envelope(&envelope);
	return msg;
}
