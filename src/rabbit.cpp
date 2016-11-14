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

static void dump_row(long count, int numinrow, int *chs) {
	int i;

	printf("%08lX:", count - numinrow);
	if (numinrow > 0) {
		for (i = 0; i < numinrow; i++) {
			if (i == 8) {
				printf(" :");
			}
			printf(" %02X", chs[i]);
		}
		for (i = numinrow; i < 16; i++) {
			if (i == 8) {
				printf(" :");
			}
			printf("   ");
		}
		printf("  ");
		for (i = 0; i < numinrow; i++) {
			if (isprint(chs[i])) {
				printf("%c", chs[i]);
			} else {
				printf(".");
			}
		}
	}
	printf("\n");
}

static int rows_eq(int *a, int *b) {
	int i;

	for (i=0; i < 16; i++) {
		if (a[i] != b[i]) {
			return 0;
		}
	}
	return 1;
}

void amqp_dump(void const *buffer, size_t len) {
	unsigned char *buf = (unsigned char *) buffer;
	long count = 0;
	int numinrow = 0;
	int chs[16];
	int oldchs[16] = {0};
	int showed_dots = 0;
	size_t i;

	for (i = 0; i < len; i++) {
		int ch = buf[i];

		if (numinrow == 16) {
			int j;

			if (rows_eq(oldchs, chs)) {
				if (!showed_dots) {
					showed_dots = 1;
					printf("          .. .. .. .. .. .. .. .. : .. .. .. .. .. .. .. ..\n");
				}
			} else {
				showed_dots = 0;
				dump_row(count, numinrow, chs);
			}

			for (j = 0; j < 16; j++) {
				oldchs[j] = chs[j];
			}
			numinrow = 0;
		}

		count++;
		chs[numinrow++] = ch;
	}

	dump_row(count, numinrow, chs);
	if (numinrow != 0) {
		printf("%08lX:\n", count);
	}
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
	std::string exchange, std::string key, std::string body,  int deliveryMode) {

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
		0,
		0,
		&props,
		amqp_cstring_bytes(body.c_str())
	);
	die_on_error(st, "publishing message");
}

// [[Rcpp::export]]
void listen_forever(Rcpp::XPtr<amqp_connection_state_t_> conn, int chan_id, std::string queuename) {
	amqp_queue_declare_ok_t *r = amqp_queue_declare((amqp_connection_state_t) conn, chan_id, amqp_cstring_bytes(queuename.c_str()), 0, 0, 0, 1, amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "declaring queue");

	amqp_basic_consume((amqp_connection_state_t) conn, chan_id, amqp_cstring_bytes(queuename.c_str()), amqp_empty_bytes, 0, 0, 0, amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply((amqp_connection_state_t) conn), "consuming");

	for (;;) {
		amqp_rpc_reply_t res;
		amqp_envelope_t envelope;

		amqp_maybe_release_buffers((amqp_connection_state_t) conn);
		res = amqp_consume_message((amqp_connection_state_t) conn, &envelope, NULL, 0);
		if (AMQP_RESPONSE_NORMAL != res.reply_type) {
			break;
		}

		printf("Delivery %u, exchange %.*s routingkey %.*s\n",
					 (unsigned) envelope.delivery_tag,
					 (int) envelope.exchange.len, (char *) envelope.exchange.bytes,
					 (int) envelope.routing_key.len, (char *) envelope.routing_key.bytes);

		if (envelope.message.properties._flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
			printf("Content-type: %.*s\n",
						 (int) envelope.message.properties.content_type.len,
						 (char *) envelope.message.properties.content_type.bytes);
		}
		printf("----\n");
		amqp_dump(envelope.message.body.bytes, envelope.message.body.len);
		amqp_destroy_envelope(&envelope);
	}
}
