#' Class RabbitConn.
#'
#' @useDynLib Rrabbit
#' @importFrom Rcpp evalCpp
#' @export
RabbitConnection <- setClass("RabbitConnection",
	slots = c(
		ptr = "externalptr"
	)
)

#' Constructor mqDial
#'
#' @export
mqDial <- function(hostname, port, username = "guest", password = "guest") {
	conn <- new("RabbitConnection",
		ptr = open_conn(hostname, port, username, password)
	)
	return(conn)
}

#' @export
setGeneric(name = "mqCloseConn",
	def = function(conn) { standardGeneric("mqCloseConn") }
)

setMethod(f = "mqCloseConn", signature = "RabbitConnection", definition = function(conn) {
	close_conn(conn@ptr)
	TRUE
})

#' @export
setGeneric(name = "mqOpenChan",
	def = function(conn, id) { standardGeneric("mqOpenChan") }
)

setMethod(f = "mqOpenChan", signature = "RabbitConnection", definition = function(conn, id) {
	open_channel(conn@ptr, id)
	chan <- new("RabbitChannel",
		conn = conn,
		id = id
	)
	return(chan)
})

#' Class RabbitChannel.
#'
#' @export
RabbitChannel <- setClass("RabbitChannel",
	slots = c(
		conn = "RabbitConnection",
		id = "numeric"
	)
)

#' @export
setGeneric(name = "mqCloseChan",
	def = function(chan) { standardGeneric("mqCloseChan") }
)

setMethod(f = "mqCloseChan", signature = "RabbitChannel", definition = function(chan) {
	close_channel(chan@conn@ptr, chan@id)
	TRUE
})

#' Persistent is TRUE or FALSE.
#'
#' @export
setGeneric(name = "mqPublish",
	def = function(chan, exchange, key, body, persistent = FALSE) { standardGeneric("mqPublish") }
)

setMethod(f = "mqPublish", signature = "RabbitChannel", definition = function(chan, exchange, key, body, persistent) {
	deliveryMode = 1
	if (persistent) {
		deliverMode = 2
	}
	publish_string(chan@conn@ptr, chan@id, exchange, key, body, deliveryMode)
	TRUE
})

#' @export
setGeneric(name = "mqDeclareQueue",
	def = function(chan, queuename) { standardGeneric("mqDeclareQueue") }
)

setMethod(f = "mqDeclareQueue", signature = "RabbitChannel", definition = function(chan, queuename) {
	declare_queue(chan@conn@ptr, chan@id, queuename)
	q <- new("RabbitQueue",
		chan = chan,
		name = queuename
	)
	return(q)
})

#' Class RabbitQueue
#'
#' @export
RabbitQueue <- setClass("RabbitQueue",
	slots = c(
		chan = "RabbitChannel",
		name = "character"
	)
)

#' @export
setGeneric(name = "mqStartConsuming",
	def = function(queue) { standardGeneric("mqStartConsuming") }
)

setMethod(f = "mqStartConsuming", signature = "RabbitQueue", definition = function(queue) {
	start_consuming(queue@chan@conn@ptr, queue@chan@id, queue@name)
	TRUE
})

#' @export
setGeneric(name = "mqConsumeMessage",
	def = function(queue) { standardGeneric("mqConsumeMessage") }
)

setMethod(f = "mqConsumeMessage", signature = "RabbitQueue", definition = function(queue) {
	return(consume_message(queue@chan@conn@ptr))
})
