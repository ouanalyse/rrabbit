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
		connPtr = conn@ptr,
		id = id
	)
	return(chan)
})

#' Class RabbitChan.
#'
#' @export
RabbitChannel <- setClass("RabbitChannel",
	slots = c(
		connPtr = "externalptr",
		id = "numeric"
	)
)

#' @export
setGeneric(name = "mqCloseChan",
	def = function(chan) { standardGeneric("mqCloseChan") }
)

setMethod(f = "mqCloseChan", signature = "RabbitChannel", definition = function(chan) {
	close_channel(chan@connPtr, chan@id)
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
	publish_string(chan@connPtr, chan@id, exchange, key, body, deliveryMode)
	TRUE
})

#' @export
setGeneric(name = "mqListenForever",
	def = function(chan, queuename) { standardGeneric("mqListenForever") }
)

setMethod(f = "mqListenForever", signature = "RabbitChannel", definition = function(chan, queuename) {
	listen_forever(chan@connPtr, chan@id, queuename)
	TRUE
})
