# helper-ws.R
# ::rtemis.server::
# 2026- EDG rtemis.org

# WebSocket client on base R sockets for the integration tests, with an
# `Origin` header of the test's choice. Each logical message is one binary
# WebSocket message; `encode_frame()` and `decode_frame()` read its content.

# Open a WebSocket connection; NULL when nothing accepts on `port`.
ws_open <- function(
  port,
  origin = "http://localhost:3000",
  header = "Origin"
) {
  con <- tryCatch(
    suppressWarnings(socketConnection(
      host = "127.0.0.1",
      port = port,
      blocking = FALSE,
      open = "r+b",
      timeout = 5
    )),
    error = function(e) NULL
  )
  if (is.null(con)) {
    return(NULL)
  }
  key <- openssl::base64_encode(openssl::rand_bytes(16L))
  request <- c(
    "GET / HTTP/1.1",
    paste0("Host: 127.0.0.1:", port),
    "Upgrade: websocket",
    "Connection: Upgrade",
    paste0("Sec-WebSocket-Key: ", key),
    "Sec-WebSocket-Version: 13",
    if (!is.null(origin)) paste0(header, ": ", origin),
    "",
    ""
  )
  writeBin(charToRaw(paste(request, collapse = "\r\n")), con)
  ws <- new.env(parent = emptyenv())
  ws[["con"]] <- con
  ws[["buffer"]] <- raw(0L)
  ws[["closed"]] <- FALSE
  # The response headers end at the first blank line.
  deadline <- Sys.time() + 5
  repeat {
    ws_fill(ws)
    at <- grepRaw("\r\n\r\n", ws[["buffer"]], fixed = TRUE)
    if (length(at) > 0L) {
      ws[["status"]] <- rawToChar(ws[["buffer"]][10:12])
      ws[["buffer"]] <- ws[["buffer"]][-seq_len(at + 3L)]
      return(ws)
    }
    if (Sys.time() > deadline) {
      close(con)
      return(NULL)
    }
    Sys.sleep(0.02)
  }
}

# Append the available bytes to the read buffer.
ws_fill <- function(ws) {
  bytes <- tryCatch(
    readBin(ws[["con"]], "raw", n = 1048576L),
    error = function(e) raw(0L)
  )
  ws[["buffer"]] <- c(ws[["buffer"]], bytes)
  invisible(ws)
}

# Big-endian bytes of a non-negative whole number.
be_bytes <- function(x, n) {
  as.raw(rev((x %/% 256^(seq_len(n) - 1L)) %% 256))
}

# One client message with opcode 2 (binary), masked as RFC 6455 requires.
ws_message <- function(bytes) {
  n <- length(bytes)
  len <- if (n < 126L) {
    as.raw(128L + n)
  } else if (n < 65536L) {
    c(as.raw(254L), be_bytes(n, 2L))
  } else {
    c(as.raw(255L), be_bytes(n, 8L))
  }
  mask <- openssl::rand_bytes(4L)
  c(as.raw(130L), len, mask, xor(bytes, rep_len(mask, n)))
}

# Write bytes to the socket. The server may have closed it.
ws_write <- function(ws, bytes) {
  tryCatch(
    suppressWarnings(writeBin(bytes, ws[["con"]])),
    error = function(e) NULL
  )
  invisible(ws)
}

ws_send_raw <- function(ws, bytes) {
  ws_write(ws, ws_message(bytes))
}

ws_request <- function(ws, method, params = NULL, id = method) {
  ws_send_raw(
    ws,
    encode_frame(list(v = 1L, id = id, method = method, params = params))
  )
}

# The next message as a decoded frame header, `"closed"` after a close frame,
# or NULL on timeout.
ws_recv <- function(ws, timeout = 5) {
  deadline <- Sys.time() + timeout
  message <- raw(0L)
  repeat {
    b <- as.integer(ws[["buffer"]][1:10])
    n <- NA_real_
    head <- 2L
    if (!is.na(b[[2L]])) {
      n <- bitwAnd(b[[2L]], 127L)
      if (n == 126L) {
        n <- sum(b[3:4] * 256^(1:0))
        head <- 4L
      } else if (n == 127L) {
        n <- sum(b[3:10] * 256^(7:0))
        head <- 10L
      }
    }
    if (!is.na(n) && length(ws[["buffer"]]) >= head + n) {
      body <- ws[["buffer"]][head + seq_len(n)]
      ws[["buffer"]] <- ws[["buffer"]][-seq_len(head + n)]
      opcode <- bitwAnd(b[[1L]], 15L)
      if (opcode == 8L) {
        ws[["closed"]] <- TRUE
        return("closed")
      }
      if (opcode %in% c(0L, 2L)) {
        message <- c(message, body)
        if (b[[1L]] >= 128L) {
          return(decode_frame(message)[["header"]])
        }
      }
      next
    }
    if (Sys.time() > deadline) {
      return(NULL)
    }
    Sys.sleep(0.02)
    ws_fill(ws)
  }
}

ws_close <- function(ws) {
  tryCatch(close(ws[["con"]]), error = function(e) NULL)
  invisible(NULL)
}

# Connect once the server accepts, and read the `ready` event.
ws_connect <- function(port, origin = "http://localhost:3000", timeout = 20) {
  deadline <- Sys.time() + timeout
  repeat {
    ws <- ws_open(port, origin)
    if (!is.null(ws)) {
      ready <- ws_recv(ws)
      if (is.list(ready) && identical(ready[["event"]], "ready")) {
        return(ws)
      }
      ws_close(ws)
    }
    if (Sys.time() > deadline) {
      stop("No server on port ", port, " within ", timeout, " s.")
    }
    Sys.sleep(0.1)
  }
}
