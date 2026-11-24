# frame.R
# ::rtemis.server::
# 2026- EDG rtemis.org

# Wire frame encode/decode for the rtemislive WebSocket protocol.
#
# Each logical message is a single binary WebSocket frame with the layout:
#
#   [4 bytes u32 BE header_len][header_len bytes UTF-8 JSON][optional payload]
#
# - The header is always present.
# - When the header's `payload` field is non-null, the frame carries
#   `payload$bytes` bytes of binary (typically Arrow IPC) data immediately
#   after the header.
# - Each frame is one binary WebSocket message. nanonext reassembles the
#   fragments of a message and closes the connection on an incoming message
#   larger than 1 MiB, so the limits below bind on outgoing frames.
#
# spec: rtemislive.md 3.3 (frame format) and 4 (envelope shapes)

.RTEMISLIVE_HEADER_MAX <- 1048576L # 1 MiB
# Header is JSON metadata only (binary data travels in the payload),
# but with tunable-grid hyperparameter arrays + a long features list +
# verbose result summaries the JSON can still grow into the hundreds of
# KiB. 1 MiB is a generous upper bound - anything beyond is genuinely
# malformed.
.RTEMISLIVE_FRAME_MAX <- 268435456L # 256 MiB
.RTEMISLIVE_PROTOCOL_V <- 1L


# %% .wire_json ----
#' Write an R object as wire JSON
#'
#' The one JSON writer of the package: frame headers, the size check of the run
#' record, and the daemons' progress envelopes. Numbers are written with 15
#' significant digits. The `jsonlite::toJSON()` default keeps 4 decimal places
#' and would write a learning rate of 0.00012 as 0.0001.
#'
#' @param x List: Object to write.
#'
#' @return Character scalar.
#'
#' @author EDG
#' @keywords internal
#' @noRd
.wire_json <- function(x) {
  as.character(jsonlite::toJSON(
    x,
    auto_unbox = TRUE,
    null = "null",
    na = "null",
    digits = NA
  ))
}


# %% encode_frame ----
#' Encode an rtemislive wire frame
#'
#' Serializes a header (a JSON-able R list) plus an optional binary payload
#' into a single raw vector ready to send over a WebSocket.
#'
#' If `payload` is supplied, the function adds a `payload` field to the
#' header automatically (`list(format = "arrow-ipc", bytes = N)`). Any
#' `payload` field already present in `header` is overwritten.
#'
#' @param header Named list: The frame envelope (request / response / event).
#'   Must include a `v` field; if missing, defaults to the current protocol
#'   version.
#' @param payload Optional raw vector: Optional binary blob (typically
#'   Arrow IPC bytes).
#'
#' @return Raw vector - the complete on-the-wire frame.
#'
#' @author EDG
#' @keywords internal
#' @noRd
encode_frame <- function(header, payload = NULL) {
  if (!is.list(header)) {
    rtemis.core::abort("`header` must be a named list.")
  }
  if (!is.null(payload) && !is.raw(payload)) {
    rtemis.core::abort("`payload` must be a raw vector or NULL.")
  }

  if (is.null(header[["v"]])) {
    header[["v"]] <- .RTEMISLIVE_PROTOCOL_V
  }

  if (is.null(payload)) {
    header[["payload"]] <- NULL
  } else {
    header[["payload"]] <- list(
      format = "arrow-ipc",
      bytes = length(payload)
    )
  }

  json_bytes <- charToRaw(.wire_json(header))
  header_len <- length(json_bytes)

  if (header_len > .RTEMISLIVE_HEADER_MAX) {
    rtemis.core::abort(
      "Header too large (",
      header_len,
      " bytes, limit ",
      .RTEMISLIVE_HEADER_MAX,
      ")."
    )
  }

  payload_len <- if (is.null(payload)) 0L else length(payload)
  total <- 4L + header_len + payload_len
  if (total > .RTEMISLIVE_FRAME_MAX) {
    rtemis.core::abort(
      "Frame too large (",
      total,
      " bytes, limit ",
      .RTEMISLIVE_FRAME_MAX,
      ")."
    )
  }

  len_raw <- writeBin(
    as.integer(header_len),
    raw(),
    size = 4L,
    endian = "big"
  )

  if (is.null(payload)) {
    c(len_raw, json_bytes)
  } else {
    c(len_raw, json_bytes, payload)
  }
}


# %% decode_frame ----
#' Decode an rtemislive wire frame
#'
#' A frame is one complete WebSocket message. Aborts on malformed input:
#' header length out of range, header JSON unparseable, and a message shorter
#' or longer than the header declares.
#'
#' @param buf Raw vector: The message.
#'
#' @return Named list: `header`, a list, and `payload`, a raw vector or `NULL`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
decode_frame <- function(buf) {
  if (!is.raw(buf)) {
    rtemis.core::abort("`buf` must be a raw vector.")
  }

  n <- length(buf)
  if (n < 4L) {
    rtemis.core::abort(
      "Malformed frame: ",
      n,
      " bytes is too short for the header length."
    )
  }

  header_len <- readBin(
    buf[1L:4L],
    what = "integer",
    n = 1L,
    size = 4L,
    endian = "big",
    signed = TRUE
  )
  if (header_len < 0L || header_len > .RTEMISLIVE_HEADER_MAX) {
    # The first bytes identify what was sent instead of a frame, such as a
    # bare Arrow IPC stream.
    preview_n <- min(32L, n)
    preview <- paste(
      sprintf("%02x", as.integer(buf[1L:preview_n])),
      collapse = " "
    )
    rtemis.core::abort(
      "Malformed frame: header_len ",
      header_len,
      " out of range (limit ",
      .RTEMISLIVE_HEADER_MAX,
      "). First ",
      preview_n,
      " bytes: ",
      preview
    )
  }

  header_end <- 4L + header_len
  if (n < header_end) {
    rtemis.core::abort(
      "Malformed frame: header of ",
      header_len,
      " bytes declared, ",
      n - 4L,
      " bytes received."
    )
  }

  header_bytes <- buf[4L + seq_len(header_len)]
  header <- tryCatch(
    jsonlite::fromJSON(rawToChar(header_bytes), simplifyVector = FALSE),
    error = function(e) {
      # Include the parser's message in the wire text so the client sees
      # what about the JSON was invalid (unlike cli which
      # would choke on `{` characters from invalid input; rtemis.core's
      # abort() doesn't interpolate, so this is safe).
      rtemis.core::abort(
        "Malformed frame: header JSON could not be parsed: ",
        conditionMessage(e),
        parent = e
      )
    }
  )
  if (!is.list(header)) {
    rtemis.core::abort("Malformed frame: header is not a JSON object.")
  }

  payload_info <- header[["payload"]]
  payload_len <- 0
  if (!is.null(payload_info)) {
    if (!is.list(payload_info) || is.null(payload_info[["bytes"]])) {
      rtemis.core::abort(
        "Malformed frame: header `payload` must be a `{bytes, format}` object."
      )
    }
    payload_len <- payload_info[["bytes"]]
    if (
      !is.numeric(payload_len) ||
        length(payload_len) != 1L ||
        !is.finite(payload_len) ||
        payload_len < 0 ||
        payload_len != round(payload_len)
    ) {
      rtemis.core::abort(
        "Malformed frame: payload bytes is not a non-negative integer."
      )
    }
  }
  if (n != header_end + payload_len) {
    rtemis.core::abort(
      "Malformed frame: payload of ",
      payload_len,
      " bytes declared, ",
      n - header_end,
      " bytes received."
    )
  }

  list(
    header = header,
    payload = if (!is.null(payload_info)) {
      buf[header_end + seq_len(payload_len)]
    }
  )
}


# %% make_response ----
#' Build a response header
#'
#' Convenience helper for handlers - produces a well-formed response
#' envelope that pairs with a request's `id`.
#'
#' @param id Character: Correlation id from the request.
#' @param result Optional list: Response payload (success path).
#'
#' @return Named list ready to pass to `encode_frame()`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
make_response <- function(id, result = NULL) {
  list(
    v = .RTEMISLIVE_PROTOCOL_V,
    id = id,
    ok = TRUE,
    result = result
  )
}


# %% make_error ----
#' Build an error response header
#'
#' @param id Character: Correlation id from the request, or `NA` when no
#'   id was extractable (e.g. malformed frame).
#' @param code Character: One of the error codes in spec paragraph 15.
#' @param message Character: Human-readable description.
#' @param details List or NULL: Optional structured details.
#'
#' @return Named list ready to pass to `encode_frame()`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
make_error <- function(id, code, message, details = NULL) {
  err <- list(code = code, message = message)
  if (!is.null(details)) {
    err[["details"]] <- details
  }
  list(
    v = .RTEMISLIVE_PROTOCOL_V,
    id = id,
    ok = FALSE,
    error = err
  )
}


# %% make_event ----
#' Build a server-pushed event header
#'
#' @param event Character: Event name (see spec paragraph 8 for the catalog).
#' @param data List or NULL: Event-specific payload.
#'
#' @return Named list ready to pass to `encode_frame()`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
make_event <- function(event, data = NULL) {
  list(
    v = .RTEMISLIVE_PROTOCOL_V,
    event = event,
    data = data
  )
}
