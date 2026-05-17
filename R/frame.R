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
# - All frames are sent and received as binary WebSocket frames
#   (`textframes = FALSE` on `nanonext::stream`).
#
# See specs/rtemislive.md paragraph 3.3 and paragraph 4 for the full wire format and envelope
# shapes (request / response / event).

# Limits per spec paragraph 3.4
.RTEMISLIVE_HEADER_MAX <- 1048576L # 1 MiB
# Header is JSON metadata only (binary data travels in the payload),
# but with tunable-grid hyperparameter arrays + a long features list +
# verbose result summaries the JSON can still grow into the hundreds of
# KiB. 1 MiB is a generous upper bound - anything beyond is genuinely
# malformed.
.RTEMISLIVE_FRAME_MAX <- 268435456L # 256 MiB
.RTEMISLIVE_PROTOCOL_V <- 1L


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
#' @param header Named list. The frame envelope (request / response / event).
#'   Must include a `v` field; if missing, defaults to the current protocol
#'   version.
#' @param payload Raw vector or `NULL`. Optional binary blob (typically
#'   Arrow IPC bytes).
#'
#' @return Raw vector - the complete on-the-wire frame.
#'
#' @author EDG
#' @keywords internal
#' @noRd
encode_frame <- function(header, payload = NULL) {
  if (!is.list(header)) {
    cli::cli_abort("`header` must be a named list.")
  }
  if (!is.null(payload) && !is.raw(payload)) {
    cli::cli_abort("`payload` must be a raw vector or NULL.")
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

  json <- jsonlite::toJSON(
    header,
    auto_unbox = TRUE,
    null = "null",
    na = "null"
  )
  json_bytes <- charToRaw(as.character(json))
  header_len <- length(json_bytes)

  if (header_len > .RTEMISLIVE_HEADER_MAX) {
    header_max <- .RTEMISLIVE_HEADER_MAX
    cli::cli_abort(
      "Header too large ({header_len} bytes, limit {header_max})."
    )
  }

  payload_len <- if (is.null(payload)) 0L else length(payload)
  total <- 4L + header_len + payload_len
  if (total > .RTEMISLIVE_FRAME_MAX) {
    frame_max <- .RTEMISLIVE_FRAME_MAX
    cli::cli_abort(
      "Frame too large ({total} bytes, limit {frame_max})."
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
} # /rtemis::encode_frame


# %% decode_frame ----
#' Decode an rtemislive wire frame from a buffer
#'
#' Streaming-friendly decode: callers maintain a per-connection raw buffer
#' that grows as bytes arrive. They call `decode_frame()` on the buffer; if
#' a complete frame is available, the function returns it along with the
#' number of bytes consumed (so the caller can advance the buffer). If the
#' buffer is short (incomplete frame), the function returns
#' `list(complete = FALSE)` and the caller waits for more bytes.
#'
#' Throws on malformed input (header JSON unparseable, header_len exceeds
#' limits). Truncation alone is not an error - it just means more bytes
#' are needed.
#'
#' @param buf Raw vector. The current connection buffer.
#'
#' @return Named list:
#'
#' - `complete = FALSE` - buffer holds an incomplete frame; wait for more.
#' - `complete = TRUE`, `header = <list>`, `payload = <raw|NULL>`,
#'   `consumed = <integer>` - a complete frame has been parsed; advance
#'   the buffer by `consumed` bytes.
#'
#' @author EDG
#' @keywords internal
#' @noRd
decode_frame <- function(buf) {
  if (!is.raw(buf)) {
    cli::cli_abort("`buf` must be a raw vector.")
  }

  n <- length(buf)
  if (n < 4L) {
    return(list(complete = FALSE))
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
    # Capture the first 32 bytes of the buffer as hex so we can identify
    # what kind of payload tripped us up - Arrow IPC streams, garbage,
    # leftover bytes from a desynced previous frame, etc. all have
    # distinctive prefixes.
    header_max <- .RTEMISLIVE_HEADER_MAX
    preview_n <- min(32L, n)
    preview <- paste(
      sprintf("%02x", as.integer(buf[1L:preview_n])),
      collapse = " "
    )
    cli::cli_abort(
      "Malformed frame: header_len {header_len} out of range (limit {header_max}). First {preview_n} bytes: {preview}"
    )
  }

  header_end <- 4L + header_len
  if (n < header_end) {
    return(list(complete = FALSE))
  }

  header_bytes <- buf[5L:header_end]
  header <- tryCatch(
    jsonlite::fromJSON(rawToChar(header_bytes), simplifyVector = FALSE),
    error = function(e) {
      # Chain via `parent` so the underlying error message is shown by the
      # condition system without re-running through cli's glue interpolation
      # (which would choke on `{` characters in invalid JSON input).
      cli::cli_abort(
        "Malformed frame: header JSON could not be parsed.",
        parent = e
      )
    }
  )
  if (!is.list(header)) {
    cli::cli_abort("Malformed frame: header is not a JSON object.")
  }

  payload_info <- header[["payload"]]
  if (is.null(payload_info)) {
    return(list(
      complete = TRUE,
      header = header,
      payload = NULL,
      consumed = header_end
    ))
  }

  if (!is.list(payload_info) || is.null(payload_info[["bytes"]])) {
    cli::cli_abort("Malformed frame: header `payload` must be {bytes, format}.")
  }

  payload_len <- as.integer(payload_info[["bytes"]])
  if (is.na(payload_len) || payload_len < 0L) {
    cli::cli_abort(
      "Malformed frame: payload bytes is not a non-negative integer."
    )
  }

  total <- header_end + payload_len
  if (total > .RTEMISLIVE_FRAME_MAX) {
    frame_max <- .RTEMISLIVE_FRAME_MAX
    cli::cli_abort(
      "Malformed frame: frame size {total} exceeds limit {frame_max}."
    )
  }
  if (n < total) {
    return(list(complete = FALSE))
  }

  payload <- if (payload_len == 0L) {
    raw(0)
  } else {
    buf[(header_end + 1L):total]
  }

  list(
    complete = TRUE,
    header = header,
    payload = payload,
    consumed = total
  )
} # /rtemis::decode_frame


# %% make_response ----
#' Build a response header
#'
#' Convenience helper for handlers - produces a well-formed response
#' envelope that pairs with a request's `id`.
#'
#' @param id Character. Correlation id from the request.
#' @param result List or NULL. Response payload (success path).
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
} # /rtemis::make_response


# %% make_error ----
#' Build an error response header
#'
#' @param id Character. Correlation id from the request, or `NA` when no
#'   id was extractable (e.g. malformed frame).
#' @param code Character. One of the error codes in spec paragraph 15.
#' @param message Character. Human-readable description.
#' @param details List or NULL. Optional structured details.
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
} # /rtemis::make_error


# %% make_event ----
#' Build a server-pushed event header
#'
#' @param event Character. Event name (see spec paragraph 8 for the catalog).
#' @param data List or NULL. Event-specific payload.
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
} # /rtemis::make_event
