# test_frame.R
# ::rtemis::
# 2026- EDG rtemis.org

skip_if_not_installed("jsonlite")


# encode_frame: header only ----
test_that("encode_frame() with no payload produces 4-byte length + JSON header", {
  buf <- encode_frame(list(method = "ping", id = "req-1"))
  expect_true(is.raw(buf))

  hl <- readBin(
    buf[1:4],
    "integer",
    n = 1L,
    size = 4L,
    endian = "big",
    signed = TRUE
  )
  expect_equal(length(buf), 4L + hl)

  json <- rawToChar(buf[5:length(buf)])
  parsed <- jsonlite::fromJSON(json, simplifyVector = FALSE)
  expect_equal(parsed[["method"]], "ping")
  expect_equal(parsed[["id"]], "req-1")
  expect_equal(parsed[["v"]], 1L)
})

test_that("encode_frame() rejects non-list header", {
  expect_error(encode_frame("not a list"), "named list")
  expect_error(encode_frame(NULL), "named list")
})

test_that("encode_frame() rejects non-raw payload", {
  expect_error(encode_frame(list(method = "x"), payload = "string"), "raw")
})


# encode_frame: with payload ----
test_that("encode_frame() with payload appends payload bytes after header", {
  payload <- as.raw(1:10)
  buf <- encode_frame(list(method = "data.upload", id = "r"), payload = payload)

  hl <- readBin(
    buf[1:4],
    "integer",
    n = 1L,
    size = 4L,
    endian = "big",
    signed = TRUE
  )
  expect_equal(length(buf), 4L + hl + 10L)

  parsed <- jsonlite::fromJSON(
    rawToChar(buf[5:(4 + hl)]),
    simplifyVector = FALSE
  )
  expect_equal(parsed[["payload"]][["bytes"]], 10L)
  expect_equal(parsed[["payload"]][["format"]], "arrow-ipc")

  expect_equal(buf[(5 + hl):(4 + hl + 10)], payload)
})

test_that("encode_frame() defaults v to current protocol version", {
  buf <- encode_frame(list(method = "ping", id = "x"))
  hl <- readBin(buf[1:4], "integer", n = 1L, size = 4L, endian = "big")
  parsed <- jsonlite::fromJSON(
    rawToChar(buf[5:(4 + hl)]),
    simplifyVector = FALSE
  )
  expect_equal(parsed[["v"]], 1L)
})

test_that("encode_frame() preserves user-supplied v", {
  buf <- encode_frame(list(v = 7L, method = "ping", id = "x"))
  hl <- readBin(buf[1:4], "integer", n = 1L, size = 4L, endian = "big")
  parsed <- jsonlite::fromJSON(
    rawToChar(buf[5:(4 + hl)]),
    simplifyVector = FALSE
  )
  expect_equal(parsed[["v"]], 7L)
})


# decode_frame: incomplete inputs ----
test_that("decode_frame() returns complete=FALSE on empty buffer", {
  res <- decode_frame(raw(0))
  expect_false(res[["complete"]])
})

test_that("decode_frame() returns complete=FALSE when only length prefix arrived", {
  buf <- encode_frame(list(method = "ping", id = "x"))
  res <- decode_frame(buf[1:4])
  expect_false(res[["complete"]])
})

test_that("decode_frame() returns complete=FALSE when header is truncated", {
  buf <- encode_frame(list(method = "ping", id = "x"))
  res <- decode_frame(buf[1:(length(buf) - 2L)])
  expect_false(res[["complete"]])
})

test_that("decode_frame() returns complete=FALSE when payload is truncated", {
  payload <- as.raw(1:100)
  buf <- encode_frame(list(method = "data.upload", id = "x"), payload = payload)
  res <- decode_frame(buf[1:(length(buf) - 50L)])
  expect_false(res[["complete"]])
})


# decode_frame: complete frames ----
test_that("decode_frame() roundtrips a header-only frame", {
  hdr_in <- list(method = "ping", id = "req-1", params = list(token = "abc"))
  buf <- encode_frame(hdr_in)
  res <- decode_frame(buf)
  expect_true(res[["complete"]])
  expect_equal(res[["consumed"]], length(buf))
  expect_null(res[["payload"]])
  expect_equal(res[["header"]][["method"]], "ping")
  expect_equal(res[["header"]][["id"]], "req-1")
  expect_equal(res[["header"]][["params"]][["token"]], "abc")
  expect_equal(res[["header"]][["v"]], 1L)
})

test_that("decode_frame() roundtrips a frame with payload", {
  payload_in <- charToRaw("hello arrow ipc bytes")
  buf <- encode_frame(
    list(method = "data.upload", id = "r"),
    payload = payload_in
  )
  res <- decode_frame(buf)
  expect_true(res[["complete"]])
  expect_equal(res[["consumed"]], length(buf))
  expect_equal(res[["payload"]], payload_in)
  expect_equal(res[["header"]][["payload"]][["bytes"]], length(payload_in))
})

test_that("decode_frame() advances consumed past one frame, leaving extra bytes", {
  buf1 <- encode_frame(list(method = "ping", id = "1"))
  buf2 <- encode_frame(list(method = "ping", id = "2"))
  combined <- c(buf1, buf2)
  res1 <- decode_frame(combined)
  expect_true(res1[["complete"]])
  expect_equal(res1[["consumed"]], length(buf1))
  remaining <- combined[(res1[["consumed"]] + 1L):length(combined)]
  res2 <- decode_frame(remaining)
  expect_true(res2[["complete"]])
  expect_equal(res2[["header"]][["id"]], "2")
})


# decode_frame: malformed inputs ----
test_that("decode_frame() rejects negative header_len", {
  bad_len <- writeBin(-1L, raw(), size = 4L, endian = "big")
  expect_error(decode_frame(bad_len), "out of range")
})

test_that("decode_frame() rejects header_len above the limit", {
  over <- 1048576L + 1L
  big_len <- writeBin(over, raw(), size = 4L, endian = "big")
  buf <- c(big_len, raw(over))
  expect_error(decode_frame(buf), "out of range")
})

test_that("decode_frame() rejects unparseable header JSON", {
  bad_json <- charToRaw("{not valid")
  buf <- c(
    writeBin(length(bad_json), raw(), size = 4L, endian = "big"),
    bad_json
  )
  expect_error(decode_frame(buf), "could not be parsed")
})

test_that("decode_frame() rejects non-raw input", {
  expect_error(decode_frame("not raw"), "raw vector")
})


# Helpers: make_response, make_error, make_event ----
test_that("make_response() builds an ok-response with id and result", {
  r <- make_response("req-1", result = list(connection_id = "c-1"))
  expect_equal(r[["id"]], "req-1")
  expect_true(r[["ok"]])
  expect_equal(r[["result"]][["connection_id"]], "c-1")
  expect_equal(r[["v"]], 1L)
})

test_that("make_error() builds an error response with code and message", {
  r <- make_error("req-1", "unauthorized", "Authenticate first")
  expect_equal(r[["id"]], "req-1")
  expect_false(r[["ok"]])
  expect_equal(r[["error"]][["code"]], "unauthorized")
  expect_equal(r[["error"]][["message"]], "Authenticate first")
})

test_that("make_error() optionally includes details", {
  r <- make_error("r", "invalid_params", "bad", details = list(field = "port"))
  expect_equal(r[["error"]][["details"]][["field"]], "port")
})

test_that("make_event() builds an event header without id/ok", {
  ev <- make_event("heartbeat", data = list(ts = "now"))
  expect_equal(ev[["event"]], "heartbeat")
  expect_equal(ev[["data"]][["ts"]], "now")
  expect_null(ev[["id"]])
  expect_null(ev[["ok"]])
  expect_equal(ev[["v"]], 1L)
})


# End-to-end: response and event encode/decode ----
test_that("response frame round-trips through encode/decode", {
  resp <- make_response("req-1", result = list(connection_id = "c-1"))
  buf <- encode_frame(resp)
  res <- decode_frame(buf)
  expect_true(res[["complete"]])
  expect_true(res[["header"]][["ok"]])
  expect_equal(res[["header"]][["id"]], "req-1")
  expect_equal(res[["header"]][["result"]][["connection_id"]], "c-1")
})

test_that("event frame round-trips through encode/decode", {
  ev <- make_event(
    "job.progress",
    data = list(job_id = "j-1", message = "Fold 1/3")
  )
  buf <- encode_frame(ev)
  res <- decode_frame(buf)
  expect_true(res[["complete"]])
  expect_equal(res[["header"]][["event"]], "job.progress")
  expect_equal(res[["header"]][["data"]][["job_id"]], "j-1")
})
