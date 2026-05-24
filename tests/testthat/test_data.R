# test_data.R
# ::rtemis::
# 2026- EDG rtemis.org

skip_if_not_installed("arrow")

library(data.table)


# Helpers ----
make_session <- function(name = NULL) {
  clear_sessions()
  new_session(name)
}

ipc_bytes <- function(dt) {
  # Round-trip a data.table to Arrow IPC stream bytes.
  arrow::write_to_raw(dt, format = "stream")
}

sample_dt <- function() {
  data.table(
    x = 1:10,
    y = letters[1:10],
    z = c(rnorm(9), NA_real_)
  )
}


# Identifier helpers ----
test_that("new_data_handle_id() returns data-<hex16>", {
  expect_match(new_data_handle_id(), "^data-[0-9a-f]{16}$")
})

test_that("new_upload_id() returns upload-<hex16>", {
  expect_match(new_upload_id(), "^upload-[0-9a-f]{16}$")
})


# decode_arrow_ipc ----
test_that("decode_arrow_ipc() round-trips a data.table", {
  dt <- sample_dt()
  bytes <- ipc_bytes(dt)
  expect_true(is.raw(bytes))
  back <- decode_arrow_ipc(bytes)
  expect_s3_class(back, "data.table")
  expect_equal(nrow(back), nrow(dt))
  expect_equal(ncol(back), ncol(dt))
  expect_equal(back[["x"]], dt[["x"]])
  # `decode_arrow_ipc()` deliberately converts character columns to factor
  # at the IPC boundary (rtemis ML pipeline expects categoricals as
  # factors). Compare against the post-coercion expected value.
  expect_equal(back[["y"]], factor(dt[["y"]]))
})

test_that("decode_arrow_ipc() rejects non-raw or empty input", {
  expect_error(decode_arrow_ipc("not raw"), "raw vector")
  expect_error(decode_arrow_ipc(raw(0)), "empty")
})

test_that("decode_arrow_ipc() reports parse errors via parent chaining", {
  expect_error(decode_arrow_ipc(as.raw(1:10)), "Could not decode")
})


# new_data_handle (single-frame upload) ----
test_that("new_data_handle() registers and returns a summary", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)

  summary <- new_data_handle(s, "iris", ipc_bytes(sample_dt()))
  expect_match(summary[["data_handle"]], "^data-")
  expect_equal(summary[["name"]], "iris")
  expect_equal(summary[["rows"]], 10L)
  expect_equal(summary[["cols"]], 3L)

  expect_true(summary[["data_handle"]] %in% ls(s[["data"]]))
})

test_that("new_data_handle() rejects empty / non-string names", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)
  bytes <- ipc_bytes(sample_dt())
  expect_error(new_data_handle(s, "", bytes))
  expect_error(new_data_handle(s, NA_character_, bytes))
  expect_error(new_data_handle(s, 123, bytes))
})

test_that("new_data_handle() respects max_handles cap", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)
  bytes <- ipc_bytes(sample_dt())
  new_data_handle(s, "a", bytes, max_handles = 2L)
  new_data_handle(s, "b", bytes, max_handles = 2L)
  expect_error(
    new_data_handle(s, "c", bytes, max_handles = 2L),
    class = "rtemislive_too_many"
  )
})


# get_data / list / delete ----
test_that("get_data() returns the stored data.table and bumps last_used", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)
  summary <- new_data_handle(s, "x", ipc_bytes(sample_dt()))
  handle <- summary[["data_handle"]]

  h <- s[["data"]][[handle]]
  h[["last_used"]] <- Sys.time() - 60
  ts_before <- h[["last_used"]]

  dt <- get_data(s, handle)
  expect_s3_class(dt, "data.table")
  expect_gt(as.numeric(h[["last_used"]]), as.numeric(ts_before))
})

test_that("get_data() throws rtemislive_not_found on unknown handle", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)
  expect_error(
    get_data(s, "data-bogus"),
    class = "rtemislive_not_found"
  )
})

test_that("list_data_handles() summarises each", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)
  new_data_handle(s, "a", ipc_bytes(sample_dt()))
  new_data_handle(s, "b", ipc_bytes(sample_dt()))
  out <- list_data_handles(s)
  expect_length(out, 2L)
  names <- vapply(out, `[[`, character(1L), "name")
  expect_setequal(names, c("a", "b"))
})

test_that("delete_data() removes by handle", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)
  summary <- new_data_handle(s, "x", ipc_bytes(sample_dt()))
  h <- summary[["data_handle"]]
  expect_true(delete_data(s, h))
  expect_false(h %in% ls(s[["data"]]))
  expect_false(delete_data(s, h))
  expect_false(delete_data(s, "data-bogus"))
})


# describe_data ----
test_that("describe_data() returns per-column summaries", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)
  summary <- new_data_handle(s, "x", ipc_bytes(sample_dt()))
  desc <- describe_data(s, summary[["data_handle"]])

  expect_equal(desc[["rows"]], 10L)
  expect_equal(desc[["cols"]], 3L)
  expect_length(desc[["columns"]], 3L)

  cols_by_name <- setNames(
    desc[["columns"]],
    vapply(desc[["columns"]], `[[`, character(1L), "name")
  )
  expect_equal(cols_by_name[["x"]][["type"]], "integer")
  # `y` arrives as character but `decode_arrow_ipc()` coerces to factor.
  expect_equal(cols_by_name[["y"]][["type"]], "factor")
  expect_equal(cols_by_name[["z"]][["type"]], "double")
  expect_equal(cols_by_name[["x"]][["n_unique"]], 10L)
  expect_equal(cols_by_name[["z"]][["n_missing"]], 1L)

  expect_true(!is.null(cols_by_name[["x"]][["summary"]][["min"]]))
  expect_true(!is.null(cols_by_name[["y"]][["summary"]][["top"]]))
})


# GC ----
test_that("gc_data() drops handles past TTL", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)
  bytes <- ipc_bytes(sample_dt())
  s1 <- new_data_handle(s, "old", bytes)
  s2 <- new_data_handle(s, "young", bytes)

  s[["data"]][[s1[["data_handle"]]]][["last_used"]] <- Sys.time() - 5000

  expired <- gc_data(s, ttl = 1000)
  expect_equal(expired, s1[["data_handle"]])
  expect_false(s1[["data_handle"]] %in% ls(s[["data"]]))
  expect_true(s2[["data_handle"]] %in% ls(s[["data"]]))
})


# Chunked upload ----
test_that("begin/chunk/end upload assembles bytes and registers handle", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)

  dt <- sample_dt()
  full <- ipc_bytes(dt)
  # Split into 3 chunks
  n <- length(full)
  cuts <- c(floor(n / 3), floor(2 * n / 3))
  c1 <- full[1:cuts[1]]
  c2 <- full[(cuts[1] + 1L):cuts[2]]
  c3 <- full[(cuts[2] + 1L):n]

  uid <- begin_upload(s, "chunked", total_bytes = n, n_chunks = 3L)
  expect_match(uid, "^upload-")

  prog1 <- chunk_upload(s, uid, 1L, c1)
  expect_equal(prog1[["received_count"]], 1L)
  expect_equal(prog1[["received_bytes"]], length(c1))

  chunk_upload(s, uid, 3L, c3) # out-of-order ok
  chunk_upload(s, uid, 2L, c2)

  summary <- end_upload(s, uid)
  expect_match(summary[["data_handle"]], "^data-")
  expect_equal(summary[["rows"]], nrow(dt))
  expect_equal(summary[["cols"]], ncol(dt))

  # Pending upload state is gone after end_upload
  expect_false(uid %in% ls(pending_uploads(s)))
})

test_that("chunk_upload() rejects unknown upload_id", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)
  expect_error(
    chunk_upload(s, "upload-bogus", 1L, as.raw(1:10)),
    class = "rtemislive_not_found"
  )
})

test_that("chunk_upload() rejects out-of-range index and duplicates", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)
  uid <- begin_upload(s, "x", total_bytes = 10L, n_chunks = 2L)
  expect_error(
    chunk_upload(s, uid, 0L, as.raw(1:5)),
    class = "rtemislive_invalid_params"
  )
  expect_error(
    chunk_upload(s, uid, 3L, as.raw(1:5)),
    class = "rtemislive_invalid_params"
  )
  chunk_upload(s, uid, 1L, as.raw(1:5))
  expect_error(
    chunk_upload(s, uid, 1L, as.raw(6:10)),
    class = "rtemislive_invalid_params"
  )
})

test_that("end_upload() refuses on missing chunks", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)
  uid <- begin_upload(s, "x", total_bytes = 10L, n_chunks = 2L)
  chunk_upload(s, uid, 1L, as.raw(1:5))
  expect_error(
    end_upload(s, uid),
    class = "rtemislive_invalid_params"
  )
  # state was cleaned up regardless
  expect_false(uid %in% ls(pending_uploads(s)))
})

test_that("end_upload() refuses on byte-count mismatch", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)
  uid <- begin_upload(s, "x", total_bytes = 100L, n_chunks = 2L)
  chunk_upload(s, uid, 1L, as.raw(1:5))
  chunk_upload(s, uid, 2L, as.raw(6:10))
  expect_error(end_upload(s, uid), class = "rtemislive_invalid_params")
})

test_that("cancel_upload() drops state and returns logical", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)
  uid <- begin_upload(s, "x", total_bytes = 10L, n_chunks = 2L)
  expect_true(cancel_upload(s, uid))
  expect_false(uid %in% ls(pending_uploads(s)))
  expect_false(cancel_upload(s, uid))
  expect_false(cancel_upload(s, "upload-bogus"))
})
