# test_serial.R
# ::rtemis::
# 2026- EDG rtemis.org
#
# Tests for the Arrow IPC serial layer (serial.R) and the
# bulk-data slices it exposes through `job.result`.

skip_if_not_installed("arrow")
skip_if_not_installed("mirai")

library(data.table)


# File-level: ensure a daemon pool is available for the integration test
# that trains a Supervised and then asks for the `predictions` slice.
if (!isTRUE(getOption("rtemislive.test_daemons_started"))) {
  mirai::daemons(2L)
  options(rtemislive.test_daemons_started = TRUE)
}


# Helpers --------------------------------------------------------------------

make_server <- function(token = "test-toke-nnnn-9999") {
  new_server_state(token = token)
}

make_request <- function(method, params = NULL, id = NULL, payload = NULL) {
  hdr <- list(
    v = 1L,
    id = id %||% paste0("req-", basename(tempfile())),
    method = method,
    params = params
  )
  list(header = hdr, payload = payload)
}

authed_conn <- function(server, attach_session = NULL) {
  conn <- new_connection()
  resp <- dispatch_request(
    conn,
    make_request("auth", params = list(token = server[["token"]])),
    server
  )
  stopifnot(isTRUE(resp[["ok"]]))
  if (!is.null(attach_session)) {
    resp2 <- dispatch_request(
      conn,
      make_request("session.create", params = list(name = attach_session)),
      server
    )
    stopifnot(isTRUE(resp2[["ok"]]))
  }
  conn
}

ipc_bytes <- function(dt) {
  arrow::write_to_raw(dt, format = "stream")
}

wait_for_resolved <- function(job, timeout = 10) {
  start <- Sys.time()
  while (mirai::unresolved(job[["mirai"]])) {
    if (as.numeric(difftime(Sys.time(), start, units = "secs")) > timeout) {
      stop("Timed out waiting for job to resolve")
    }
    Sys.sleep(0.02)
  }
}


# encode_arrow_ipc ----------------------------------------------------------

test_that("encode_arrow_ipc round-trips a data.table via decode_arrow_ipc", {
  dt <- data.table(
    x = 1:5,
    y = c("a", "b", "c", "d", "e"),
    z = c(0.1, 0.2, NA_real_, 0.4, 0.5)
  )
  bytes <- encode_arrow_ipc(dt)
  expect_true(is.raw(bytes))
  expect_gt(length(bytes), 0L)

  back <- decode_arrow_ipc(bytes)
  expect_s3_class(back, "data.table")
  expect_equal(names(back), names(dt))
  expect_equal(nrow(back), nrow(dt))
  expect_equal(back[["x"]], dt[["x"]])
  # Character columns become factors on decode by design - see comment in
  # `decode_arrow_ipc()` for rationale (rtemis ML expects categoricals as
  # factors).
  expect_equal(back[["y"]], factor(dt[["y"]]))
  expect_equal(back[["z"]], dt[["z"]])
})

test_that("encode_arrow_ipc accepts a plain data.frame", {
  df <- data.frame(a = 1:3, b = letters[1:3], stringsAsFactors = FALSE)
  bytes <- encode_arrow_ipc(df)
  back <- decode_arrow_ipc(bytes)
  expect_equal(back[["a"]], df[["a"]])
  expect_equal(back[["b"]], factor(df[["b"]]))
})

test_that("encode_arrow_ipc errors on un-coercible input", {
  expect_error(encode_arrow_ipc(function(x) x))
})


# predictions_table / varimp_table ------------------------------------------

test_that("predictions_table errors with invalid_params on non-Supervised", {
  expect_error(
    predictions_table(1L),
    class = "rtemislive_invalid_params"
  )
})

test_that("varimp_table returns NULL on non-Supervised", {
  expect_null(varimp_table(1L))
})


# roc_table -----------------------------------------------------------------

test_that("roc_table returns an empty typed table on non-Supervised", {
  rt <- roc_table(1L)
  expect_s3_class(rt, "data.table")
  expect_equal(
    names(rt),
    c("split", "class", "fold", "fpr", "tpr", "auc")
  )
  expect_equal(nrow(rt), 0L)
})

test_that("roc_table emits an aggregate plus one curve per resample", {
  skip_if_not_installed("rtemis")

  set.seed(1L)
  n <- 120L
  dt <- data.frame(a = rnorm(n), b = rnorm(n))
  lp <- 1.2 * dt[["a"]] - 0.8 * dt[["b"]]
  dt[["y"]] <- factor(ifelse(plogis(lp) > runif(n), "pos", "neg"))

  fit <- rtemis::train(
    dt,
    algorithm = "glm",
    outer_resampling_config = rtemis::setup_Resampler(
      n_resamples = 3L,
      type = "KFold",
      seed = 1L
    ),
    verbosity = 0L
  )
  expect_true(inherits(fit, "rtemis::SupervisedRes"))

  rt <- roc_table(fit)
  expect_s3_class(rt, "data.table")
  expect_equal(names(rt), c("split", "class", "fold", "fpr", "tpr", "auc"))

  test_rows <- rt[split == "test"]
  expect_gt(nrow(test_rows), 0L)

  # One pooled "aggregate" curve plus exactly one curve per resample.
  expect_true("aggregate" %in% test_rows[["fold"]])
  per_fold <- unique(test_rows[fold != "aggregate"][["fold"]])
  expect_equal(length(per_fold), 3L)

  # AUC is constant within a split/class/fold group (repeated per vertex).
  auc_by_group <- test_rows[,
    .(n_auc = length(unique(auc))),
    by = .(class, fold)
  ]
  expect_true(all(auc_by_group[["n_auc"]] == 1L))
})


# make_response_payload ----------------------------------------------------

test_that("make_response_payload bundles header + payload", {
  resp <- make_response_payload(
    "req-1",
    list(rows = 3L, cols = 2L),
    as.raw(c(1L, 2L, 3L))
  )
  expect_named(resp, c("header", "payload"))
  expect_true(is.raw(resp[["payload"]]))
  expect_equal(resp[["header"]][["id"]], "req-1")
  expect_true(resp[["header"]][["ok"]])
  expect_equal(resp[["header"]][["result"]][["rows"]], 3L)
})

test_that("make_response_payload rejects non-raw payload", {
  expect_error(make_response_payload("req-1", list(), "not raw"))
})


# job.result `predictions` end-to-end ---------------------------------------

test_that("job.result `predictions` returns Arrow IPC payload for a trained Regression", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")

  set.seed(2027L)
  dt <- data.table(
    a = rnorm(60),
    b = rnorm(60),
    c = rnorm(60),
    y = NA_real_
  )
  dt[, y := a + 0.5 * b + rnorm(60)]

  upload <- dispatch_request(
    conn,
    make_request(
      "data.upload",
      params = list(name = "small"),
      payload = ipc_bytes(dt)
    ),
    server
  )
  handle <- upload[["result"]][["data_handle"]]

  submitted <- dispatch_request(
    conn,
    make_request(
      "train",
      params = list(data_handle = handle, algorithm = "glm")
    ),
    server
  )
  job_id <- submitted[["result"]][["job_id"]]

  s <- get_session_by_name("s")
  job <- s[["jobs"]][[job_id]]
  wait_for_resolved(job)
  check_job_resolved(job)
  expect_equal(job[["status"]], "complete")

  resp <- dispatch_request(
    conn,
    make_request(
      "job.result",
      params = list(job_id = job_id, slice = "predictions")
    ),
    server
  )
  # Wrapped {header, payload} shape
  expect_named(resp, c("header", "payload"))
  expect_true(resp[["header"]][["ok"]])
  result <- resp[["header"]][["result"]]
  expect_equal(result[["format"]], "arrow-ipc")
  expect_true("predicted" %in% result[["columns"]])
  expect_true("split" %in% result[["columns"]])
  expect_gt(result[["rows"]], 0L)
  expect_true(is.raw(resp[["payload"]]))
  expect_gt(length(resp[["payload"]]), 0L)

  # Decode the payload and confirm shape matches the JSON pointer
  back <- decode_arrow_ipc(resp[["payload"]])
  expect_equal(nrow(back), result[["rows"]])
  expect_equal(ncol(back), result[["cols"]])
  expect_true(all(result[["columns"]] %in% names(back)))
})


# job.result `metrics` end-to-end ------------------------------------------

test_that("job.result `metrics` returns per-split JSON for a trained Regression", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")

  set.seed(2029L)
  dt <- data.table(
    a = rnorm(60),
    b = rnorm(60),
    c = rnorm(60),
    y = NA_real_
  )
  dt[, y := a + 0.5 * b + rnorm(60)]

  upload <- dispatch_request(
    conn,
    make_request(
      "data.upload",
      params = list(name = "small"),
      payload = ipc_bytes(dt)
    ),
    server
  )
  handle <- upload[["result"]][["data_handle"]]

  submitted <- dispatch_request(
    conn,
    make_request(
      "train",
      params = list(data_handle = handle, algorithm = "glm")
    ),
    server
  )
  job_id <- submitted[["result"]][["job_id"]]

  s <- get_session_by_name("s")
  job <- s[["jobs"]][[job_id]]
  wait_for_resolved(job)
  check_job_resolved(job)

  resp <- dispatch_request(
    conn,
    make_request(
      "job.result",
      params = list(job_id = job_id, slice = "metrics")
    ),
    server
  )
  expect_true(resp[["ok"]])
  expect_true("training" %in% names(resp[["result"]]))
})

test_that("job.result `metrics` on a non-Supervised -> invalid_params", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  s <- connection_session(conn)
  job <- submit_job(s, "test", list(), expr = quote(1L))
  wait_for_resolved(job)
  check_job_resolved(job)
  resp <- dispatch_request(
    conn,
    make_request(
      "job.result",
      params = list(job_id = job[["id"]], slice = "metrics")
    ),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "invalid_params")
})


# job.result `varimp` end-to-end -------------------------------------------

test_that("job.result `varimp` returns JSON (no payload) for a trained Supervised", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")

  set.seed(2028L)
  dt <- data.table(
    a = rnorm(60),
    b = rnorm(60),
    c = rnorm(60),
    y = NA_real_
  )
  dt[, y := a + 0.5 * b + rnorm(60)]

  upload <- dispatch_request(
    conn,
    make_request(
      "data.upload",
      params = list(name = "small"),
      payload = ipc_bytes(dt)
    ),
    server
  )
  handle <- upload[["result"]][["data_handle"]]

  submitted <- dispatch_request(
    conn,
    make_request(
      "train",
      params = list(data_handle = handle, algorithm = "glm")
    ),
    server
  )
  job_id <- submitted[["result"]][["job_id"]]

  s <- get_session_by_name("s")
  job <- s[["jobs"]][[job_id]]
  wait_for_resolved(job)
  check_job_resolved(job)

  resp <- dispatch_request(
    conn,
    make_request(
      "job.result",
      params = list(job_id = job_id, slice = "varimp")
    ),
    server
  )
  # Wrapped {header, payload} shape - varimp slice now ships Arrow IPC
  # so the wire layer in `process_connection()` can attach the binary
  # bytes alongside the JSON pointer.
  expect_named(resp, c("header", "payload"))
  expect_true(resp[["header"]][["ok"]])
  result <- resp[["header"]][["result"]]
  expect_equal(result[["format"]], "arrow-ipc")
  expect_true("columns" %in% names(result))
  # Most GLMs expose a varimp table; if so the payload decodes to a
  # data.table with the named columns. Empty varimp (rows == 0) is also
  # acceptable for algorithms without varimp.
  if (result[["rows"]] > 0L) {
    expect_true(is.raw(resp[["payload"]]))
    back <- decode_arrow_ipc(resp[["payload"]])
    expect_equal(ncol(back), result[["cols"]])
    expect_true(all(result[["columns"]] %in% names(back)))
  }
})
