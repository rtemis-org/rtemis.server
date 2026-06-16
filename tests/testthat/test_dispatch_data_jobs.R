# test_dispatch_data_jobs.R
# ::rtemis::
# 2026- EDG rtemis.org

# Dispatcher tests for the data.* and train / job.* handlers. The
# session-level and connection-level handler tests live in
# test_dispatch.R.

skip_if_not_installed("arrow")
skip_if_not_installed("mirai")
skip_if_not_installed("rtemis")

library(data.table)


# File-level: ensure a daemon pool is available for the job-flow tests.
# See note in test_jobs.R.
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

sample_dt <- function() {
  data.table(x = 1:10, y = letters[1:10], z = c(rnorm(9), NA_real_))
}

ipc_bytes <- function(dt) {
  arrow::write_to_raw(dt, format = "stream")
}

wait_for_resolved <- function(job, timeout = 5) {
  start <- Sys.time()
  while (mirai::unresolved(job[["mirai"]])) {
    if (as.numeric(difftime(Sys.time(), start, units = "secs")) > timeout) {
      stop("Timed out waiting for job to resolve")
    }
    Sys.sleep(0.02)
  }
}


# data.upload (single frame) ------------------------------------------------

test_that("data.upload registers a handle and returns its summary", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")

  resp <- dispatch_request(
    conn,
    make_request(
      "data.upload",
      params = list(name = "iris"),
      payload = ipc_bytes(sample_dt())
    ),
    server
  )
  expect_true(resp[["ok"]])
  expect_equal(resp[["result"]][["name"]], "iris")
  expect_equal(resp[["result"]][["rows"]], 10L)
  expect_equal(resp[["result"]][["cols"]], 3L)
})

test_that("data.upload without payload -> invalid_params", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  resp <- dispatch_request(
    conn,
    make_request("data.upload", params = list(name = "iris")),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "invalid_params")
})

test_that("data.upload without name -> invalid_params", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  resp <- dispatch_request(
    conn,
    make_request("data.upload", payload = ipc_bytes(sample_dt())),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "invalid_params")
})


# data.list / data.describe / data.delete -----------------------------------

test_that("data.list returns wire summaries", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  dispatch_request(
    conn,
    make_request(
      "data.upload",
      params = list(name = "a"),
      payload = ipc_bytes(sample_dt())
    ),
    server
  )
  dispatch_request(
    conn,
    make_request(
      "data.upload",
      params = list(name = "b"),
      payload = ipc_bytes(sample_dt())
    ),
    server
  )
  resp <- dispatch_request(conn, make_request("data.list"), server)
  expect_true(resp[["ok"]])
  names <- vapply(resp[["result"]][["handles"]], `[[`, character(1L), "name")
  expect_setequal(names, c("a", "b"))
})

test_that("data.describe returns per-column type / n_unique / n_missing", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  upload <- dispatch_request(
    conn,
    make_request(
      "data.upload",
      params = list(name = "iris"),
      payload = ipc_bytes(sample_dt())
    ),
    server
  )
  handle <- upload[["result"]][["data_handle"]]

  resp <- dispatch_request(
    conn,
    make_request("data.describe", params = list(data_handle = handle)),
    server
  )
  expect_true(resp[["ok"]])
  expect_equal(resp[["result"]][["rows"]], 10L)
  expect_length(resp[["result"]][["columns"]], 3L)
})

test_that("data.describe unknown handle -> not_found", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  resp <- dispatch_request(
    conn,
    make_request("data.describe", params = list(data_handle = "data-bogus")),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "not_found")
})

test_that("data.delete drops a handle", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  upload <- dispatch_request(
    conn,
    make_request(
      "data.upload",
      params = list(name = "iris"),
      payload = ipc_bytes(sample_dt())
    ),
    server
  )
  handle <- upload[["result"]][["data_handle"]]

  resp <- dispatch_request(
    conn,
    make_request("data.delete", params = list(data_handle = handle)),
    server
  )
  expect_true(resp[["ok"]])
  expect_true(resp[["result"]][["deleted"]])
  # gone
  resp2 <- dispatch_request(
    conn,
    make_request("data.describe", params = list(data_handle = handle)),
    server
  )
  expect_equal(resp2[["error"]][["code"]], "not_found")
})


# Chunked upload via dispatcher ---------------------------------------------

test_that("data.upload.begin / chunk / end assembles a handle", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")

  full <- ipc_bytes(sample_dt())
  n <- length(full)
  c1 <- full[1L:floor(n / 2L)]
  c2 <- full[(floor(n / 2L) + 1L):n]

  begin <- dispatch_request(
    conn,
    make_request(
      "data.upload.begin",
      params = list(name = "chunked", total_bytes = n, n_chunks = 2L)
    ),
    server
  )
  expect_true(begin[["ok"]])
  uid <- begin[["result"]][["upload_id"]]

  c1_resp <- dispatch_request(
    conn,
    make_request(
      "data.upload.chunk",
      params = list(upload_id = uid, chunk_index = 1L),
      payload = c1
    ),
    server
  )
  expect_true(c1_resp[["ok"]])
  expect_equal(c1_resp[["result"]][["received_count"]], 1L)

  c2_resp <- dispatch_request(
    conn,
    make_request(
      "data.upload.chunk",
      params = list(upload_id = uid, chunk_index = 2L),
      payload = c2
    ),
    server
  )
  expect_true(c2_resp[["ok"]])

  end_resp <- dispatch_request(
    conn,
    make_request("data.upload.end", params = list(upload_id = uid)),
    server
  )
  expect_true(end_resp[["ok"]])
  expect_equal(end_resp[["result"]][["rows"]], 10L)
})

test_that("data.upload.cancel drops pending upload", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  begin <- dispatch_request(
    conn,
    make_request(
      "data.upload.begin",
      params = list(name = "x", total_bytes = 10L, n_chunks = 2L)
    ),
    server
  )
  uid <- begin[["result"]][["upload_id"]]
  resp <- dispatch_request(
    conn,
    make_request("data.upload.cancel", params = list(upload_id = uid)),
    server
  )
  expect_true(resp[["ok"]])
  expect_true(resp[["result"]][["cancelled"]])
})


# train -> job.list / status / cancel / result / delete ----------------------

test_that("train + job.list + job.status + job.result happy path (GLM regression)", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")

  # Small regression dataset matching rtemis conventions:
  # outcome on the last column.
  set.seed(2026L)
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
  expect_true(submitted[["ok"]])
  job_id <- submitted[["result"]][["job_id"]]
  expect_match(job_id, "^job-")

  # job.list should include it
  listed <- dispatch_request(conn, make_request("job.list"), server)
  expect_true(listed[["ok"]])
  ids <- vapply(listed[["result"]][["jobs"]], `[[`, character(1L), "job_id")
  expect_true(job_id %in% ids)

  # Wait for resolution
  s <- get_session_by_name("s")
  job <- s[["jobs"]][[job_id]]
  wait_for_resolved(job)
  check_job_resolved(job)
  expect_equal(job[["status"]], "complete")

  # job.status returns the summary
  status <- dispatch_request(
    conn,
    make_request("job.status", params = list(job_id = job_id)),
    server
  )
  expect_true(status[["ok"]])
  expect_equal(status[["result"]][["status"]], "complete")

  # job.result returns a to_json-shaped object
  result <- dispatch_request(
    conn,
    make_request("job.result", params = list(job_id = job_id)),
    server
  )
  expect_true(result[["ok"]])
  expect_equal(result[["result"]][[".class"]], "Regression")
  expect_equal(result[["result"]][["algorithm"]], "GLM")
})

test_that("train rejects missing data_handle / algorithm", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  resp <- dispatch_request(
    conn,
    make_request("train", params = list(algorithm = "glm")),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "invalid_params")
})

test_that("train accepts preprocessor_config and runs end-to-end", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")

  set.seed(2034L)
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
      params = list(
        data_handle = handle,
        algorithm = "glm",
        preprocessor_config = list(scale = TRUE, center = TRUE)
      )
    ),
    server
  )
  expect_true(submitted[["ok"]])
  job_id <- submitted[["result"]][["job_id"]]

  s <- get_session_by_name("s")
  job <- s[["jobs"]][[job_id]]
  wait_for_resolved(job)
  check_job_resolved(job)
  expect_equal(job[["status"]], "complete")
  expect_true(inherits(job[["result"]], "rtemis::Supervised"))
})

test_that("train rejects malformed preprocessor_config with invalid_params", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")

  set.seed(2035L)
  dt <- data.table(x = rnorm(30), y = rnorm(30))

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

  resp <- dispatch_request(
    conn,
    make_request(
      "train",
      params = list(
        data_handle = handle,
        algorithm = "glm",
        preprocessor_config = list(no_such_arg = TRUE)
      )
    ),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "invalid_params")
})

test_that("train with unknown data_handle -> not_found", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  resp <- dispatch_request(
    conn,
    make_request(
      "train",
      params = list(data_handle = "data-nope", algorithm = "glm")
    ),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "not_found")
})

test_that("job.status / job.cancel / job.delete / job.result errors", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")

  # Missing job_id
  for (m in c("job.status", "job.cancel", "job.delete", "job.result")) {
    r <- dispatch_request(conn, make_request(m), server)
    expect_false(r[["ok"]], info = m)
    expect_equal(r[["error"]][["code"]], "invalid_params", info = m)
  }

  # Unknown job_id
  for (m in c("job.status", "job.cancel")) {
    r <- dispatch_request(
      conn,
      make_request(m, params = list(job_id = "job-nope")),
      server
    )
    expect_false(r[["ok"]], info = m)
    expect_equal(r[["error"]][["code"]], "not_found", info = m)
  }
})

test_that("job.result before completion -> invalid_params (no result yet)", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")

  s <- connection_session(conn)
  job <- submit_job(s, "test", list(), expr = quote(Sys.sleep(1)))
  resp <- dispatch_request(
    conn,
    make_request("job.result", params = list(job_id = job[["id"]])),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "invalid_params")

  # cleanup: wait so daemons aren't busy when test file unloads
  wait_for_resolved(job)
  check_job_resolved(job)
})

test_that("job.result unsupported slice -> invalid_params", {
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
      params = list(job_id = job[["id"]], slice = "bogus")
    ),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "invalid_params")
})

test_that("job.result `predictions` on a non-Supervised result -> invalid_params", {
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
      params = list(job_id = job[["id"]], slice = "predictions")
    ),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "invalid_params")
})

test_that("job.cancel marks cancelling, then cancelled after resolution", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  s <- connection_session(conn)

  job <- submit_job(s, "test", list(), expr = quote(Sys.sleep(2)))

  resp <- dispatch_request(
    conn,
    make_request("job.cancel", params = list(job_id = job[["id"]])),
    server
  )
  expect_true(resp[["ok"]])
  expect_true(resp[["result"]][["cancelled"]])
  expect_equal(job[["status"]], "cancelling")

  wait_for_resolved(job)
  check_job_resolved(job)
  expect_equal(job[["status"]], "cancelled")
})

test_that("job.delete removes a finished job", {
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
    make_request("job.delete", params = list(job_id = job[["id"]])),
    server
  )
  expect_true(resp[["ok"]])
  expect_true(resp[["result"]][["deleted"]])
})

test_that("job.save writes the result to an .rds file and round-trips", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  s <- connection_session(conn)
  job <- submit_job(s, "test", list(), expr = quote(list(answer = 42L)))
  wait_for_resolved(job)
  check_job_resolved(job)

  dir <- file.path(tempdir(), basename(tempfile("savetest_")))
  on.exit(unlink(dir, recursive = TRUE), add = TRUE)
  resp <- dispatch_request(
    conn,
    make_request(
      "job.save",
      params = list(job_id = job[["id"]], dir = dir, filename = "obj")
    ),
    server
  )
  expect_true(resp[["ok"]])
  path <- resp[["result"]][["path"]]
  expect_true(file.exists(path))
  expect_match(path, "obj\\.rds$")
  expect_gt(resp[["result"]][["bytes"]], 0)
  expect_equal(readRDS(path), list(answer = 42L))
})

test_that("job.save defaults the filename from the job type + id", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  s <- connection_session(conn)
  job <- submit_job(s, "train", list(), expr = quote(1L))
  wait_for_resolved(job)
  check_job_resolved(job)

  dir <- file.path(tempdir(), basename(tempfile("savetest_")))
  on.exit(unlink(dir, recursive = TRUE), add = TRUE)
  resp <- dispatch_request(
    conn,
    make_request("job.save", params = list(job_id = job[["id"]], dir = dir)),
    server
  )
  expect_true(resp[["ok"]])
  expect_match(basename(resp[["result"]][["path"]]), "^train_.*\\.rds$")
})

test_that("job.save requires `dir`", {
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
    make_request("job.save", params = list(job_id = job[["id"]])),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "invalid_params")
})

test_that("job.save on an unresolved job -> invalid_params", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  s <- connection_session(conn)
  job <- submit_job(s, "test", list(), expr = quote(Sys.sleep(2)))
  resp <- dispatch_request(
    conn,
    make_request(
      "job.save",
      params = list(job_id = job[["id"]], dir = tempdir())
    ),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "invalid_params")
  wait_for_resolved(job)
  check_job_resolved(job)
})


# Method table coverage -----------------------------------------------------

test_that("method table now exposes all wire methods", {
  expected <- c(
    "auth",
    "ping",
    "info",
    "algorithms",
    "session.list",
    "session.create",
    "session.join",
    "session.detach",
    "session.rename",
    "session.delete",
    "session.info",
    "data.upload",
    "data.upload.begin",
    "data.upload.chunk",
    "data.upload.end",
    "data.upload.cancel",
    "data.list",
    "data.describe",
    "data.delete",
    "train",
    "job.list",
    "job.status",
    "job.cancel",
    "job.result",
    "job.delete",
    "job.save",
    "dialog.choose_dir"
  )
  expect_true(all(expected %in% names(.METHOD_TABLE)))
})
