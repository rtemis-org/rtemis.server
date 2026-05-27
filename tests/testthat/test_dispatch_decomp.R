# test_dispatch_decomp.R
# ::rtemis::
# 2026- EDG rtemis.org

# Dispatcher tests for the decomp.* and decomp job handlers. Mirrors
# the patterns in test_dispatch_data_jobs.R. Test helpers are
# duplicated here to match the per-file convention used elsewhere in
# this directory.

skip_if_not_installed("arrow")
skip_if_not_installed("mirai")
skip_if_not_installed("rtemis")

library(data.table)


# File-level: ensure a daemon pool is available for the job-flow tests.
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

wait_for_resolved <- function(job, timeout = 30) {
  start <- Sys.time()
  while (mirai::unresolved(job[["mirai"]])) {
    if (as.numeric(difftime(Sys.time(), start, units = "secs")) > timeout) {
      stop("Timed out waiting for job to resolve")
    }
    Sys.sleep(0.02)
  }
}


# decomp.algorithms ----------------------------------------------------------

test_that("decomp.algorithms returns the catalogue with name + description", {
  server <- make_server()
  conn <- authed_conn(server)
  resp <- dispatch_request(
    conn,
    make_request("decomp.algorithms"),
    server
  )
  expect_true(resp[["ok"]])
  algos <- resp[["result"]][["algorithms"]]
  expect_true(length(algos) > 0L)
  names_seen <- vapply(algos, `[[`, character(1L), "name")
  expect_true("PCA" %in% names_seen)
  expect_true("UMAP" %in% names_seen)
  # Each entry has both fields.
  expect_true(all(vapply(
    algos,
    function(a) all(c("name", "description") %in% names(a)),
    logical(1L)
  )))
})


# decomp.algorithm.describe --------------------------------------------------

test_that("decomp.algorithm.describe returns a config schema for PCA", {
  server <- make_server()
  conn <- authed_conn(server)
  resp <- dispatch_request(
    conn,
    make_request(
      "decomp.algorithm.describe",
      params = list(name = "PCA")
    ),
    server
  )
  expect_true(resp[["ok"]])
  expect_equal(resp[["result"]][["name"]], "PCA")
  hps <- resp[["result"]][["hyperparameters"]]
  hp_names <- vapply(hps, `[[`, character(1L), "name")
  expect_true(all(c("k", "center", "scale", "tol") %in% hp_names))
  # No tunable concept for decomposition.
  expect_true(all(vapply(hps, `[[`, logical(1L), "tunable") == FALSE))
})

test_that("decomp.algorithm.describe unknown algorithm -> not_found", {
  server <- make_server()
  conn <- authed_conn(server)
  resp <- dispatch_request(
    conn,
    make_request(
      "decomp.algorithm.describe",
      params = list(name = "NotARealAlgo")
    ),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "not_found")
})


# decomp job submission ------------------------------------------------------

test_that("decomp + job.status + job.result happy path (PCA)", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")

  set.seed(2026L)
  dt <- data.table(
    a = rnorm(40),
    b = rnorm(40),
    c = rnorm(40),
    d = rnorm(40)
  )

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
      "decomp",
      params = list(
        data_handle = handle,
        algorithm = "PCA",
        hyperparameters = list(k = 2L)
      )
    ),
    server
  )
  expect_true(submitted[["ok"]])
  job_id <- submitted[["result"]][["job_id"]]
  expect_match(job_id, "^job-")

  s <- get_session_by_name("s")
  job <- s[["jobs"]][[job_id]]
  wait_for_resolved(job)
  check_job_resolved(job)
  expect_equal(job[["status"]], "complete")

  # job.status confirms completion
  status <- dispatch_request(
    conn,
    make_request("job.status", params = list(job_id = job_id)),
    server
  )
  expect_equal(status[["result"]][["status"]], "complete")

  # job.result default (summary): the Decomposition's summary
  result <- dispatch_request(
    conn,
    make_request("job.result", params = list(job_id = job_id)),
    server
  )
  expect_true(result[["ok"]])
  expect_equal(result[["result"]][[".class"]], "Decomposition")
  expect_equal(result[["result"]][["algorithm"]], "PCA")
  # Heavy fields stripped from summary
  expect_true(
    is.null(result[["result"]][["transformed"]]) ||
      isTRUE(result[["result"]][["transformed"]][["available"]])
  )

  # `transformed` slice: arrow IPC pointer with 40 rows × 2 cols. Binary
  # slices come back as `list(header, payload)` (unwrapped by the loop
  # layer in production).
  tr <- dispatch_request(
    conn,
    make_request(
      "job.result",
      params = list(job_id = job_id, slice = "transformed")
    ),
    server
  )
  expect_true(is.raw(tr[["payload"]]))
  tr_hdr <- tr[["header"]]
  expect_true(tr_hdr[["ok"]])
  expect_equal(tr_hdr[["result"]][["rows"]], 40L)
  expect_equal(tr_hdr[["result"]][["cols"]], 2L)
  expect_equal(tr_hdr[["result"]][["format"]], "arrow-ipc")

  # `loadings` slice: PCA exposes rotation - 4 variables × 2 PCs, plus
  # a leading `variable` column.
  ld <- dispatch_request(
    conn,
    make_request(
      "job.result",
      params = list(job_id = job_id, slice = "loadings")
    ),
    server
  )
  ld_hdr <- ld[["header"]]
  expect_true(ld_hdr[["ok"]])
  expect_equal(ld_hdr[["result"]][["rows"]], 4L)
  expect_true("variable" %in% ld_hdr[["result"]][["columns"]])
})

test_that("decomp accepts a features subset and uses only those columns", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")

  set.seed(2027L)
  dt <- data.table(
    a = rnorm(30),
    b = rnorm(30),
    c = rnorm(30),
    d = rnorm(30)
  )

  upload <- dispatch_request(
    conn,
    make_request(
      "data.upload",
      params = list(name = "subset"),
      payload = ipc_bytes(dt)
    ),
    server
  )
  handle <- upload[["result"]][["data_handle"]]

  submitted <- dispatch_request(
    conn,
    make_request(
      "decomp",
      params = list(
        data_handle = handle,
        algorithm = "PCA",
        hyperparameters = list(k = 2L),
        features = c("a", "c")
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

  # Loadings should have exactly 2 variable rows: `a` and `c`.
  ld <- dispatch_request(
    conn,
    make_request(
      "job.result",
      params = list(job_id = job_id, slice = "loadings")
    ),
    server
  )
  expect_equal(ld[["header"]][["result"]][["rows"]], 2L)
})

test_that("decomp rejects missing data_handle / algorithm", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  resp <- dispatch_request(
    conn,
    make_request("decomp", params = list(algorithm = "PCA")),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "invalid_params")
})

test_that("decomp rejects unknown algorithm -> not_found", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  resp <- dispatch_request(
    conn,
    make_request(
      "decomp",
      params = list(data_handle = "data-x", algorithm = "NotReal")
    ),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "not_found")
})

test_that("decomp rejects features not in dataset -> invalid_params", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")

  dt <- data.table(a = rnorm(10), b = rnorm(10))
  upload <- dispatch_request(
    conn,
    make_request(
      "data.upload",
      params = list(name = "tiny"),
      payload = ipc_bytes(dt)
    ),
    server
  )
  handle <- upload[["result"]][["data_handle"]]

  resp <- dispatch_request(
    conn,
    make_request(
      "decomp",
      params = list(
        data_handle = handle,
        algorithm = "PCA",
        features = c("a", "missing_col")
      )
    ),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "invalid_params")
  expect_match(resp[["error"]][["message"]], "missing_col")
})


# Cross-type slice guards ----------------------------------------------------

test_that("job.result `transformed` slice on a non-Decomposition -> invalid_params", {
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
      params = list(job_id = job[["id"]], slice = "transformed")
    ),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "invalid_params")
})

test_that("job.result `loadings` slice on a non-Decomposition -> invalid_params", {
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
      params = list(job_id = job[["id"]], slice = "loadings")
    ),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "invalid_params")
})


# Dispatch table -------------------------------------------------------------

test_that("dispatch table exposes decomp.* and decomp methods", {
  expected <- c(
    "decomp.algorithms",
    "decomp.algorithm.describe",
    "decomp"
  )
  expect_true(all(expected %in% names(.METHOD_TABLE)))
})
