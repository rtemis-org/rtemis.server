# test_dispatch_train_gate.R
# ::rtemis::
# 2026- EDG rtemis.org

# The `train` handler's pre-submission gate: the config is checked against the
# data before a job exists, so a run that cannot answer the question asked is
# refused rather than queued.
#
# No daemon pool is started here and no model is fitted. Every test below
# either refuses before `submit_job()` is reached, or inspects the response to
# an accepted submission -- the run itself belongs to the job tests.

skip_if_not_installed("arrow")
skip_if_not_installed("rtemis")

library(data.table)


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

# `minority` cases in the rarer outcome class; a constant factor column when
# `constant` is TRUE, which is a warning rather than an error.
sample_dt <- function(n = 60L, minority = 30L, constant = FALSE) {
  set.seed(2026L)
  dt <- data.table(
    x1 = rnorm(n),
    x2 = rnorm(n),
    y = factor(rep(c("no", "yes"), times = c(n - minority, minority)))
  )
  if (constant) {
    dt <- dt[, list(x1, x2, site = factor("A"), y)]
  }
  dt[]
}

upload <- function(conn, server, dt, name = "d") {
  resp <- dispatch_request(
    conn,
    make_request(
      "data.upload",
      params = list(name = name),
      payload = arrow::write_to_raw(dt, format = "stream")
    ),
    server
  )
  stopifnot(isTRUE(resp[["ok"]]))
  resp[["result"]][["data_handle"]]
}

train_request <- function(handle, ...) {
  make_request(
    "train",
    params = list(data_handle = handle, algorithm = "LightRF", ...)
  )
}


# The gate -------------------------------------------------------------------

test_that("a config that cannot run on the data is refused, not queued", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  # Ten folds over four minority cases: the run would fit folds a class never
  # reaches, and report a number computed on them.
  handle <- upload(conn, server, sample_dt(60L, minority = 4L))

  resp <- dispatch_request(
    conn,
    train_request(
      handle,
      outer_resampling_config = list(type = "KFold", n_resamples = 10L)
    ),
    server
  )

  expect_false(resp[["ok"]])
  expect_identical(resp[["error"]][["code"]], "invalid_config")
  # No job was created: a refused submission spends nothing.
  expect_null(resp[["result"]][["job_id"]])
})


test_that("the refusal carries the findings, not just a sentence", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  handle <- upload(conn, server, sample_dt(60L, minority = 4L))

  resp <- dispatch_request(
    conn,
    train_request(
      handle,
      outer_resampling_config = list(type = "KFold", n_resamples = 10L)
    ),
    server
  )

  found <- resp[["error"]][["details"]][["diagnostics"]]
  expect_gt(length(found), 0L)
  codes <- vapply(found, function(d) d[["code"]], character(1L))
  expect_true("RESAMPLE_MIN_CLASS" %in% codes)
  # Each finding arrives in the `diagnostic/v1` shape, plain-language account
  # included -- the same shape `config.validate` returns, so a client renders
  # both with one renderer.
  first <- found[[which(codes == "RESAMPLE_MIN_CLASS")[[1L]]]]
  for (key in c("code", "severity", "message", "plain", "evidence")) {
    expect_true(key %in% names(first), info = key)
  }
  expect_true(nzchar(first[["plain"]]))
})


test_that("a clean config is accepted and reports no findings", {
  skip_on_cran()
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  handle <- upload(conn, server, sample_dt())

  resp <- dispatch_request(
    conn,
    train_request(
      handle,
      outer_resampling_config = list(type = "KFold", n_resamples = 5L)
    ),
    server
  )

  expect_true(resp[["ok"]])
  expect_false(is.null(resp[["result"]][["job_id"]]))
  # Present and empty rather than absent: a caller reading `length()` should
  # not have to tell one from the other.
  expect_identical(resp[["result"]][["diagnostics"]], list())
})


test_that("a warning travels with the accepted job instead of only the log", {
  skip_on_cran()
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  # A zero-variance predictor: the run completes, and whether that result is
  # wanted is the submitter's judgment -- which they cannot make without being
  # told.
  handle <- upload(conn, server, sample_dt(constant = TRUE))

  resp <- dispatch_request(
    conn,
    train_request(
      handle,
      outer_resampling_config = list(type = "KFold", n_resamples = 5L)
    ),
    server
  )

  expect_true(resp[["ok"]])
  expect_false(is.null(resp[["result"]][["job_id"]]))
  found <- resp[["result"]][["diagnostics"]]
  expect_gt(length(found), 0L)
  expect_true(
    "FEATURE_CONSTANT" %in%
      vapply(found, function(d) d[["code"]], character(1L))
  )
  # Warnings do not block: every finding on an accepted job is one that let it
  # through.
  severities <- vapply(found, function(d) d[["severity"]], character(1L))
  expect_false("error" %in% severities)
})
