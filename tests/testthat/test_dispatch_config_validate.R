# test_dispatch_config_validate.R
# ::rtemis::
# 2026- EDG rtemis.org

# Dispatcher tests for `config.validate`. Synchronous, so unlike `train` there
# is no job to wait on and no daemon pool to start: the diagnostics come back
# in the response to the request.

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

.schema_id <- "https://schema.rtemis.org/supervised/v1/schema.json"

# The wire decodes with `simplifyVector = FALSE`, so a JSON array arrives as a
# list of length-1 atomics. Fixtures are written that way on purpose: it is the
# shape the handler has to collapse.
wire_config <- function(...) {
  list(
    hyperparameters = list(algorithm = "LightRF", hyperparameters = list()),
    ...
  )
}

# 40 rows, two numeric predictors, a binary factor outcome with `minority`
# cases in the rarer class.
sample_dt <- function(n = 40L, minority = 20L) {
  set.seed(2026L)
  data.table(
    x1 = rnorm(n),
    x2 = rnorm(n),
    y = factor(rep(c("no", "yes"), times = c(n - minority, minority)))
  )
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


# config.validate: schema only ----------------------------------------------

test_that("config.validate returns no diagnostics for a clean config", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")

  resp <- dispatch_request(
    conn,
    make_request(
      "config.validate",
      params = list(schema_id = .schema_id, config = wire_config())
    ),
    server
  )
  expect_true(resp[["ok"]])
  expect_identical(resp[["result"]][["diagnostics"]], list())
})


test_that("config.validate reports an unreadable config as SCHEMA_INVALID", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")

  resp <- dispatch_request(
    conn,
    make_request(
      "config.validate",
      params = list(
        schema_id = .schema_id,
        config = wire_config(n_foldz = 10L)
      )
    ),
    server
  )
  expect_true(resp[["ok"]])
  diags <- resp[["result"]][["diagnostics"]]
  expect_length(diags, 1L)
  expect_identical(diags[[1L]][["code"]], "SCHEMA_INVALID")
  expect_identical(diags[[1L]][["severity"]], "error")
  # A finding is a report, not a transport failure: the response is `ok`.
  expect_true(nzchar(diags[[1L]][["plain"]]))
})


test_that("config.validate takes the schema from schema_id", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")

  resp <- dispatch_request(
    conn,
    make_request(
      "config.validate",
      params = list(
        schema_id = "https://example.com/not-a-schema.json",
        config = wire_config()
      )
    ),
    server
  )
  expect_true(resp[["ok"]])
  expect_identical(
    resp[["result"]][["diagnostics"]][[1L]][["code"]],
    "SCHEMA_INVALID"
  )
})


# config.validate: with data -------------------------------------------------

test_that("config.validate checks a config against an uploaded dataset", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  handle <- upload(conn, server, sample_dt(60L, minority = 4L))

  resp <- dispatch_request(
    conn,
    make_request(
      "config.validate",
      params = list(
        schema_id = .schema_id,
        config = wire_config(
          outer_resampling_config = list(type = "KFold", n_resamples = 10L)
        ),
        data_handle = handle
      )
    ),
    server
  )
  expect_true(resp[["ok"]])
  diags <- resp[["result"]][["diagnostics"]]
  expect_length(diags, 1L)
  expect_identical(diags[[1L]][["code"]], "RESAMPLE_MIN_CLASS")
  expect_identical(diags[[1L]][["severity"]], "error")
  expect_identical(diags[[1L]][["evidence"]][["min_class_n"]], 4L)
  # The fix travels as an RFC 6902 patch the client can apply.
  expect_identical(
    diags[[1L]][["fix"]][[1L]][["path"]],
    "/outer_resampling_config/n_resamples"
  )
})


test_that("config.validate returns no diagnostics when the config fits", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  handle <- upload(conn, server, sample_dt(60L, minority = 30L))

  resp <- dispatch_request(
    conn,
    make_request(
      "config.validate",
      params = list(
        schema_id = .schema_id,
        config = wire_config(
          outer_resampling_config = list(type = "KFold", n_resamples = 5L)
        ),
        data_handle = handle
      )
    ),
    server
  )
  expect_true(resp[["ok"]])
  expect_identical(resp[["result"]][["diagnostics"]], list())
})


test_that("config.validate honors an explicit outcome column", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  handle <- upload(conn, server, sample_dt())

  resp <- dispatch_request(
    conn,
    make_request(
      "config.validate",
      params = list(
        schema_id = .schema_id,
        config = wire_config(),
        data_handle = handle,
        outcome = "readmit"
      )
    ),
    server
  )
  expect_true(resp[["ok"]])
  diags <- resp[["result"]][["diagnostics"]]
  expect_identical(diags[[1L]][["code"]], "OUTCOME_MISSING")
  expect_identical(diags[[1L]][["evidence"]][["outcome"]], "readmit")
})


test_that("config.validate reads an empty outcome as no selection", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")
  handle <- upload(conn, server, sample_dt())

  resp <- dispatch_request(
    conn,
    make_request(
      "config.validate",
      params = list(
        schema_id = .schema_id,
        config = wire_config(),
        data_handle = handle,
        outcome = ""
      )
    ),
    server
  )
  expect_true(resp[["ok"]])
  # Not OUTCOME_MISSING: an empty selection means "use the convention", so the
  # last column is the outcome and the config is clean.
  expect_identical(resp[["result"]][["diagnostics"]], list())
})


test_that("config.validate records the plan step on every finding", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")

  # A JSON number decodes to a double, so the step arrives as `2` rather than
  # `2L`. Written that way here because that is what a client actually sends.
  resp <- dispatch_request(
    conn,
    make_request(
      "config.validate",
      params = list(
        schema_id = .schema_id,
        config = wire_config(n_foldz = 1L),
        step = 2
      )
    ),
    server
  )
  expect_identical(resp[["result"]][["diagnostics"]][[1L]][["step"]], 2L)
})


test_that("config.validate collapses JSON arrays before validating", {
  # `simplifyVector = FALSE` turns `["a", "b"]` into a list of two length-1
  # strings, which `setup_Preprocessor()` would reject as a scalar field.
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")

  resp <- dispatch_request(
    conn,
    make_request(
      "config.validate",
      params = list(
        schema_id = .schema_id,
        config = wire_config(
          preprocessor_config = list(remove_features = list("x1", "x2"))
        )
      )
    ),
    server
  )
  expect_true(resp[["ok"]])
  expect_identical(resp[["result"]][["diagnostics"]], list())
})


# config.validate: errors ----------------------------------------------------

test_that("config.validate without schema_id or config -> invalid_params", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")

  resp <- dispatch_request(
    conn,
    make_request("config.validate", params = list(config = wire_config())),
    server
  )
  expect_false(resp[["ok"]])
  expect_identical(resp[["error"]][["code"]], "invalid_params")

  resp2 <- dispatch_request(
    conn,
    make_request("config.validate", params = list(schema_id = .schema_id)),
    server
  )
  expect_false(resp2[["ok"]])
  expect_identical(resp2[["error"]][["code"]], "invalid_params")
})


test_that("config.validate with an unknown data_handle -> not_found", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "s")

  resp <- dispatch_request(
    conn,
    make_request(
      "config.validate",
      params = list(
        schema_id = .schema_id,
        config = wire_config(),
        data_handle = "dh-nope"
      )
    ),
    server
  )
  expect_false(resp[["ok"]])
  expect_identical(resp[["error"]][["code"]], "not_found")
})


test_that("config.validate requires auth and a session", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()

  resp <- dispatch_request(
    new_connection(),
    make_request("config.validate", params = list(schema_id = .schema_id)),
    server
  )
  expect_identical(resp[["error"]][["code"]], "unauthorized")

  resp2 <- dispatch_request(
    authed_conn(server),
    make_request("config.validate", params = list(schema_id = .schema_id)),
    server
  )
  expect_identical(resp2[["error"]][["code"]], "not_attached")
})
