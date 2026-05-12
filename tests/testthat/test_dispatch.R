# test_dispatch.R
# ::rtemis::
# 2026- EDG rtemis.org

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
  req <- make_request("auth", params = list(token = server[["token"]]))
  resp <- dispatch_request(conn, req, server)
  stopifnot(isTRUE(resp[["ok"]]))
  if (!is.null(attach_session)) {
    req2 <- make_request("session.create", params = list(name = attach_session))
    resp2 <- dispatch_request(conn, req2, server)
    stopifnot(isTRUE(resp2[["ok"]]))
  }
  conn
}


# new_connection / new_connection_id ----
test_that("new_connection_id() returns conn-<hex16>", {
  expect_match(new_connection_id(), "^conn-[0-9a-f]{16}$")
})

test_that("new_connection() initializes a fresh env", {
  conn <- new_connection()
  expect_true(is.environment(conn))
  expect_match(conn[["id"]], "^conn-")
  expect_false(conn[["authed"]])
  expect_null(conn[["session_id"]])
  expect_equal(conn[["auth_attempts"]], 0L)
  expect_false(conn[["close_after_response"]])
})


# Dispatcher: malformed / unknown / unauthorized -----------------------------

test_that("dispatch_request() returns malformed_frame when header is not a list", {
  server <- make_server()
  conn <- new_connection()
  resp <- dispatch_request(conn, list(header = "nope"), server)
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "malformed_frame")
})

test_that("dispatch_request() returns malformed_frame when method is missing", {
  server <- make_server()
  conn <- new_connection()
  resp <- dispatch_request(
    conn,
    list(header = list(v = 1L, id = "r")),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "malformed_frame")
})

test_that("dispatch_request() returns unknown_method for unrecognized method", {
  server <- make_server()
  conn <- new_connection()
  conn[["authed"]] <- TRUE
  resp <- dispatch_request(conn, make_request("bogus.method"), server)
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "unknown_method")
})

test_that("dispatch_request() returns unauthorized for authed-only method when not authed", {
  server <- make_server()
  conn <- new_connection()
  resp <- dispatch_request(conn, make_request("ping"), server)
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "unauthorized")
})


# auth -----------------------------------------------------------------------

test_that("auth happy path marks conn authed and returns connection_id", {
  server <- make_server()
  conn <- new_connection()
  resp <- dispatch_request(
    conn,
    make_request("auth", params = list(token = server[["token"]])),
    server
  )
  expect_true(resp[["ok"]])
  expect_equal(resp[["result"]][["connection_id"]], conn[["id"]])
  expect_true(conn[["authed"]])
  expect_equal(conn[["auth_attempts"]], 0L)
})

test_that("auth with bad token fails and increments auth_attempts", {
  server <- make_server()
  conn <- new_connection()
  resp <- dispatch_request(
    conn,
    make_request("auth", params = list(token = "wrong")),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "unauthorized")
  expect_false(conn[["authed"]])
  expect_equal(conn[["auth_attempts"]], 1L)
})

test_that("three failed auth attempts mark connection for closure", {
  server <- make_server()
  conn <- new_connection()
  for (i in 1:3) {
    dispatch_request(
      conn,
      make_request("auth", params = list(token = "wrong")),
      server
    )
  }
  expect_true(conn[["close_after_response"]])
})


# ping / info / algorithms ---------------------------------------------------

test_that("ping returns ok with a timestamp", {
  server <- make_server()
  conn <- authed_conn(server)
  resp <- dispatch_request(conn, make_request("ping"), server)
  expect_true(resp[["ok"]])
  expect_match(resp[["result"]][["ts"]], "^[0-9]{4}-[0-9]{2}-[0-9]{2}T")
})

test_that("info returns server metadata", {
  server <- make_server()
  conn <- authed_conn(server)
  resp <- dispatch_request(conn, make_request("info"), server)
  expect_true(resp[["ok"]])
  r <- resp[["result"]]
  expect_equal(r[["server"]], "rtemislive")
  expect_true(is.character(r[["r_version"]]))
  expect_true(is.numeric(r[["uptime_seconds"]]))
  expect_true(is.numeric(r[["n_sessions"]]))
})

test_that("algorithms returns a list of available supervised algorithms", {
  server <- make_server()
  conn <- authed_conn(server)
  resp <- dispatch_request(conn, make_request("algorithms"), server)
  expect_true(resp[["ok"]])
  algos <- resp[["result"]][["algorithms"]]
  expect_true(is.list(algos))
  expect_gt(length(algos), 0L)
  first <- algos[[1L]]
  expect_named(
    first,
    c(
      "name",
      "description",
      "supports_classification",
      "supports_regression",
      "supports_survival"
    ),
    ignore.order = TRUE
  )
  names <- vapply(algos, `[[`, character(1L), "name")
  expect_true("GLM" %in% names)
  expect_true("LightRF" %in% names)
})

test_that("algorithm.describe returns a hyperparameter schema for GLM", {
  server <- make_server()
  conn <- authed_conn(server)
  resp <- dispatch_request(
    conn,
    make_request("algorithm.describe", params = list(name = "GLM")),
    server
  )
  expect_true(resp[["ok"]])
  expect_equal(resp[["result"]][["name"]], "GLM")
  hps <- resp[["result"]][["hyperparameters"]]
  expect_true(is.list(hps))
  expect_gt(length(hps), 0L)
  ifw <- hps[[which(vapply(hps, `[[`, character(1L), "name") == "ifw")]]
  expect_equal(ifw[["type"]], "logical")
  expect_equal(ifw[["default"]], FALSE)
  expect_true(ifw[["tunable"]])
})

test_that("algorithm.describe marks fixed vs tunable hyperparameters", {
  server <- make_server()
  conn <- authed_conn(server)
  resp <- dispatch_request(
    conn,
    make_request("algorithm.describe", params = list(name = "LightRF")),
    server
  )
  expect_true(resp[["ok"]])
  hps <- resp[["result"]][["hyperparameters"]]
  by_name <- setNames(hps, vapply(hps, `[[`, character(1L), "name"))
  # nrounds is tunable, force_col_wise is fixed
  expect_true(by_name[["nrounds"]][["tunable"]])
  expect_false(by_name[["force_col_wise"]][["tunable"]])
  expect_equal(by_name[["nrounds"]][["type"]], "integer")
  expect_equal(by_name[["nrounds"]][["default"]], 500L)
})

test_that("algorithm.describe rejects unknown algorithm", {
  server <- make_server()
  conn <- authed_conn(server)
  resp <- dispatch_request(
    conn,
    make_request("algorithm.describe", params = list(name = "NotAnAlg")),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "not_found")
})

test_that("algorithm.describe requires a name", {
  server <- make_server()
  conn <- authed_conn(server)
  resp <- dispatch_request(
    conn,
    make_request("algorithm.describe"),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "invalid_params")
})

test_that("resampler.describe returns the setup_Resampler schema with choices", {
  server <- make_server()
  conn <- authed_conn(server)
  resp <- dispatch_request(
    conn,
    make_request("resampler.describe"),
    server
  )
  expect_true(resp[["ok"]])
  params <- resp[["result"]][["parameters"]]
  expect_true(is.list(params))
  expect_gt(length(params), 0L)
  by_name <- setNames(params, vapply(params, `[[`, character(1L), "name"))
  # type is an enum; choices preserved, first value as default
  expect_true("choices" %in% names(by_name[["type"]]))
  expect_true("KFold" %in% by_name[["type"]][["choices"]])
  expect_equal(by_name[["type"]][["default"]], "KFold")
  # n_resamples is integer, default 10L, not tunable
  expect_equal(by_name[["n_resamples"]][["type"]], "integer")
  expect_equal(by_name[["n_resamples"]][["default"]], 10L)
  expect_false(by_name[["n_resamples"]][["tunable"]])
})


# session.list / create / join / info / detach / delete / rename ------------

test_that("session.list starts empty and grows after session.create", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server)

  resp <- dispatch_request(conn, make_request("session.list"), server)
  expect_true(resp[["ok"]])
  expect_equal(resp[["result"]][["sessions"]], list())

  dispatch_request(
    conn,
    make_request("session.create", params = list(name = "iris")),
    server
  )

  conn2 <- authed_conn(server)
  resp2 <- dispatch_request(conn2, make_request("session.list"), server)
  expect_length(resp2[["result"]][["sessions"]], 1L)
  expect_equal(resp2[["result"]][["sessions"]][[1L]][["name"]], "iris")
})

test_that("session.create attaches the calling connection", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server)

  resp <- dispatch_request(
    conn,
    make_request("session.create", params = list(name = "iris")),
    server
  )
  expect_true(resp[["ok"]])
  expect_equal(resp[["result"]][["name"]], "iris")
  expect_match(resp[["result"]][["session_id"]], "^sess-")
  expect_equal(conn[["session_id"]], resp[["result"]][["session_id"]])

  s <- get_session_by_name("iris")
  expect_true(conn[["id"]] %in% s[["connections"]])
})

test_that("session.create with duplicate name → session_exists", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server)
  dispatch_request(
    conn,
    make_request("session.create", params = list(name = "x")),
    server
  )

  conn2 <- authed_conn(server)
  resp <- dispatch_request(
    conn2,
    make_request("session.create", params = list(name = "x")),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "session_exists")
})

test_that("session.create with invalid name → invalid_name", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server)
  resp <- dispatch_request(
    conn,
    make_request("session.create", params = list(name = "bad name")),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "invalid_name")
})

test_that("session.create while attached → invalid_params (must detach first)", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "a")
  resp <- dispatch_request(
    conn,
    make_request("session.create", params = list(name = "b")),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "invalid_params")
})

test_that("session.join by name and by id both attach", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn1 <- authed_conn(server)
  resp_create <- dispatch_request(
    conn1,
    make_request("session.create", params = list(name = "iris")),
    server
  )
  sid <- resp_create[["result"]][["session_id"]]

  # Join by name
  conn2 <- authed_conn(server)
  resp_byname <- dispatch_request(
    conn2,
    make_request("session.join", params = list(name = "iris")),
    server
  )
  expect_true(resp_byname[["ok"]])
  expect_equal(conn2[["session_id"]], sid)

  # Join by id
  conn3 <- authed_conn(server)
  resp_byid <- dispatch_request(
    conn3,
    make_request("session.join", params = list(id = sid)),
    server
  )
  expect_true(resp_byid[["ok"]])
  expect_equal(conn3[["session_id"]], sid)
})

test_that("session.join unknown → session_not_found", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server)
  resp <- dispatch_request(
    conn,
    make_request("session.join", params = list(name = "nope")),
    server
  )
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "session_not_found")
})

test_that("session.detach drops session attachment but keeps the session", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "iris")
  sid <- conn[["session_id"]]

  resp <- dispatch_request(conn, make_request("session.detach"), server)
  expect_true(resp[["ok"]])
  expect_null(conn[["session_id"]])
  # Session is still in the registry
  expect_false(is.null(get_session_by_id(sid)))
})

test_that("session.info returns the snapshot for the attached session", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "iris")
  resp <- dispatch_request(conn, make_request("session.info"), server)
  expect_true(resp[["ok"]])
  expect_equal(resp[["result"]][["name"]], "iris")
  expect_equal(resp[["result"]][["n_connections"]], 1L)
})

test_that("session.rename updates name", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "iris")
  resp <- dispatch_request(
    conn,
    make_request("session.rename", params = list(name = "iris-renamed")),
    server
  )
  expect_true(resp[["ok"]])
  expect_equal(resp[["result"]][["name"]], "iris-renamed")
})

test_that("session.rename without name → invalid_params", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "iris")
  resp <- dispatch_request(conn, make_request("session.rename"), server)
  expect_false(resp[["ok"]])
  expect_equal(resp[["error"]][["code"]], "invalid_params")
})

test_that("session.delete removes the session and detaches", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server, attach_session = "iris")
  sid <- conn[["session_id"]]

  resp <- dispatch_request(conn, make_request("session.delete"), server)
  expect_true(resp[["ok"]])
  expect_null(conn[["session_id"]])
  expect_null(get_session_by_id(sid))
})

test_that("session.* attached-only methods refuse when unattached", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  server <- make_server()
  conn <- authed_conn(server)

  for (method in c(
    "session.info",
    "session.detach",
    "session.delete",
    "session.rename"
  )) {
    resp <- dispatch_request(conn, make_request(method), server)
    expect_false(resp[["ok"]])
    expect_equal(resp[["error"]][["code"]], "not_attached", info = method)
  }
})


# id propagation -------------------------------------------------------------

test_that("response echoes the request id", {
  server <- make_server()
  conn <- authed_conn(server)
  rid <- "req-echo-test-123"
  resp <- dispatch_request(conn, make_request("ping", id = rid), server)
  expect_equal(resp[["id"]], rid)
})

test_that("error responses still echo the request id", {
  server <- make_server()
  conn <- new_connection()
  rid <- "req-echo-err-456"
  resp <- dispatch_request(conn, make_request("ping", id = rid), server)
  expect_equal(resp[["id"]], rid)
  expect_false(resp[["ok"]])
})
