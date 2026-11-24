# test_serve_integration.R
# ::rtemis.server::
# 2026- EDG rtemis.org
#
# Lifecycle and authentication of a running `serve()`.
#
# Each server runs in a mirai daemon of the test's own compute profile, with
# the installed rtemis.server. `mirai::stop_mirai()` sends that daemon an
# interrupt, which is what Ctrl-C does, and the daemon survives it, so a
# second server can start in the same R process. mirai starts daemons with
# SIGINT ignored, so `tools::pskill()` has no effect on them. A test that
# observes the process from inside schedules a `later` callback, keeps what it
# finds in the global environment, and reads it after the server stops. The
# client is `helper-ws.R`.

skip_on_cran()

TOKEN <- "test-toke-aaaa-9999"
PROFILE <- "rtemis.server-test"


# Helpers ----
# A port that nothing uses, outside the range of the default port.
free_port <- function() {
  for (port in sample(20000:40000, 20L)) {
    con <- tryCatch(
      suppressWarnings(serverSocket(port)),
      error = function(e) NULL
    )
    if (!is.null(con)) {
      close(con)
      return(port)
    }
  }
  stop("No free port found.")
}

start_process <- function() {
  # `cleanup = FALSE` keeps the global environment between tasks.
  mirai::daemons(1L, cleanup = FALSE, .compute = PROFILE)
}

stop_process <- function() {
  mirai::daemons(0L, .compute = PROFILE)
}

start_test_server <- function(port, ...) {
  mirai::mirai(
    do.call(
      rtemis.server::serve,
      c(list(port = port, token = token, verbosity = 0L), args)
    ),
    port = port,
    token = TOKEN,
    args = list(...),
    .compute = PROFILE
  )
}

# Evaluate an expression in the process that ran the server.
in_process <- function(expr) {
  m <- mirai::mirai(.expr = expr, .compute = PROFILE)
  mirai::call_mirai(m)
  m[["data"]]
}

wait_until <- function(pred, timeout = 10) {
  deadline <- Sys.time() + timeout
  while (Sys.time() < deadline) {
    if (isTRUE(pred())) {
      return(TRUE)
    }
    Sys.sleep(0.05)
  }
  FALSE
}

auth <- function(ws, token = TOKEN) {
  ws_request(ws, "auth", list(token = token))
  ws_recv(ws)
}


# Authentication ----
test_that("the server closes the WebSocket after three failed attempts", {
  start_process()
  on.exit(stop_process(), add = TRUE)
  port <- free_port()
  m <- start_test_server(port)
  ws <- ws_connect(port)
  on.exit(ws_close(ws), add = TRUE)
  for (i in 1:3) {
    expect_identical(
      auth(ws, "wrong-token-wrong")[["error"]][["code"]],
      "unauthorized"
    )
  }
  expect_identical(ws_recv(ws), "closed")

  # The limit applies to one connection.
  other <- ws_connect(port)
  on.exit(ws_close(other), add = TRUE)
  expect_true(auth(other)[["ok"]])
  ws_request(other, "info")
  info <- ws_recv(other)
  expect_true(info[["ok"]])
  expect_equal(info[["result"]][["daemons"]], 1L)
})

test_that("no frame is dispatched after the last failed attempt", {
  start_process()
  on.exit(stop_process(), add = TRUE)
  port <- free_port()
  m <- start_test_server(port)
  ws <- ws_connect(port)
  on.exit(ws_close(ws), add = TRUE)
  for (i in 1:4) {
    ws_request(ws, "auth", list(token = "wrong-token-wrong"))
  }
  ws_request(ws, "auth", list(token = TOKEN))
  replies <- lapply(1:4, function(i) ws_recv(ws))
  for (reply in replies[1:3]) {
    expect_identical(reply[["error"]][["code"]], "unauthorized")
  }
  expect_identical(replies[[4L]], "closed")
})

test_that("a client without the token cannot stall the server", {
  start_process()
  on.exit(stop_process(), add = TRUE)
  port <- free_port()
  m <- start_test_server(port)
  # `ws$send()` waits for the client to read. These requests are not followed
  # by a read, and their replies together would exceed the socket buffers.
  flood <- ws_connect(port)
  on.exit(ws_close(flood), add = TRUE)
  ping <- ws_message(encode_frame(list(
    v = 1L,
    id = strrep("x", 3000L),
    method = "ping"
  )))
  # One write: a write to a closed socket waits for the socket's timeout.
  ws_write(flood, rep(ping, 500L))
  other <- ws_connect(port, timeout = 5)
  on.exit(ws_close(other), add = TRUE)
  expect_true(auth(other)[["ok"]])
})

test_that("a connection that does not authenticate is closed", {
  start_process()
  on.exit(stop_process(), add = TRUE)
  port <- free_port()
  m <- start_test_server(port)
  silent <- ws_connect(port)
  on.exit(ws_close(silent), add = TRUE)
  authed <- ws_connect(port)
  on.exit(ws_close(authed), add = TRUE)
  expect_true(auth(authed)[["ok"]])
  expect_identical(ws_recv(silent, timeout = 15), "closed")
  ws_request(authed, "ping")
  expect_true(ws_recv(authed)[["ok"]])
})

test_that("the origin is checked whatever the case of the header name", {
  start_process()
  on.exit(stop_process(), add = TRUE)
  port <- free_port()
  m <- start_test_server(port)
  ws_close(ws_connect(port))
  for (origin in list("https://evil.example.com", NULL)) {
    ws <- ws_open(port, origin)
    expect_identical(ws_recv(ws), "closed")
    ws_close(ws)
  }
  ws <- ws_open(port, "https://live.rtemis.org", header = "origin")
  expect_identical(ws_recv(ws)[["event"]], "ready")
  ws_close(ws)
})


# Stopping and restarting ----
test_that("an interrupt stops the server and releases everything", {
  start_process()
  on.exit(stop_process(), add = TRUE)
  port <- free_port()
  for (run in 1:3) {
    m <- start_test_server(port)
    ws <- ws_connect(port)
    expect_true(auth(ws)[["ok"]])
    ws_request(ws, "session.create", list(name = "kept"))
    expect_true(ws_recv(ws)[["ok"]])
    mirai::stop_mirai(m)
    expect_identical(ws_recv(ws), "closed")
    ws_close(ws)

    left <- in_process(quote({
      ns <- asNamespace("rtemis.server")
      list(
        loop_empty = later::loop_empty(),
        n_sessions = length(ls(ns$session_registry())),
        daemons = mirai::daemons_set(.compute = ns$.RTEMISLIVE_COMPUTE),
        verbosity = getOption("rtemis.server.verbosity")
      )
    }))
    expect_true(left[["loop_empty"]])
    expect_identical(left[["n_sessions"]], 0L)
    expect_false(left[["daemons"]])
    expect_null(left[["verbosity"]])
  }
})

test_that("the caller's daemons and tasks survive the server", {
  start_process()
  on.exit(stop_process(), add = TRUE)
  in_process(quote({
    mirai::daemons(1L)
    assign(
      "task",
      mirai::mirai({
        Sys.sleep(3)
        "done"
      }),
      envir = globalenv()
    )
  }))
  port <- free_port()
  m <- start_test_server(port)
  ws_close(ws_connect(port))
  mirai::stop_mirai(m)
  value <- in_process(quote({
    value <- mirai::call_mirai(get("task", envir = globalenv()))[["data"]]
    mirai::daemons(0L)
    value
  }))
  expect_identical(value, "done")
})

test_that("serve() prints nothing at verbosity 0", {
  start_process()
  on.exit(stop_process(), add = TRUE)
  port <- free_port()
  out_file <- tempfile("serve-out-")
  on.exit(unlink(out_file), add = TRUE)
  in_process(bquote({
    out <- file(.(out_file), "w")
    sink(out)
    sink(out, type = "message")
  }))
  m <- start_test_server(port)
  ws <- ws_connect(port)
  expect_true(auth(ws)[["ok"]])
  mirai::stop_mirai(m)
  expect_identical(ws_recv(ws), "closed")
  ws_close(ws)
  in_process(quote({
    sink(type = "message")
    sink()
    close(out)
  }))
  # R itself writes a newline when it is interrupted.
  expect_identical(Filter(nzchar, readLines(out_file)), character(0L))
})

test_that("a second serve() in the same process aborts, the first continues", {
  start_process()
  on.exit(stop_process(), add = TRUE)
  port <- free_port()
  in_process(bquote({
    later::later(
      function() {
        second <<- tryCatch(
          rtemis.server::serve(
            port = .(port + 1L),
            token = .(TOKEN),
            verbosity = 0L
          ),
          error = conditionMessage
        )
      },
      delay = 1
    )
    NULL
  }))
  m <- start_test_server(port)
  ws <- ws_connect(port)
  on.exit(ws_close(ws), add = TRUE)
  expect_true(auth(ws)[["ok"]])
  Sys.sleep(1.5)
  ws_request(ws, "ping")
  expect_true(ws_recv(ws)[["ok"]])
  mirai::stop_mirai(m)
  expect_match(in_process(quote(second)), "One server runs per R process")
})


# Startup failures ----
test_that("a busy port fails with its number and leaves nothing behind", {
  start_process()
  on.exit(stop_process(), add = TRUE)
  port <- free_port()
  # As in `serve()`, the event loop frees the port after the close.
  busy <- nanonext::http_server(
    url = paste0("http://127.0.0.1:", port),
    handlers = list(nanonext::handler("/", function(req) NULL))
  )
  busy$start()
  failed <- in_process(bquote(list(
    message = tryCatch(
      rtemis.server::serve(port = .(port), token = .(TOKEN), verbosity = 0L),
      error = conditionMessage
    ),
    daemons = mirai::daemons_set(.compute = .(.RTEMISLIVE_COMPUTE)),
    verbosity = getOption("rtemis.server.verbosity"),
    loop_empty = later::loop_empty()
  )))
  busy$close()
  later::run_now(0)
  expect_match(
    failed[["message"]],
    paste0("127.0.0.1:", port, " (Address"),
    fixed = TRUE
  )
  expect_false(failed[["daemons"]])
  expect_null(failed[["verbosity"]])
  expect_true(failed[["loop_empty"]])

  # The same process binds the port once it is free.
  m <- start_test_server(port)
  ws <- ws_connect(port)
  expect_true(auth(ws)[["ok"]])
  ws_close(ws)
})

test_that("serve() checks its arguments", {
  expect_error(serve(port = 80), "1024 to 49151")
  expect_error(serve(port = 5757.5), "1024 to 49151")
  expect_error(serve(host = "0.0.0.0"), "same machine")
  expect_error(serve(host = "::1"), "same machine")
  expect_error(serve(n_daemons = 0), "`n_daemons`")
  expect_error(serve(max_concurrent = 1.5), "`max_concurrent`")
  expect_error(serve(session_ttl = -1), "`session_ttl`")
  expect_error(serve(token = "short"), "at least 16")
  expect_error(serve(token = NA_character_), "at least 16")
  expect_error(serve(verbosity = 0L), "`token` is required")
})

test_that("resolve_port() reads the environment variable", {
  old <- Sys.getenv("RTEMISLIVE_PORT", unset = NA)
  on.exit(
    if (is.na(old)) {
      Sys.unsetenv("RTEMISLIVE_PORT")
    } else {
      Sys.setenv(RTEMISLIVE_PORT = old)
    }
  )
  Sys.setenv(RTEMISLIVE_PORT = "6001")
  expect_identical(resolve_port(NULL), 6001L)
  expect_identical(resolve_port(6002), 6002L)
  Sys.setenv(RTEMISLIVE_PORT = "abc")
  expect_error(resolve_port(NULL), "RTEMISLIVE_PORT")
  Sys.unsetenv("RTEMISLIVE_PORT")
  expect_identical(resolve_port(NULL), 5757L)
})


# Workers ----
test_that("a daemon that dies is replaced and its job fails readably", {
  start_process()
  on.exit(stop_process(), add = TRUE)
  port <- free_port()
  # The jobs are submitted from inside the server process: no wire method runs
  # arbitrary code.
  in_process(quote({
    later::later(
      function() {
        ns <- asNamespace("rtemis.server")
        s <- ns$new_session("crash")
        jobs <<- lapply(
          list(
            crash = quote(tools::pskill(Sys.getpid(), tools::SIGKILL)),
            after = quote(6 * 7)
          ),
          function(expr) {
            ns$submit_job(s, "test", list(), expr, max_concurrent = 1L)
          }
        )
      },
      delay = 1
    )
    NULL
  }))
  m <- start_test_server(port)
  ws <- ws_connect(port)
  on.exit(ws_close(ws), add = TRUE)
  expect_true(auth(ws)[["ok"]])
  ws_request(ws, "session.join", list(name = "crash"))
  ws_recv(ws)
  finished <- wait_until(
    function() {
      ws_request(ws, "session.join", list(name = "crash"))
      ws_request(ws, "job.list")
      repeat {
        reply <- ws_recv(ws)
        if (is.null(reply)) {
          return(FALSE)
        }
        if (identical(reply[["id"]], "job.list")) {
          jobs <- reply[["result"]][["jobs"]]
          return(
            length(jobs) == 2L &&
              all(
                vapply(jobs, `[[`, character(1L), "status") %in%
                  c("failed", "complete")
              )
          )
        }
      }
    },
    timeout = 30
  )
  expect_true(finished)
  mirai::stop_mirai(m)
  out <- in_process(quote(lapply(
    jobs,
    function(j) mget(c("status", "error", "result"), envir = j)
  )))
  expect_identical(out[["crash"]][["status"]], "failed")
  expect_match(out[["crash"]][["error"]][["message"]], "worker process ended")
  expect_identical(out[["after"]][["status"]], "complete")
  expect_identical(out[["after"]][["result"]], 42)
})


test_that("a job reports progress and its result over the WebSocket", {
  start_process()
  on.exit(stop_process(), add = TRUE)
  port <- free_port()
  m <- start_test_server(port)
  ws <- ws_connect(port)
  on.exit(ws_close(ws), add = TRUE)
  expect_true(auth(ws)[["ok"]])
  ws_request(ws, "session.create")
  expect_true(ws_recv(ws)[["ok"]])
  ws_send_raw(
    ws,
    encode_frame(
      list(
        v = 1L,
        id = "upload",
        method = "data.upload",
        params = list(name = "cars")
      ),
      payload = encode_arrow_ipc(mtcars[, c("wt", "hp", "mpg")])
    )
  )
  handle <- ws_recv(ws)[["result"]][["data_handle"]]
  ws_request(ws, "train", list(data_handle = handle, algorithm = "GLM"))
  events <- character(0L)
  deadline <- Sys.time() + 60
  while (Sys.time() < deadline && !"job.complete" %in% events) {
    reply <- ws_recv(ws)
    events <- c(events, if (is.list(reply)) reply[["event"]])
  }
  expect_true("job.progress" %in% events)
  expect_true("job.complete" %in% events)
})
