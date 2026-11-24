# test_loop.R
# ::rtemis::
# 2026- EDG rtemis.org

# Helpers --------------------------------------------------------------------

make_server <- function(
  token = "test-toke-nnnn-9999",
  heartbeat_interval = 5,
  gc_interval = 60
) {
  new_server_state(
    token = token,
    heartbeat_interval = heartbeat_interval,
    gc_interval = gc_interval
  )
}

# A connection whose send_raw closure captures outbound frames into a list.
captor_conn <- function() {
  conn <- new_connection()
  conn[["sent"]] <- list()
  conn[["send_raw"]] <- function(bytes) {
    conn[["sent"]][[length(conn[["sent"]]) + 1L]] <<- bytes
    invisible(NULL)
  }
  conn
}

# Deliver a request to a connection as one WebSocket message.
deliver <- function(conn, server, frame_list, payload = NULL) {
  handle_message(conn, encode_frame(frame_list, payload = payload), server)
}

# Decode every frame captured by a captor connection.
captured_decoded <- function(conn) {
  lapply(conn[["sent"]], function(b) decode_frame(b)[["header"]])
}

req <- function(method, params = NULL, id = NULL) {
  list(
    v = 1L,
    id = id %||% paste0("req-", basename(tempfile())),
    method = method,
    params = params
  )
}


# new_server_state ----
test_that("new_server_state() returns an env with mutable fields", {
  s <- make_server()
  expect_true(is.environment(s))
  expect_equal(s[["token"]], "test-toke-nnnn-9999")
  expect_true(is.environment(s[["connections"]]))
  expect_s3_class(s[["started_at"]], "POSIXct")
  expect_false(s[["running"]])
})


# register_connection / disconnect_connection ----
test_that("register_connection() puts the connection in server$connections", {
  s <- make_server()
  conn <- new_connection()
  register_connection(s, conn)
  expect_identical(s[["connections"]][[conn[["id"]]]], conn)
})

test_that("disconnect_connection() removes from registry + detaches session", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s <- make_server()
  conn <- captor_conn()
  register_connection(s, conn)
  sess <- new_session("x")
  attach_connection(sess, conn[["id"]])
  conn[["session_id"]] <- sess[["id"]]

  disconnect_connection(s, conn)
  expect_false(conn[["id"]] %in% ls(s[["connections"]]))
  expect_false(conn[["id"]] %in% sess[["connections"]])
  expect_null(conn[["session_id"]])
})


test_that("disconnect_connection() closes the socket once", {
  s <- make_server()
  conn <- captor_conn()
  n_closed <- 0L
  conn[["close_socket"]] <- function() n_closed <<- n_closed + 1L
  register_connection(s, conn)

  disconnect_connection(s, conn)
  disconnect_connection(s, conn)
  expect_identical(n_closed, 1L)
  expect_true(conn[["closed"]])
})


# handle_message ----
test_that("handle_message() dispatches a frame and sends the response", {
  s <- make_server()
  conn <- captor_conn()
  register_connection(s, conn)
  expect_true(deliver(
    conn,
    s,
    req("auth", params = list(token = s[["token"]]), id = "r1")
  ))
  expect_length(conn[["sent"]], 1L)

  resp_hdr <- captured_decoded(conn)[[1L]]
  expect_equal(resp_hdr[["id"]], "r1")
  expect_true(resp_hdr[["ok"]])
  expect_true(conn[["authed"]])
})

test_that("handle_message() answers a malformed message and keeps going", {
  s <- make_server()
  conn <- captor_conn()
  conn[["authed"]] <- TRUE
  register_connection(s, conn)
  expect_false(handle_message(conn, as.raw(1:3), s))
  full <- encode_frame(req("ping"))
  expect_false(handle_message(conn, full[1:5], s))
  deliver(conn, s, req("ping"))

  sent <- captured_decoded(conn)
  expect_equal(sent[[1L]][["error"]][["code"]], "malformed_frame")
  expect_equal(sent[[2L]][["error"]][["code"]], "malformed_frame")
  expect_true(sent[[3L]][["ok"]])
})

test_that("handle_message() rejects params that are not an object", {
  s <- make_server()
  conn <- captor_conn()
  register_connection(s, conn)
  deliver(conn, s, req("auth", params = "token"))
  err <- captured_decoded(conn)[[1L]][["error"]]
  expect_equal(err[["code"]], "invalid_params")
})

test_that("the third failed auth attempt closes the connection for good", {
  s <- make_server()
  conn <- captor_conn()
  n_closed <- 0L
  conn[["close_socket"]] <- function() n_closed <<- n_closed + 1L
  register_connection(s, conn)
  for (i in 1:3) {
    expect_identical(n_closed, 0L)
    deliver(conn, s, req("auth", params = list(token = "wrong")))
  }
  expect_identical(n_closed, 1L)
  expect_length(conn[["sent"]], 3L)
  expect_false(conn[["id"]] %in% ls(s[["connections"]]))

  # The correct token on the closed connection is ignored.
  expect_false(deliver(
    conn,
    s,
    req("auth", params = list(token = s[["token"]]))
  ))
  expect_false(conn[["authed"]])
  expect_length(conn[["sent"]], 3L)
  expect_identical(n_closed, 1L)
})

test_that("a response over the frame limits becomes an error response", {
  big <- make_response("r9", list(x = strrep("a", .RTEMISLIVE_HEADER_MAX)))
  header <- decode_frame(encode_response(big))[["header"]]
  expect_equal(header[["id"]], "r9")
  expect_false(header[["ok"]])
  expect_equal(header[["error"]][["code"]], "payload_too_large")
})


test_that("an unauthenticated connection may send `auth` only", {
  s <- make_server()
  long <- strrep("x", 1e5)
  messages <- list(
    encode_frame(req("ping")),
    encode_frame(req("no.such.method")),
    encode_frame(req(long)),
    encode_frame(req("ping", id = long)),
    encode_frame(list(v = 1L, id = "r")),
    as.raw(1:3),
    charToRaw(long)
  )
  for (bytes in messages) {
    conn <- captor_conn()
    register_connection(s, conn)
    handle_message(conn, bytes, s)
    expect_true(conn[["closed"]])
    expect_length(conn[["sent"]], 1L)
    # The reply has a bounded size whatever the request holds.
    expect_lt(length(conn[["sent"]][[1L]]), 1000L)
  }
})

test_that("an `auth` request with a long id gets a bounded reply", {
  s <- make_server()
  conn <- captor_conn()
  register_connection(s, conn)
  deliver(
    conn,
    s,
    req("auth", params = list(token = "wrong"), id = strrep("x", 1e5))
  )
  expect_lt(length(conn[["sent"]][[1L]]), 1000L)
  expect_false(conn[["authed"]])
})


# drop_unauthenticated ----
test_that("drop_unauthenticated() closes connections past the deadline", {
  s <- make_server()
  late <- captor_conn()
  authed <- captor_conn()
  fresh <- captor_conn()
  for (conn in list(late, authed, fresh)) {
    register_connection(s, conn)
  }
  late[["unauthed_seconds"]] <- .RTEMISLIVE_AUTH_TIMEOUT
  authed[["unauthed_seconds"]] <- .RTEMISLIVE_AUTH_TIMEOUT
  authed[["authed"]] <- TRUE

  expect_identical(drop_unauthenticated(s), 1L)
  expect_setequal(
    ls(s[["connections"]]),
    c(authed[["id"]], fresh[["id"]])
  )
})

test_that("drop_unauthenticated() counts a blocked loop as one second", {
  s <- make_server()
  conn <- captor_conn()
  register_connection(s, conn)
  s[["last_auth_check"]] <- Sys.time() - 3600
  expect_identical(drop_unauthenticated(s), 0L)
  expect_identical(conn[["unauthed_seconds"]], 1)
})


# write_frame ----
test_that("write_frame() sends through send_raw", {
  conn <- new_connection()
  captured <- NULL
  conn[["send_raw"]] <- function(b) {
    captured <<- b
  }
  ok <- write_frame(conn, charToRaw("hi"))
  expect_true(ok)
  expect_equal(rawToChar(captured), "hi")
})

test_that("write_frame() returns FALSE without send_raw", {
  conn <- new_connection()
  ok <- write_frame(conn, charToRaw("hi"))
  expect_false(ok)
})

test_that("write_frame() returns FALSE if the send_raw closure errors", {
  conn <- new_connection()
  conn[["send_raw"]] <- function(b) stop("boom")
  expect_false(write_frame(conn, charToRaw("hi")))
})

test_that("write_frame() returns FALSE if send_raw returns an errorValue", {
  # As `ws$send()` does on a closed connection.
  conn <- new_connection()
  conn[["send_raw"]] <- function(b) {
    structure(7L, class = c("errorValue", "try-error"))
  }
  expect_false(write_frame(conn, charToRaw("hi")))
})


# emit_event_to_session: fan-out + buffer ----
test_that("emit_event_to_session() sends to all attached connections", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s <- make_server()
  c1 <- captor_conn()
  c2 <- captor_conn()
  register_connection(s, c1)
  register_connection(s, c2)
  sess <- new_session("x")
  attach_connection(sess, c1[["id"]])
  attach_connection(sess, c2[["id"]])

  ev <- make_event("heartbeat", data = list(ts = "now"))
  sent <- emit_event_to_session(s, sess, ev)
  expect_equal(sent, 2L)
  expect_length(c1[["sent"]], 1L)
  expect_length(c2[["sent"]], 1L)
  expect_equal(captured_decoded(c1)[[1L]][["event"]], "heartbeat")
})

test_that("emit_event_to_session() buffers when no connections attached", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s <- make_server()
  sess <- new_session("x")
  ev <- make_event("heartbeat", data = list(ts = "now"))
  sent <- emit_event_to_session(s, sess, ev)
  expect_equal(sent, 0L)
  expect_length(sess[["event_buffer"]], 1L)
})

test_that("emit_event_to_session() buffers when the last connection fails", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s <- make_server()
  bad <- new_connection()
  bad[["send_raw"]] <- function(b) stop("nope")
  register_connection(s, bad)
  sess <- new_session("x")
  attach_connection(sess, bad[["id"]])
  bad[["session_id"]] <- sess[["id"]]

  ev <- make_event("job.complete", data = list(job_id = "job-1"))
  expect_equal(emit_event_to_session(s, sess, ev), 0L)
  expect_identical(sess[["event_buffer"]], list(ev))
})

test_that("emit_event_to_session() drops the failing connection but keeps others", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s <- make_server()
  bad <- new_connection()
  bad[["send_raw"]] <- function(b) stop("nope")
  good <- captor_conn()
  register_connection(s, bad)
  register_connection(s, good)
  sess <- new_session("x")
  attach_connection(sess, bad[["id"]])
  attach_connection(sess, good[["id"]])

  ev <- make_event("heartbeat", data = list(ts = "now"))
  sent <- emit_event_to_session(s, sess, ev)
  expect_equal(sent, 1L)
  expect_false(bad[["id"]] %in% ls(s[["connections"]]))
  expect_true(good[["id"]] %in% ls(s[["connections"]]))
})


# poll_active_jobs ----
test_that("poll_active_jobs() emits resolution events for newly-resolved jobs", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s <- make_server()
  conn <- captor_conn()
  register_connection(s, conn)
  sess <- new_session("x")
  attach_connection(sess, conn[["id"]])
  conn[["session_id"]] <- sess[["id"]]

  # Synthesise a finalized job directly (no mirai needed).
  job <- new.env(parent = emptyenv())
  job[["id"]] <- "job-test"
  job[["session_id"]] <- sess[["id"]]
  job[["status"]] <- "complete"
  job[["result"]] <- list(value = 42L)
  job[["progress"]] <- list()
  job[["submitted_at"]] <- Sys.time()
  job[["started_at"]] <- Sys.time()
  job[["completed_at"]] <- Sys.time()
  job[["error"]] <- NULL
  # check_job_resolved short-circuits on terminal status without touching mirai
  sess[["jobs"]][["job-test"]] <- job

  emitted <- poll_active_jobs(s)
  expect_equal(emitted, 1L)
  expect_length(conn[["sent"]], 1L)
  ev <- captured_decoded(conn)[[1L]]
  expect_equal(ev[["event"]], "job.complete")
  expect_equal(ev[["data"]][["job_id"]], "job-test")
  expect_true(isTRUE(job[["emitted_resolution"]]))

  # Idempotent - same poll doesn't re-emit
  expect_equal(poll_active_jobs(s), 0L)
})

test_that("poll_active_jobs() emits job.failed with the error envelope", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s <- make_server()
  conn <- captor_conn()
  register_connection(s, conn)
  sess <- new_session("x")
  attach_connection(sess, conn[["id"]])

  job <- new.env(parent = emptyenv())
  job[["id"]] <- "job-bad"
  job[["session_id"]] <- sess[["id"]]
  job[["status"]] <- "failed"
  job[["error"]] <- list(code = "internal_error", message = "boom")
  job[["progress"]] <- list()
  job[["submitted_at"]] <- Sys.time()
  job[["started_at"]] <- Sys.time()
  job[["completed_at"]] <- Sys.time()
  sess[["jobs"]][["job-bad"]] <- job

  poll_active_jobs(s)
  ev <- captured_decoded(conn)[[1L]]
  expect_equal(ev[["event"]], "job.failed")
  expect_equal(ev[["data"]][["error"]][["code"]], "internal_error")
})


# emit_heartbeats / maybe_tick_periodic ----
test_that("emit_heartbeats() sends one event per session", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s <- make_server()
  c1 <- captor_conn()
  c2 <- captor_conn()
  register_connection(s, c1)
  register_connection(s, c2)
  s1 <- new_session("a")
  s2 <- new_session("b")
  attach_connection(s1, c1[["id"]])
  attach_connection(s2, c2[["id"]])

  n <- emit_heartbeats(s)
  expect_equal(n, 2L)
  expect_equal(captured_decoded(c1)[[1L]][["event"]], "heartbeat")
  expect_equal(captured_decoded(c2)[[1L]][["event"]], "heartbeat")
})

test_that("maybe_tick_periodic() respects heartbeat_interval", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s <- make_server(heartbeat_interval = 60, gc_interval = 3600)
  conn <- captor_conn()
  register_connection(s, conn)
  sess <- new_session("x")
  attach_connection(sess, conn[["id"]])

  # Fresh server - interval has not yet elapsed; no heartbeat
  out <- maybe_tick_periodic(s)
  expect_equal(out[["heartbeats_emitted"]], 0L)

  # Force the timer back so heartbeat is due
  s[["last_heartbeat"]] <- Sys.time() - 1000
  out <- maybe_tick_periodic(s)
  expect_equal(out[["heartbeats_emitted"]], 1L)
})

test_that("maybe_tick_periodic() emits no heartbeat when disabled", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s <- make_server(heartbeat_interval = 0, gc_interval = 3600)
  conn <- captor_conn()
  register_connection(s, conn)
  sess <- new_session("x")
  attach_connection(sess, conn[["id"]])

  # Even with the timer well past due, a non-positive interval emits nothing.
  s[["last_heartbeat"]] <- Sys.time() - 1000
  out <- maybe_tick_periodic(s)
  expect_equal(out[["heartbeats_emitted"]], 0L)
  expect_length(conn[["sent"]], 0L)
})

test_that("gc_tick() returns dropped session ids and updates last_gc", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s <- make_server()
  sess <- new_session("old")
  sess[["last_seen"]] <- Sys.time() - 1e6 # very old, no connections
  s[["session_ttl"]] <- 60
  s[["data_ttl"]] <- 60
  before <- s[["last_gc"]]
  out <- gc_tick(s)
  expect_equal(out[["sessions_dropped"]], sess[["id"]])
  expect_gt(as.numeric(s[["last_gc"]]), as.numeric(before))
})


test_that("gc_sessions() keeps an idle session with an unfinished job", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  sess <- new_session("busy")
  sess[["last_seen"]] <- Sys.time() - 1e6
  job <- new.env(parent = emptyenv())
  job[["status"]] <- "running"
  sess[["jobs"]][["job-1"]] <- job
  expect_length(gc_sessions(ttl = 60), 0L)
  job[["status"]] <- "complete"
  expect_equal(gc_sessions(ttl = 60), sess[["id"]])
})


# host_tick ----
test_that("host_tick() reports the counts of each step", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s <- make_server(heartbeat_interval = 3600, gc_interval = 3600)
  res <- host_tick(s)
  expect_equal(res[["unauthenticated_closed"]], 0L)
  expect_equal(res[["daemons_launched"]], 0L)
  expect_equal(res[["progress_routed"]], 0L)
  expect_equal(res[["jobs_resolved"]], 0L)
  expect_equal(res[["heartbeats_emitted"]], 0L)
  expect_false(res[["gc_ran"]])
})
