# test_loop.R
# ::rtemis::
# 2026- EDG rtemis.org


# Helpers --------------------------------------------------------------------

make_server <- function(token = "test-toke-nnnn-9999",
                        heartbeat_interval = 5,
                        gc_interval = 60) {
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

# Stuff a request frame's raw bytes into a connection's read buffer.
push_bytes <- function(conn, frame_list, payload = NULL) {
  bytes <- encode_frame(frame_list, payload = payload)
  conn[["buffer"]] <- c(conn[["buffer"]], bytes)
  invisible(NULL)
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
  expect_false(s[["stop_requested"]])
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


# drain_buffer: dispatches frames and writes responses --------------------
test_that("drain_buffer() dispatches one frame, captor receives response", {
  s <- make_server()
  conn <- captor_conn()
  register_connection(s, conn)
  push_bytes(conn, req("auth", params = list(token = s[["token"]]), id = "r1"))

  dispatched <- drain_buffer(conn, s)
  expect_equal(dispatched, 1L)
  expect_length(conn[["sent"]], 1L)

  resp_hdr <- captured_decoded(conn)[[1L]]
  expect_equal(resp_hdr[["id"]], "r1")
  expect_true(resp_hdr[["ok"]])
  expect_true(conn[["authed"]])
})

test_that("drain_buffer() handles multiple frames in one read", {
  s <- make_server()
  conn <- captor_conn()
  register_connection(s, conn)
  push_bytes(conn, req("auth", params = list(token = s[["token"]])))
  push_bytes(conn, req("ping", id = "p1"))
  push_bytes(conn, req("ping", id = "p2"))

  dispatched <- drain_buffer(conn, s)
  expect_equal(dispatched, 3L)
  expect_length(conn[["sent"]], 3L)
  ids <- vapply(captured_decoded(conn), `[[`, character(1L), "id")
  expect_true("p1" %in% ids)
  expect_true("p2" %in% ids)
})

test_that("drain_buffer() leaves an incomplete trailing frame in the buffer", {
  s <- make_server()
  conn <- captor_conn()
  register_connection(s, conn)
  push_bytes(conn, req("auth", params = list(token = s[["token"]])))
  full <- encode_frame(req("ping", id = "p1"))
  conn[["buffer"]] <- c(conn[["buffer"]], full[1:5])  # truncated

  before <- length(conn[["buffer"]])
  dispatched <- drain_buffer(conn, s)
  expect_equal(dispatched, 1L)
  # The incomplete bytes remain
  expect_equal(length(conn[["buffer"]]), 5L)
})

test_that("drain_buffer() with close_after_response triggers disconnect", {
  s <- make_server()
  conn <- captor_conn()
  register_connection(s, conn)
  # 3 failed auth attempts => sets close_after_response
  for (i in 1:3) {
    push_bytes(conn, req("auth", params = list(token = "wrong")))
  }
  drain_buffer(conn, s)
  expect_false(conn[["id"]] %in% ls(s[["connections"]]))
})


# write_frame: send_raw or socket ----
test_that("write_frame() prefers send_raw closure", {
  conn <- new_connection()
  captured <- NULL
  conn[["send_raw"]] <- function(b) {
    captured <<- b
  }
  ok <- write_frame(conn, charToRaw("hi"))
  expect_true(ok)
  expect_equal(rawToChar(captured), "hi")
})

test_that("write_frame() returns FALSE when neither send_raw nor socket", {
  conn <- new_connection()
  ok <- write_frame(conn, charToRaw("hi"))
  expect_false(ok)
})

test_that("write_frame() returns FALSE if the send_raw closure errors", {
  conn <- new_connection()
  conn[["send_raw"]] <- function(b) stop("boom")
  expect_false(write_frame(conn, charToRaw("hi")))
})


# emit_event_to_session: fan-out + buffer ----
test_that("emit_event_to_session() sends to all attached connections", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s <- make_server()
  c1 <- captor_conn(); c2 <- captor_conn()
  register_connection(s, c1); register_connection(s, c2)
  sess <- new_session("x")
  attach_connection(sess, c1[["id"]]); attach_connection(sess, c2[["id"]])

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

test_that("emit_event_to_session() drops the failing connection but keeps others", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s <- make_server()
  bad <- new_connection()
  bad[["send_raw"]] <- function(b) stop("nope")
  good <- captor_conn()
  register_connection(s, bad); register_connection(s, good)
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

  # Idempotent — same poll doesn't re-emit
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
  job[["id"]] <- "job-bad"; job[["session_id"]] <- sess[["id"]]
  job[["status"]] <- "failed"
  job[["error"]] <- list(code = "internal_error", message = "boom")
  job[["progress"]] <- list()
  job[["submitted_at"]] <- Sys.time(); job[["started_at"]] <- Sys.time()
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
  c1 <- captor_conn(); c2 <- captor_conn()
  register_connection(s, c1); register_connection(s, c2)
  s1 <- new_session("a"); s2 <- new_session("b")
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

  # Fresh server — interval has not yet elapsed; no heartbeat
  out <- maybe_tick_periodic(s)
  expect_equal(out[["heartbeats_emitted"]], 0L)

  # Force the timer back so heartbeat is due
  s[["last_heartbeat"]] <- Sys.time() - 1000
  out <- maybe_tick_periodic(s)
  expect_equal(out[["heartbeats_emitted"]], 1L)
})

test_that("gc_tick() returns dropped session ids and updates last_gc", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s <- make_server()
  sess <- new_session("old")
  sess[["last_seen"]] <- Sys.time() - 1e6   # very old, no connections
  s[["session_ttl"]] <- 60
  s[["data_ttl"]] <- 60
  before <- s[["last_gc"]]
  out <- gc_tick(s)
  expect_equal(out[["sessions_dropped"]], sess[["id"]])
  expect_gt(as.numeric(s[["last_gc"]]), as.numeric(before))
})


# loop_tick: combines steps ----
test_that("loop_tick() dispatches connection frames and reports counts", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s <- make_server(heartbeat_interval = 3600, gc_interval = 3600)
  conn <- captor_conn()
  register_connection(s, conn)
  push_bytes(conn, req("auth", params = list(token = s[["token"]])))
  push_bytes(conn, req("ping"))

  res <- loop_tick(s)
  expect_equal(res[["frames_dispatched"]], 2L)
  expect_equal(res[["progress_routed"]], 0L)
  expect_equal(res[["jobs_resolved"]], 0L)
  expect_equal(res[["heartbeats_emitted"]], 0L)
  expect_false(res[["gc_ran"]])
  expect_length(conn[["sent"]], 2L)
})

test_that("run_loop() exits when stop_requested is set", {
  s <- make_server(heartbeat_interval = 3600, gc_interval = 3600)
  s[["stop_requested"]] <- TRUE
  expect_silent(run_loop(s, tick_ms = 1))
})
