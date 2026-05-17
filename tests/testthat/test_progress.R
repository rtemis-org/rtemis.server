# test_progress.R
# ::rtemis::
# 2026- EDG rtemis.org

skip_if_not_installed("nanonext")
skip_if_not_installed("jsonlite")


# default_progress_url ----
test_that("default_progress_url() returns ipc:// path", {
  url <- default_progress_url()
  expect_match(url, "^ipc://")
  expect_false(url == default_progress_url()) # tempfile is unique
})


# bind / drain (in-process round-trip) ---------------------------------------

#' Helper: build a push socket dialing `url`.
push_socket <- function(url) {
  nanonext::socket("push", dial = url)
}

#' Helper: encode a list as JSON raw bytes (mirroring the daemon side).
to_json_raw <- function(x) {
  charToRaw(as.character(jsonlite::toJSON(
    x,
    auto_unbox = TRUE,
    na = "null",
    null = "null"
  )))
}

test_that("bind_progress_socket() opens a pull socket", {
  url <- paste0("inproc://test-bind-", basename(tempfile()))
  sock <- bind_progress_socket(url)
  on.exit(close_progress_socket(sock), add = TRUE)
  expect_true(inherits(sock, "nanoSocket") || inherits(sock, "nano"))
})

test_that("bind_progress_socket() rejects bad URL", {
  expect_error(bind_progress_socket(""))
  expect_error(bind_progress_socket(NULL))
  expect_error(bind_progress_socket(c("a", "b")))
})

test_that("drain_progress_socket() returns empty list when nothing sent", {
  url <- paste0("inproc://test-empty-", basename(tempfile()))
  sock <- bind_progress_socket(url)
  on.exit(close_progress_socket(sock), add = TRUE)
  expect_equal(drain_progress_socket(sock), list())
})

test_that("drain_progress_socket() returns decoded envelopes", {
  url <- paste0("inproc://test-roundtrip-", basename(tempfile()))
  pull <- bind_progress_socket(url)
  push <- push_socket(url)
  on.exit(
    {
      close_progress_socket(pull)
      close_progress_socket(push)
    },
    add = TRUE
  )

  payload <- list(
    job_id = "job-abc",
    caller = "train",
    message = "hi",
    ts = "2026-05-11 12:00:00",
    level = "info"
  )
  nanonext::send(push, to_json_raw(payload), mode = "raw", block = FALSE)

  # NNG may need a short tick to deliver in-process
  Sys.sleep(0.05)
  out <- drain_progress_socket(pull)
  expect_length(out, 1L)
  expect_equal(out[[1L]][["job_id"]], "job-abc")
  expect_equal(out[[1L]][["caller"]], "train")
  expect_equal(out[[1L]][["message"]], "hi")
  expect_equal(out[[1L]][["level"]], "info")
})

test_that("drain_progress_socket() silently drops malformed bytes", {
  url <- paste0("inproc://test-malformed-", basename(tempfile()))
  pull <- bind_progress_socket(url)
  push <- push_socket(url)
  on.exit(
    {
      close_progress_socket(pull)
      close_progress_socket(push)
    },
    add = TRUE
  )

  nanonext::send(push, as.raw(c(0x00, 0x01, 0x02)), mode = "raw", block = FALSE)
  Sys.sleep(0.05)
  out <- drain_progress_socket(pull)
  expect_equal(out, list())
})


# find_session_for_job -------------------------------------------------------
test_that("find_session_for_job() returns the owning session", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s1 <- new_session("a")
  s2 <- new_session("b")
  fake_job <- new.env(parent = emptyenv())
  fake_job[["id"]] <- "job-x"
  s1[["jobs"]][["job-x"]] <- fake_job

  expect_identical(find_session_for_job("job-x"), s1)
  expect_null(find_session_for_job("job-y"))
  expect_null(find_session_for_job(NULL))
  expect_null(find_session_for_job(NA_character_))
})


# route_progress -------------------------------------------------------------

# Synthesize a minimal job env so we can route to it without mirai.
make_fake_job <- function(session, id = "job-test") {
  job <- new.env(parent = emptyenv())
  job[["id"]] <- id
  job[["session_id"]] <- session[["id"]]
  job[["status"]] <- "running"
  job[["progress"]] <- list()
  session[["jobs"]][[id]] <- job
  job
}

test_that("route_progress() updates job progress for known job_ids", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s <- new_session("a")
  j <- make_fake_job(s)

  routed <- route_progress(list(list(
    job_id = j[["id"]],
    caller = "train",
    message = "Fold 1/3",
    ts = "ts1",
    level = "info"
  )))

  expect_equal(routed, 1L)
  expect_equal(j[["progress"]][["stage"]], "train")
  expect_equal(j[["progress"]][["message"]], "Fold 1/3")
  expect_equal(j[["progress"]][["ts"]], "ts1")
})

test_that("route_progress() skips envelopes for unknown job_ids", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  routed <- route_progress(list(list(
    job_id = "job-bogus",
    caller = "x",
    message = "y"
  )))
  expect_equal(routed, 0L)
})

test_that("route_progress() buffers events when no connections, calls send_event when given", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s <- new_session("a")
  j <- make_fake_job(s)

  # No connections, no callback -> buffered on session
  route_progress(list(list(job_id = j[["id"]], caller = "x", message = "m1")))
  expect_length(s[["event_buffer"]], 1L)
  buffered <- s[["event_buffer"]][[1L]]
  expect_equal(buffered[["event"]], "job.progress")
  expect_equal(buffered[["data"]][["job_id"]], j[["id"]])

  # With send_event callback -> not buffered
  captured <- list()
  cb <- function(session, event) {
    captured[[length(captured) + 1L]] <<- list(s = session, e = event)
  }
  attach_connection(s, "c-1")
  route_progress(
    list(list(job_id = j[["id"]], caller = "y", message = "m2")),
    send_event = cb
  )
  expect_length(captured, 1L)
  expect_equal(captured[[1L]][["e"]][["event"]], "job.progress")
  expect_equal(captured[[1L]][["e"]][["data"]][["message"]], "m2")
})


# End-to-end with msg() sink + in-process NNG -------------------------------
test_that("msg() routed through sink + push/pull pipeline reaches the host", {
  url <- paste0("inproc://test-msgsink-", basename(tempfile()))
  pull <- bind_progress_socket(url)
  push <- push_socket(url)

  # Stand up a sink mimicking what init_daemon_progress installs.
  on.exit(
    {
      rtemis::set_msg_sink(NULL)
      close_progress_socket(pull)
      close_progress_socket(push)
    },
    add = TRUE
  )

  rtemis::set_msg_sink(function(m) {
    payload <- list(
      job_id = "job-host-test",
      caller = m$caller,
      message = m$text,
      ts = m$ts,
      level = m$level
    )
    txt <- jsonlite::toJSON(
      payload,
      auto_unbox = TRUE,
      na = "null",
      null = "null"
    )
    nanonext::send(
      push,
      charToRaw(as.character(txt)),
      mode = "raw",
      block = FALSE
    )
  })

  # Sink machinery still lives in rtemis (will move to rtemis.core when
  # rtemis sheds `msg`); call the sink-aware version explicitly so the
  # test continues to exercise the daemon -> host route.
  msg("Hello from sink")
  Sys.sleep(0.05)
  out <- drain_progress_socket(pull)
  expect_length(out, 1L)
  expect_equal(out[[1L]][["job_id"]], "job-host-test")
  expect_equal(out[[1L]][["message"]], "Hello from sink")
  expect_equal(out[[1L]][["level"]], "info")
})
