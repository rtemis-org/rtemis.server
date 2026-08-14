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

test_that("route_progress() forwards every envelope field it does not rename", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s <- new_session("a")
  j <- make_fake_job(s)
  captured <- list()

  # A nested-progress envelope as rtemis.core emits it. `made_up` stands in for
  # whatever rtemis reports next: the wire is producer-defined, so an unknown
  # field must ride along rather than be dropped by a whitelist here.
  routed <- route_progress(
    list(list(
      job_id = j[["id"]],
      caller = NA,
      message = "\033[1mTuning\033[0m 4/12",
      ts = "ts1",
      level = "progress",
      session_id = "s-abc",
      node_id = "pb2",
      parent_id = "pb1",
      kind = "tune",
      label = "Tuning",
      status = "update",
      current = 4L,
      total = 12L,
      made_up = "future field"
    )),
    send_event = function(session, event) {
      captured[[length(captured) + 1L]] <<- event
    }
  )

  expect_equal(routed, 1L)
  data <- captured[[1L]][["data"]]
  expect_equal(data[["node_id"]], "pb2")
  expect_equal(data[["parent_id"]], "pb1")
  expect_equal(data[["kind"]], "tune")
  expect_equal(data[["label"]], "Tuning")
  expect_equal(data[["session_id"]], "s-abc")
  expect_equal(data[["status"]], "update")
  expect_equal(data[["current"]], 4L)
  expect_equal(data[["total"]], 12L)
  expect_equal(data[["made_up"]], "future field")
  # Renamed fields: `caller` becomes `stage`, and the text is ANSI-stripped so no
  # escape sequence reaches the browser.
  expect_equal(data[["message"]], "Tuning 4/12")
  expect_true(is.na(data[["stage"]]))
  expect_false("caller" %in% names(data))
  # The recorded snapshot keeps only what `job_summary()` reports: per-node
  # fields would go stale the moment an event without them arrived.
  expect_equal(j[["progress"]][["message"]], "Tuning 4/12")
  expect_equal(j[["progress"]][["level"]], "progress")
  expect_false(any(
    c("node_id", "current", "total", "label", "made_up") %in%
      names(j[["progress"]])
  ))
})

test_that("route_progress() tracks the outermost loop's fraction", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s <- new_session("a")
  j <- make_fake_job(s)
  tick <- function(node_id, parent_id, current, total) {
    route_progress(list(list(
      job_id = j[["id"]],
      caller = NA,
      message = "m",
      level = "progress",
      node_id = node_id,
      parent_id = parent_id,
      status = "update",
      current = current,
      total = total
    )))
    j[["progress"]][["fraction"]]
  }

  # The first progress node a job opens is its outermost loop.
  expect_equal(tick("pb1", "n1", 1L, 4L), 0.25)
  # An inner loop reports its own counters but must not move the job's figure.
  expect_equal(tick("pb2", "pb1", 3L, 6L), 0.25)
  expect_equal(tick("pb1", "n1", 2L, 4L), 0.5)
  # Plain msg() envelopes carry no counters and leave it alone.
  route_progress(list(list(
    job_id = j[["id"]],
    caller = "train",
    message = "Training...",
    level = "info"
  )))
  expect_equal(j[["progress"]][["fraction"]], 0.5)
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
      rtemis.core::set_msg_sink(NULL)
      close_progress_socket(pull)
      close_progress_socket(push)
    },
    add = TRUE
  )

  rtemis.core::set_msg_sink(function(m) {
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
