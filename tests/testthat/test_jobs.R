# test_jobs.R
# ::rtemis::
# 2026- EDG rtemis.org

skip_if_not_installed("mirai")
skip_if_not_installed("rtemis")


# File-level: ensure a small daemon pool is available for these tests.
# Daemons load `library(rtemis)` from the system library on first use of
# `rtemis::...`; we assume the installed rtemis matches the version under
# test (re-`install` after source changes if the daemons need to see
# them).
if (!isTRUE(getOption("rtemislive.test_daemons_started"))) {
  mirai::daemons(2L)
  options(rtemislive.test_daemons_started = TRUE)
}


# Helpers --------------------------------------------------------------------

make_session <- function(name = NULL) {
  clear_sessions()
  new_session(name)
}

# Bounded polling helper — avoids forever-loops on a hung mirai.
wait_for_resolved <- function(job, timeout = 5) {
  start <- Sys.time()
  while (mirai::unresolved(job[["mirai"]])) {
    if (as.numeric(difftime(Sys.time(), start, units = "secs")) > timeout) {
      stop("Timed out waiting for job to resolve")
    }
    Sys.sleep(0.02)
  }
}


# new_job_id ----
test_that("new_job_id() returns job-<hex16>", {
  expect_match(new_job_id(), "^job-[0-9a-f]{16}$")
})


# submit_job: happy path -----------------------------------------------------
test_that("submit_job() registers a running job that resolves to its value", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)

  job <- submit_job(
    session = s,
    type = "test",
    params = list(),
    expr = quote(2L + 2L)
  )

  expect_match(job[["id"]], "^job-")
  expect_equal(job[["status"]], "running")
  expect_s3_class(job[["submitted_at"]], "POSIXct")
  expect_identical(get_job(s, job[["id"]]), job)

  wait_for_resolved(job)
  expect_true(check_job_resolved(job))
  expect_equal(job[["status"]], "complete")
  expect_equal(job[["result"]], 4L)
  expect_s3_class(job[["completed_at"]], "POSIXct")
  expect_null(job[["error"]])
})

test_that("submit_job() injects env values into the daemon task", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)

  job <- submit_job(
    session = s,
    type = "test",
    params = list(),
    expr = quote(a * b),
    env = list(a = 6, b = 7)
  )
  wait_for_resolved(job)
  check_job_resolved(job)
  expect_equal(job[["result"]], 42)
})


# submit_job: job_id propagation to live env --------------------------------
test_that("submit_job() exposes job_id in rtemis::live on the daemon", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)

  job <- submit_job(
    session = s,
    type = "test",
    params = list(),
    expr = quote(asNamespace("rtemis")$live$rtemislive_job_id)
  )
  wait_for_resolved(job)
  check_job_resolved(job)
  expect_equal(job[["result"]], job[["id"]])
})


# submit_job: failure --------------------------------------------------------
test_that("submit_job() task error transitions job to failed with error info", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)

  job <- submit_job(
    session = s,
    type = "test",
    params = list(),
    expr = quote(stop("boom"))
  )
  wait_for_resolved(job)
  check_job_resolved(job)
  expect_equal(job[["status"]], "failed")
  expect_equal(job[["error"]][["code"]], "internal_error")
  expect_match(job[["error"]][["message"]], "boom")
  expect_null(job[["result"]])
})


# submit_job: validation -----------------------------------------------------
test_that("submit_job() rejects non-character type and bad env", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)
  expect_error(submit_job(s, 123, list(), quote(1)))
  expect_error(submit_job(s, "x", list(), quote(1), env = list(1, 2)))
})

test_that("submit_job() respects max_concurrent cap", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)

  # Two slow tasks; we cap at 1 to force rejection.
  job1 <- submit_job(
    s,
    "test",
    list(),
    expr = quote(Sys.sleep(0.5)),
    max_concurrent = 1L
  )
  expect_error(
    submit_job(
      s,
      "test",
      list(),
      expr = quote(Sys.sleep(0.5)),
      max_concurrent = 1L
    ),
    class = "rtemislive_too_many"
  )
  wait_for_resolved(job1)
  check_job_resolved(job1)
})


# check_job_resolved / finalize_job -----------------------------------------
test_that("check_job_resolved() returns FALSE while running, TRUE after", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)

  job <- submit_job(
    s,
    "test",
    list(),
    expr = quote({
      Sys.sleep(0.1)
      1L
    })
  )
  # Immediately after submission, mirai should still be unresolved
  # (within the 0.1s sleep window).
  if (mirai::unresolved(job[["mirai"]])) {
    expect_false(check_job_resolved(job))
  }
  wait_for_resolved(job)
  expect_true(check_job_resolved(job))
  # Idempotent after finalization
  expect_true(check_job_resolved(job))
})


# Cancellation ---------------------------------------------------------------
test_that("cancel_job() on a finished job returns FALSE", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)
  job <- submit_job(s, "test", list(), expr = quote(1L))
  wait_for_resolved(job)
  check_job_resolved(job)
  expect_false(cancel_job(s, job[["id"]]))
})

test_that("cancel_job() on a running job marks cancelling, then cancelled", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)

  job <- submit_job(s, "test", list(), expr = quote(Sys.sleep(2)))
  expect_true(cancel_job(s, job[["id"]]))
  expect_equal(job[["status"]], "cancelling")

  wait_for_resolved(job)
  check_job_resolved(job)
  expect_equal(job[["status"]], "cancelled")
})

test_that("cancel_job() on unknown job_id throws rtemislive_not_found", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)
  expect_error(cancel_job(s, "job-bogus"), class = "rtemislive_not_found")
})


# get_job / list_jobs / job_summary / delete_job -----------------------------
test_that("list_jobs() returns wire summaries for all jobs in a session", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)

  j1 <- submit_job(s, "test", list(), expr = quote(1L))
  j2 <- submit_job(s, "test", list(), expr = quote(stop("e")))
  wait_for_resolved(j1)
  wait_for_resolved(j2)
  check_job_resolved(j1)
  check_job_resolved(j2)

  out <- list_jobs(s)
  expect_length(out, 2L)
  statuses <- vapply(out, `[[`, character(1L), "status")
  expect_setequal(statuses, c("complete", "failed"))
  # Each summary has the keys we promise
  for (entry in out) {
    expect_true(all(
      c("job_id", "type", "status", "submitted_at") %in%
        names(entry)
    ))
  }
})

test_that("delete_job() removes a finished job and refuses to delete running", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)

  j_done <- submit_job(s, "test", list(), expr = quote(1L))
  wait_for_resolved(j_done)
  check_job_resolved(j_done)
  expect_true(delete_job(s, j_done[["id"]]))
  expect_null(get_job(s, j_done[["id"]]))
  expect_false(delete_job(s, j_done[["id"]]))

  j_run <- submit_job(s, "test", list(), expr = quote(Sys.sleep(1)))
  expect_error(
    delete_job(s, j_run[["id"]]),
    class = "rtemislive_invalid_params"
  )
  wait_for_resolved(j_run)
  check_job_resolved(j_run)
})


# Progress integration -------------------------------------------------------
test_that("record_job_progress() merges into the job's progress slot", {
  s <- make_session()
  on.exit(clear_sessions(), add = TRUE)
  job <- submit_job(s, "test", list(), expr = quote(1L))
  wait_for_resolved(job)
  check_job_resolved(job)

  record_job_progress(job, list(stage = "training", fraction = 0.5))
  expect_equal(job[["progress"]][["stage"]], "training")
  expect_equal(job[["progress"]][["fraction"]], 0.5)

  record_job_progress(job, list(fraction = 0.9, message = "Almost"))
  expect_equal(job[["progress"]][["fraction"]], 0.9)
  expect_equal(job[["progress"]][["stage"]], "training") # preserved
  expect_equal(job[["progress"]][["message"]], "Almost")
})


# count_active_jobs ----------------------------------------------------------
test_that("count_active_jobs() counts running and cancelling across sessions", {
  clear_sessions()
  on.exit(clear_sessions(), add = TRUE)
  s1 <- new_session("a")
  s2 <- new_session("b")

  j1 <- submit_job(s1, "test", list(), expr = quote(Sys.sleep(0.5)))
  j2 <- submit_job(s2, "test", list(), expr = quote(Sys.sleep(0.5)))
  expect_equal(count_active_jobs(), 2L)

  wait_for_resolved(j1)
  wait_for_resolved(j2)
  check_job_resolved(j1)
  check_job_resolved(j2)
  expect_equal(count_active_jobs(), 0L)
})
