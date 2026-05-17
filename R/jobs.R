# rtemislive_jobs.R
# ::rtemis::
# 2026- EDG rtemis.org

# Job store and mirai integration for rtemislive. See specs/rtemislive.md
# paragraph 6.4 (train), paragraph 6.6 (cancel), paragraph 10 (state machine), paragraph 9 (progress).
#
# A job lives inside a session's `jobs` sub-env, keyed by job_id. Each job
# is itself an env with:
#
#   id            character - `job-<hex16>`
#   session_id    character - owning session
#   type          character - "train" (later: "cluster", "decomp", ...)
#   params        list - original wire params (for inspection / job.list)
#   status        character - "queued" | "running" | "cancelling" |
#                              "complete" | "failed" | "cancelled"
#   submitted_at  POSIXct
#   started_at    POSIXct or NULL
#   completed_at  POSIXct or NULL
#   mirai         mirai object - the async task handle
#   result        any or NULL - set on success
#   error         list or NULL - `{code, message}` on failure
#   progress      list - last known progress state (set by progress channel)
#
# Job state transitions and the host event loop
# ----------------------------------------------
# The host loop calls `check_job_resolved(job)` each tick. When mirai
# reports the task is no longer `unresolved`, `finalize_job()` is called
# to read the mirai's value, set timing fields, and transition status to
# `complete` / `failed` / `cancelled`. The dispatcher then emits a
# `job.complete` / `job.failed` / `job.cancelled` event to attached
# connections (or buffers it on the session).

# %% Identifiers -------------------------------------------------------------

#' Generate a job id
#'
#' Returns `job-<hex16>`.
#'
#' @return Character scalar.
#'
#' @author EDG
#' @keywords internal
#' @noRd
new_job_id <- function() {
  rtemis.core::check_dependencies("uuid")
  hex <- gsub("-", "", uuid::UUIDgenerate(use.time = TRUE), fixed = TRUE)
  paste0("job-", substr(hex, 1L, 16L))
} # /rtemis::new_job_id


# %% Submission --------------------------------------------------------------

#' Submit a new job
#'
#' Wraps `expr` in a mirai task that:
#'
#' 1. Tags the daemon's `rtemis::live` with the new `job_id` (so the
#'    daemon-side `msg()` sink can attach it to forwarded messages).
#' 2. Restores the previous tag on exit.
#' 3. Evaluates `expr`.
#'
#' Variables referenced from inside `expr` must be supplied via `env` - a
#' named list - so mirai captures and serialises them to the daemon. The
#' job_id is injected automatically.
#'
#' Caller is responsible for ensuring at least one mirai daemon is
#' running (`mirai::daemons(n)`); we don't manage daemons here.
#'
#' @param session Session env.
#' @param type Character. Job type - `"train"`, etc.
#' @param params List. Original wire params, retained for inspection.
#' @param expr Quoted expression. Body of the mirai task.
#' @param env Named list. Variables to inject into the mirai task.
#' @param max_concurrent Integer. Server-wide cap on concurrent jobs
#'   (default 8, spec paragraph 11.4). Counted across all sessions.
#'
#' @return Job env.
#'
#' @author EDG
#' @keywords internal
#' @noRd
submit_job <- function(
  session,
  type,
  params,
  expr,
  env = list(),
  max_concurrent = 8L
) {
  rtemis.core::check_dependencies("mirai")
  if (!is.character(type) || length(type) != 1L || is.na(type)) {
    cli::cli_abort("`type` must be a single character string.")
  }
  if (!is.list(env) || (length(env) > 0L && is.null(names(env)))) {
    cli::cli_abort("`env` must be a (possibly empty) named list.")
  }

  if (count_active_jobs() >= max_concurrent) {
    cli::cli_abort(
      "Maximum number of concurrent jobs ({max_concurrent}) reached.",
      class = "rtemislive_too_many"
    )
  }

  job_id <- new_job_id()

  # Wrap the user expression so the daemon stamps `rtemislive_job_id` into
  # rtemis's internal `live` env for the duration of the task. The
  # daemon-side `msg()` sink reads from there to tag forwarded messages.
  #
  # Two subtleties:
  #
  # 1. `live` is not an exported name, so `rtemis::live` fails. We reach
  #    it via `asNamespace("rtemis")$live`.
  # 2. We bind the env to a local variable (`live_env`) before mutating
  #    it. Writing `ns$live$x <- v` is parsed as a rebinding of `live`
  #    in `ns`, which fails because installed-package namespaces are
  #    locked. Mutating the env's *contents* via a local reference
  #    sidesteps the lock.
  wrapped <- bquote({
    live_env <- asNamespace("rtemis")$live
    prev <- live_env$rtemislive_job_id
    live_env$rtemislive_job_id <- .(job_id)
    on.exit(
      {
        live_env$rtemislive_job_id <- prev
      },
      add = TRUE
    )
    .(expr)
  })

  m <- do.call(
    mirai::mirai,
    c(list(.expr = wrapped), env)
  )

  now <- Sys.time()
  job <- new.env(parent = emptyenv())
  job[["id"]] <- job_id
  job[["session_id"]] <- session[["id"]]
  job[["type"]] <- type
  job[["params"]] <- params
  job[["status"]] <- "running" # mirai accepts immediately; v1 doesn't
  # distinguish "queued" from "running"
  job[["submitted_at"]] <- now
  job[["started_at"]] <- now
  job[["completed_at"]] <- NULL
  job[["mirai"]] <- m
  job[["result"]] <- NULL
  job[["error"]] <- NULL
  job[["progress"]] <- list()

  session[["jobs"]][[job_id]] <- job
  touch_session(session)
  job
} # /rtemis::submit_job


#' Count active (running or cancelling) jobs across all sessions
#'
#' @return Integer.
#'
#' @author EDG
#' @keywords internal
#' @noRd
count_active_jobs <- function() {
  reg <- session_registry()
  total <- 0L
  for (sid in ls(reg)) {
    jobs <- reg[[sid]][["jobs"]]
    for (jid in ls(jobs)) {
      st <- jobs[[jid]][["status"]]
      if (identical(st, "running") || identical(st, "cancelling")) {
        total <- total + 1L
      }
    }
  }
  total
} # /rtemis::count_active_jobs


# %% Resolution and finalization --------------------------------------------

#' Non-blocking check for job resolution
#'
#' Returns `TRUE` if the mirai has resolved and the job was finalized
#' (status moved to `complete` / `failed` / `cancelled`). Returns
#' `FALSE` if the mirai is still running.
#'
#' This is the polling primitive the host event loop calls each tick on
#' every active job.
#'
#' @param job Job env.
#'
#' @return Logical scalar.
#'
#' @author EDG
#' @keywords internal
#' @noRd
check_job_resolved <- function(job) {
  if (job[["status"]] %in% c("complete", "failed", "cancelled")) {
    return(TRUE) # already finalized
  }
  if (mirai::unresolved(job[["mirai"]])) {
    return(FALSE)
  }
  finalize_job(job)
  TRUE
} # /rtemis::check_job_resolved


#' Finalize a resolved job
#'
#' Reads the mirai's value (non-blocking - only call after `unresolved()`
#' returns `FALSE`), sets timing, and transitions status.
#'
#' Status transitions:
#'
#' - mirai produced a value, status was `cancelling` -> `cancelled`
#'   (the cancel raced with normal completion; client asked, treat as
#'   cancelled).
#' - mirai produced a value, status was `running` -> `complete`.
#' - mirai produced an `errorValue`, status was `cancelling` ->
#'   `cancelled` (stop_mirai signalled an error).
#' - mirai produced an `errorValue`, status was `running` -> `failed`.
#'
#' @param job Job env.
#'
#' @return The job, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
finalize_job <- function(job) {
  m <- job[["mirai"]]
  value <- m$data
  is_err <- inherits(value, "errorValue") || inherits(value, "miraiError")
  was_cancelling <- identical(job[["status"]], "cancelling")

  job[["completed_at"]] <- Sys.time()

  if (is_err) {
    if (was_cancelling) {
      job[["status"]] <- "cancelled"
      job[["error"]] <- list(
        code = "cancelled",
        message = "Job cancelled."
      )
    } else {
      job[["status"]] <- "failed"
      job[["error"]] <- list(
        code = "internal_error",
        message = format_mirai_error(value)
      )
    }
  } else {
    if (was_cancelling) {
      # Daemon completed before the cancel landed - honour the client's
      # request by reporting cancellation, but keep the result available
      # so callers can fetch it if they choose.
      job[["status"]] <- "cancelled"
      job[["result"]] <- value
    } else {
      job[["status"]] <- "complete"
      job[["result"]] <- value
    }
  }

  invisible(job)
} # /rtemis::finalize_job


#' Format a mirai error value into a short string
#'
#' @author EDG
#' @keywords internal
#' @noRd
format_mirai_error <- function(value) {
  msg <- attr(value, "message", exact = TRUE)
  if (is.null(msg)) {
    msg <- tryCatch(as.character(value), error = function(e) "Unknown error.")
  }
  if (length(msg) > 1L) {
    msg <- paste(msg, collapse = " ")
  }
  msg
} # /rtemis::format_mirai_error


# %% Cancellation ------------------------------------------------------------

#' Cancel a job
#'
#' Best-effort. If the job is already done, returns `FALSE`. Otherwise
#' marks `cancelling` and asks mirai to stop the task. The actual status
#' transition to `cancelled` happens in `finalize_job()` once the daemon
#' returns. See spec paragraph 6.6 for the honest caveat about compiled code not
#' honouring R interrupts.
#'
#' @param session Session env.
#' @param job_id Character scalar.
#'
#' @return Logical scalar - `TRUE` if cancellation was requested,
#'   `FALSE` if the job was already done.
#'
#' @author EDG
#' @keywords internal
#' @noRd
cancel_job <- function(session, job_id) {
  job <- session[["jobs"]][[job_id]]
  if (is.null(job)) {
    cli::cli_abort(
      "Unknown job_id {.val {job_id}}.",
      class = "rtemislive_not_found"
    )
  }
  if (job[["status"]] %in% c("complete", "failed", "cancelled")) {
    return(FALSE)
  }
  job[["status"]] <- "cancelling"
  tryCatch(
    mirai::stop_mirai(job[["mirai"]]),
    error = function(e) {
      # stop_mirai may error if the task has already resolved between our
      # check and the call. That's fine - the next loop tick will pick up
      # the resolution.
      NULL
    }
  )
  touch_session(session)
  TRUE
} # /rtemis::cancel_job


# %% Lookup, list, delete ----------------------------------------------------

#' Get a job by id
#'
#' @param session Session env.
#' @param job_id Character scalar.
#'
#' @return Job env, or `NULL`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
get_job <- function(session, job_id) {
  jobs <- session[["jobs"]]
  if (
    !is.character(job_id) ||
      length(job_id) != 1L ||
      !exists(job_id, envir = jobs, inherits = FALSE)
  ) {
    return(NULL)
  }
  jobs[[job_id]]
} # /rtemis::get_job


#' Summarize a single job (wire-shaped)
#'
#' @param job Job env.
#'
#' @return Named list.
#'
#' @author EDG
#' @keywords internal
#' @noRd
job_summary <- function(job) {
  prog <- job[["progress"]]
  list(
    job_id = job[["id"]],
    type = job[["type"]],
    status = job[["status"]],
    submitted_at = iso8601(job[["submitted_at"]]),
    started_at = if (is.null(job[["started_at"]])) {
      NULL
    } else {
      iso8601(job[["started_at"]])
    },
    completed_at = if (is.null(job[["completed_at"]])) {
      NULL
    } else {
      iso8601(job[["completed_at"]])
    },
    stage = prog[["stage"]],
    fraction = prog[["fraction"]],
    last_message = prog[["message"]],
    error = job[["error"]]
  )
} # /rtemis::job_summary


#' List jobs in a session (wire-shaped)
#'
#' @param session Session env.
#'
#' @return List of named lists.
#'
#' @author EDG
#' @keywords internal
#' @noRd
list_jobs <- function(session) {
  jobs <- session[["jobs"]]
  lapply(ls(jobs), function(jid) job_summary(jobs[[jid]]))
} # /rtemis::list_jobs


#' Delete a finished job
#'
#' Refuses to delete running/cancelling jobs - clients should `cancel`
#' them first and wait for resolution.
#'
#' @param session Session env.
#' @param job_id Character scalar.
#'
#' @return Logical scalar.
#'
#' @author EDG
#' @keywords internal
#' @noRd
delete_job <- function(session, job_id) {
  jobs <- session[["jobs"]]
  if (
    !is.character(job_id) ||
      length(job_id) != 1L ||
      !exists(job_id, envir = jobs, inherits = FALSE)
  ) {
    return(FALSE)
  }
  job <- jobs[[job_id]]
  if (job[["status"]] %in% c("running", "cancelling")) {
    cli::cli_abort(
      "Cannot delete a {job[['status']]} job - cancel and wait first.",
      class = "rtemislive_invalid_params"
    )
  }
  rm(list = job_id, envir = jobs)
  touch_session(session)
  TRUE
} # /rtemis::delete_job


# %% Progress integration ----------------------------------------------------

#' Update a job's progress snapshot
#'
#' Called by the host event loop when it routes a `job.progress` event
#' arriving on the progress pull socket. Stores the latest stage /
#' fraction / message on the job so `job.list` / `job.status` can return
#' them.
#'
#' @param job Job env.
#' @param progress Named list with optional `stage`, `fraction`,
#'   `message`, `metrics`.
#'
#' @return The job, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
record_job_progress <- function(job, progress) {
  if (!is.list(progress)) {
    cli::cli_abort("`progress` must be a list.")
  }
  cur <- job[["progress"]]
  for (k in names(progress)) {
    cur[[k]] <- progress[[k]]
  }
  job[["progress"]] <- cur
  invisible(job)
} # /rtemis::record_job_progress
