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
}


# %% Submission --------------------------------------------------------------

#' Submit a new job
#'
#' Wraps `expr` in a task that:
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
#' If `count_active_jobs() < max_concurrent`, the job is launched
#' immediately and registered with `status = "running"`. Otherwise the
#' job is queued (`status = "queued"`) with its wrapped expression and
#' env retained on the job env, to be launched later by
#' `promote_queued_jobs()` when slots free up.
#'
#' Caller is responsible for ensuring at least one mirai daemon is
#' running (`mirai::daemons(n)`); we don't manage daemons here.
#'
#' @param session Session env.
#' @param type Character. Job type - `"train"`, etc.
#' @param params List. Original wire params, retained for inspection.
#' @param expr Quoted expression. Body of the mirai task.
#' @param env Named list. Variables to inject into the mirai task.
#' @param max_concurrent Integer. Server-wide cap on concurrent jobs.
#'   Counted across all sessions. Passed down from `server$max_concurrent`.
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
    rtemis.core::abort("`type` must be a single character string.")
  }
  if (!is.list(env) || (length(env) > 0L && is.null(names(env)))) {
    rtemis.core::abort("`env` must be a (possibly empty) named list.")
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
    # Lazily install the msg sink + push socket on this daemon if not
    # already done. Required because `mirai::everywhere` (used at
    # serve() startup) cannot persist writes into rtemis's namespace
    # env - see init_daemon_progress for the full rationale.
    rtemis.server::ensure_daemon_sink()
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

  now <- Sys.time()
  job <- new.env(parent = emptyenv())
  job[["id"]] <- job_id
  job[["session_id"]] <- session[["id"]]
  job[["type"]] <- type
  job[["params"]] <- params
  job[["submitted_at"]] <- now
  job[["completed_at"]] <- NULL
  job[["mirai"]] <- NULL
  job[["result"]] <- NULL
  job[["error"]] <- NULL
  job[["progress"]] <- list()
  job[["pending_expr"]] <- NULL
  job[["pending_env"]] <- NULL

  if (count_active_jobs() < max_concurrent) {
    job[["mirai"]] <- do.call(mirai::mirai, c(list(.expr = wrapped), env))
    job[["status"]] <- "running"
    job[["started_at"]] <- now
  } else {
    job[["status"]] <- "queued"
    job[["started_at"]] <- NULL
    job[["pending_expr"]] <- wrapped
    job[["pending_env"]] <- env
  }

  session[["jobs"]][[job_id]] <- job
  touch_session(session)
  rtemis.core::info(
    "Job ",
    job_id,
    " submitted (",
    type,
    ", ",
    job[["status"]],
    ", session ",
    session[["id"]],
    ").",
    package = "rtemis.server"
  )
  job
}


#' Promote queued jobs into running when slots are available
#'
#' Walks every session's job env, collects queued jobs across all
#' sessions, sorts by `submitted_at`, and launches as many as
#' `max_concurrent - count_active_jobs()` allows. For each promotion,
#' emits a `job.started` event to the owning session.
#'
#' Called once per tick from `loop_tick()`.
#'
#' @param server Server env.
#'
#' @return Integer. Number of jobs promoted this tick.
#'
#' @author EDG
#' @keywords internal
#' @noRd
promote_queued_jobs <- function(server) {
  max_c <- server[["max_concurrent"]] %||% 8L
  free <- max_c - count_active_jobs()
  if (free <= 0L) {
    return(0L)
  }

  reg <- session_registry()
  queued <- list()
  for (sid in ls(reg)) {
    s <- reg[[sid]]
    jobs <- s[["jobs"]]
    for (jid in ls(jobs)) {
      j <- jobs[[jid]]
      if (identical(j[["status"]], "queued")) {
        queued[[length(queued) + 1L]] <- j
      }
    }
  }
  if (length(queued) == 0L) {
    return(0L)
  }

  ts <- vapply(queued, function(j) as.numeric(j[["submitted_at"]]), numeric(1L))
  queued <- queued[order(ts)]

  n_promote <- min(free, length(queued))
  promoted <- 0L
  for (i in seq_len(n_promote)) {
    job <- queued[[i]]
    tryCatch(
      {
        job[["mirai"]] <- do.call(
          mirai::mirai,
          c(list(.expr = job[["pending_expr"]]), job[["pending_env"]])
        )
        job[["status"]] <- "running"
        job[["started_at"]] <- Sys.time()
        job[["pending_expr"]] <- NULL
        job[["pending_env"]] <- NULL

        s <- reg[[job[["session_id"]]]]
        if (!is.null(s)) {
          ev <- make_event(
            "job.started",
            data = list(
              job_id = job[["id"]],
              started_at = iso8601(job[["started_at"]])
            )
          )
          emit_event_to_session(server, s, ev)
        }
        rtemis.core::info(
          "Job ",
          job[["id"]],
          " started (",
          job[["type"]] %||% "?",
          ").",
          package = "rtemis.server"
        )
        promoted <- promoted + 1L
      },
      error = function(e) {
        # Promotion failed (mirai serialise error, daemon dead, emit
        # broke). Mark the job as failed so it doesn't sit in the queue
        # forever and doesn't free up a slot we then re-promote into.
        # poll_active_jobs will pick up the terminal status next tick
        # and emit job.failed.
        warning(
          sprintf(
            "rtemislive: promotion failed for job %s: %s",
            job[["id"]],
            conditionMessage(e)
          ),
          call. = FALSE
        )
        job[["status"]] <- "failed"
        job[["completed_at"]] <- Sys.time()
        job[["pending_expr"]] <- NULL
        job[["pending_env"]] <- NULL
        job[["error"]] <- list(
          code = "internal_error",
          message = paste0(
            "Failed to start queued job: ",
            conditionMessage(e)
          )
        )
      }
    )
  }
  promoted
}


#' Queue position (1-based) of a queued job
#'
#' Counts queued jobs across all sessions with an earlier
#' `submitted_at`, plus one. Returns `NULL` for non-queued jobs.
#'
#' @param job Job env.
#'
#' @return Integer scalar or `NULL`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
job_queue_position <- function(job) {
  if (!identical(job[["status"]], "queued")) {
    return(NULL)
  }
  my_ts <- as.numeric(job[["submitted_at"]])
  ahead <- 0L
  reg <- session_registry()
  for (sid in ls(reg)) {
    jobs <- reg[[sid]][["jobs"]]
    for (jid in ls(jobs)) {
      j <- jobs[[jid]]
      if (
        identical(j[["status"]], "queued") &&
          as.numeric(j[["submitted_at"]]) < my_ts
      ) {
        ahead <- ahead + 1L
      }
    }
  }
  ahead + 1L
}


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
}


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
  if (identical(job[["status"]], "queued")) {
    return(FALSE) # not started yet; no mirai handle to poll
  }
  if (mirai::unresolved(job[["mirai"]])) {
    return(FALSE)
  }
  finalize_job(job)
  TRUE
}


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

  dur <- if (!is.null(job[["started_at"]])) {
    sprintf(
      " in %.2fs",
      as.numeric(
        difftime(job[["completed_at"]], job[["started_at"]], units = "secs")
      )
    )
  } else {
    ""
  }
  if (identical(job[["status"]], "failed")) {
    rtemis.core::warn(
      "Job ",
      job[["id"]],
      " failed",
      dur,
      ": ",
      job[["error"]][["message"]],
      package = "rtemis.server"
    )
  } else {
    rtemis.core::info(
      "Job ",
      job[["id"]],
      " ",
      job[["status"]],
      dur,
      ".",
      package = "rtemis.server"
    )
  }

  invisible(job)
}


#' Format a mirai error value into a short string
#'
#' @author EDG
#' @keywords internal
#' @noRd
format_mirai_error <- function(value) {
  .msg <- attr(value, "message", exact = TRUE)
  if (is.null(.msg)) {
    .msg <- tryCatch(as.character(value), error = function(e) "Unknown error.")
  }
  if (length(.msg) > 1L) {
    .msg <- paste(.msg, collapse = " ")
  }
  .msg
}


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
    rtemis.core::abort(
      "Unknown job_id '",
      job_id,
      "'.",
      class = "rtemislive_not_found"
    )
  }
  if (job[["status"]] %in% c("complete", "failed", "cancelled")) {
    return(FALSE)
  }
  if (identical(job[["status"]], "queued")) {
    # Never reached a daemon; transition straight to cancelled. The
    # `emitted_resolution` flag is intentionally left unset so
    # `poll_active_jobs()` will emit the `job.cancelled` event on the
    # next tick.
    job[["status"]] <- "cancelled"
    job[["completed_at"]] <- Sys.time()
    job[["pending_expr"]] <- NULL
    job[["pending_env"]] <- NULL
    job[["error"]] <- list(
      code = "cancelled",
      message = "Job cancelled before start."
    )
    touch_session(session)
    rtemis.core::info(
      "Job ",
      job_id,
      " cancelled before start.",
      package = "rtemis.server"
    )
    return(TRUE)
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
  rtemis.core::info(
    "Job ",
    job_id,
    " cancellation requested.",
    package = "rtemis.server"
  )
  TRUE
}


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
}


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
  out <- list(
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
  if (identical(job[["status"]], "queued")) {
    out[["queue_position"]] <- job_queue_position(job)
  }
  out
}


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
}


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
  if (job[["status"]] %in% c("running", "cancelling", "queued")) {
    rtemis.core::abort(
      "Cannot delete a ",
      job[["status"]],
      " job - cancel and wait first.",
      class = "rtemislive_invalid_params"
    )
  }
  rm(list = job_id, envir = jobs)
  touch_session(session)
  TRUE
}


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
    rtemis.core::abort("`progress` must be a list.")
  }
  cur <- job[["progress"]]
  for (k in names(progress)) {
    cur[[k]] <- progress[[k]]
  }
  job[["progress"]] <- cur
  invisible(job)
}
