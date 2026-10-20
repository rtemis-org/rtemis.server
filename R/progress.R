# 2026- EDG rtemis.org

# Progress channel for rtemislive. See specs/rtemislive.md paragraph 9.
#
# Daemon side: each mirai daemon holds a `push` NNG socket dialing the
# host. A `msg()` sink installed on the daemon reads the current job_id
# from rtemis.server's `live` environment and forwards each message as a JSON envelope on the
# socket.
#
# Host side: the host process listens on a `pull` NNG socket. The event
# loop drains it non-blockingly each tick, looks up the owning session
# and job for each envelope, updates the job's progress snapshot, and
# emits a `job.progress` event to the session's attached connections
# (buffered when none are attached).
#
# Daemons run in separate R processes. We use `ipc://<tmp>` URLs for the
# socket so daemons can connect across processes. `inproc://` only works
# inside a single R process and is useful for tests.

# %% Progress URL ----------------------------------------------------------------------------------

#' Build a default IPC URL for the progress channel
#'
#' Produces an `ipc://` URL backed by a temp file path. Unique per server
#' start so multiple rtemislive servers on the same machine don't collide.
#'
#' @return Character scalar.
#'
#' @author EDG
#' @keywords internal
#' @noRd
default_progress_url <- function() {
  paste0("ipc://", tempfile(pattern = "rtemislive-progress-"))
}


# %% Host-side: bind / close / drain ---------------------------------------------------------------

#' Bind the host-side progress pull socket
#'
#' Opens an NNG `pull` socket listening on `url`. Daemons dial the same
#' URL with a `push` socket; messages they `send()` arrive here for the
#' host to drain.
#'
#' @param url Character. NNG URL (e.g. `"ipc:///tmp/..."` or
#'   `"inproc://name"`).
#'
#' @return The opened socket.
#'
#' @author EDG
#' @keywords internal
#' @noRd
bind_progress_socket <- function(url) {
  rtemis.core::check_dependencies("nanonext")
  if (!is.character(url) || length(url) != 1L || !nzchar(url)) {
    rtemis.core::abort("`url` must be a single non-empty character string.")
  }
  nanonext::socket("pull", listen = url)
}


#' Close a progress socket
#'
#' Idempotent - calling on an already-closed or NULL socket is a no-op.
#'
#' @param sock A nanonext socket or NULL.
#'
#' @return `NULL`, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
close_progress_socket <- function(sock) {
  if (is.null(sock)) {
    return(invisible(NULL))
  }
  tryCatch(
    nanonext::reap(sock),
    error = function(e) NULL
  )
  invisible(NULL)
}


#' Non-blocking drain of all pending progress messages
#'
#' Reads every message currently available on the pull socket and
#' decodes each as a JSON envelope. Returns when no more messages are
#' immediately available - does not block waiting for one.
#'
#' Malformed envelopes (bytes that don't decode to JSON) are silently
#' dropped - a hostile daemon shouldn't be able to crash the host loop.
#'
#' @param sock A nanonext pull socket.
#'
#' @return List of decoded envelopes (named lists with `job_id`,
#'   `caller`, `message`, `ts`, `level`).
#'
#' @author EDG
#' @keywords internal
#' @noRd
drain_progress_socket <- function(sock) {
  out <- list()
  repeat {
    val <- tryCatch(
      nanonext::recv(sock, mode = "raw", block = FALSE),
      error = function(e) NULL
    )
    if (is.null(val) || inherits(val, "errorValue")) {
      break
    }
    parsed <- tryCatch(
      jsonlite::fromJSON(rawToChar(val), simplifyVector = FALSE),
      error = function(e) NULL
    )
    if (is.list(parsed)) {
      out[[length(out) + 1L]] <- parsed
    }
  }
  out
}


# %% Routing ---------------------------------------------------------------------------------------

#' Completed share of a job's outermost progress loop
#'
#' Gives `job_summary()`'s `fraction` a producer, for clients that ask a job where it
#' stands (`job.status`, the session snapshot a reattaching client reconciles against)
#' rather than following the event stream.
#'
#' The first progress node a job opens is its outermost loop - inner loops come and go
#' beneath it - so it is remembered on the job and only its own events move the figure.
#' Whole steps only: composing the inner loops' share of the step in flight needs the
#' whole open-node tree, which is a live consumer's business rather than a snapshot
#' field's.
#'
#' @param job Job env.
#' @param m List: One decoded envelope.
#'
#' @return Numeric in `[0, 1]`, or `NULL` when this envelope does not move it (not a
#'   progress event, not the outermost loop, or an indeterminate loop).
#'
#' @author EDG
#' @keywords internal
#' @noRd
root_fraction <- function(job, m) {
  node_id <- m[["node_id"]]
  if (
    !identical(m[["level"]], "progress") ||
      is.null(node_id) ||
      !is.character(node_id) ||
      length(node_id) != 1L ||
      is.na(node_id)
  ) {
    return(NULL)
  }
  if (is.null(job[["progress_root"]])) {
    job[["progress_root"]] <- node_id
  }
  if (!identical(job[["progress_root"]], node_id)) {
    return(NULL)
  }
  total <- m[["total"]]
  current <- m[["current"]]
  if (
    is.null(total) ||
      is.null(current) ||
      is.na(total) ||
      is.na(current) ||
      total <= 0
  ) {
    return(NULL)
  }
  current / total
}

#' Find the session that owns a given job_id
#'
#' Walks every session in the registry and returns the first that has
#' a job with this id. Returns `NULL` if no session owns it.
#'
#' @param job_id Character scalar.
#'
#' @return Session env, or NULL.
#'
#' @author EDG
#' @keywords internal
#' @noRd
find_session_for_job <- function(job_id) {
  if (
    is.null(job_id) ||
      !is.character(job_id) ||
      length(job_id) != 1L ||
      is.na(job_id)
  ) {
    return(NULL)
  }
  reg <- session_registry()
  for (sid in ls(reg)) {
    s <- reg[[sid]]
    if (exists(job_id, envir = s[["jobs"]], inherits = FALSE)) {
      return(s)
    }
  }
  NULL
}


#' Route a list of progress envelopes to their sessions
#'
#' For each envelope:
#'
#' 1. Look up the owning session/job by `job_id`. Skip if not found
#'    (job may have been deleted while a stale message was in flight).
#' 2. Merge the envelope into the job's progress snapshot.
#' 3. Construct a `job.progress` event and either send it via
#'    `send_event(session, event)` if a sender is provided, or buffer
#'    it via `push_event()` if not.
#'
#' @param messages List of envelopes from `drain_progress_socket()`.
#' @param send_event Function `(session, event) -> any` or `NULL`. When
#'   supplied, called for every routed event so the host loop can
#'   forward to attached connections directly. When `NULL`, events are
#'   buffered on the session for replay on next attach.
#'
#' @return Integer - number of envelopes successfully routed.
#'
#' @author EDG
#' @keywords internal
#' @noRd
route_progress <- function(messages, send_event = NULL) {
  routed <- 0L
  for (m in messages) {
    jid <- m[["job_id"]]
    session <- find_session_for_job(jid)
    if (is.null(session)) {
      next
    }
    job <- session[["jobs"]][[jid]]
    if (is.null(job)) {
      next
    }
    # Daemon-forwarded msg() text carries the ANSI styling rtemis uses
    # for terminal output. Strip it once at the wire boundary so neither
    # the recorded snapshot nor the `job.progress` event leaks escape
    # sequences into the browser.
    clean_msg <- rtemis.core::strip_ansi(m[["message"]] %||% "")
    # Everything else the envelope carries rides along as-is. rtemis's sink fields
    # are additive by design (execution-graph ids, progress counters, labels; see
    # rtemis specs/observability.md section 5) and which ones are present depends on
    # what emitted the message, so enumerating them here would mean editing three
    # packages in lockstep every time rtemis learns to report something new. Only
    # the two renamed fields are handled by name: `caller` becomes `stage`, and the
    # ANSI-stripped text becomes `message`.
    payload <- c(
      list(
        stage = m[["caller"]],
        message = clean_msg
      ),
      m[setdiff(names(m), c("job_id", "caller", "message"))]
    )
    payload[["fraction"]] <- root_fraction(job, m)
    # The recorded snapshot answers "where does this job stand" for a client that
    # asks (`job.status`, the session snapshot on reattach), so it keeps the few
    # fields `job_summary()` reports and not the whole envelope: the envelope's
    # fields depend on what emitted the message, and merging them all would leave
    # whichever the next event omits behind as stale state - a counter with no
    # total, a node id belonging to a node that closed. Live subscribers get the
    # full envelope on the event below.
    # Absent fields are dropped rather than merged: `record_job_progress()` merges
    # by key and assigning NULL *removes* one, so recording them would have each
    # event erase what the last one knew - a progress event (no caller) blanking
    # the stage, an inner-loop event blanking the fraction. Last known wins.
    record_job_progress(
      job,
      Filter(
        Negate(is.null),
        list(
          stage = m[["caller"]],
          message = clean_msg,
          ts = m[["ts"]],
          level = m[["level"]],
          fraction = payload[["fraction"]]
        )
      )
    )
    event <- make_event(
      "job.progress",
      data = c(list(job_id = jid), payload)
    )
    if (is.function(send_event)) {
      send_event(session, event)
    } else {
      push_event(session, event)
    }
    routed <- routed + 1L
  }
  routed
}


# %% Daemon-side setup -----------------------------------------------------------------------------

#' Configure daemons to forward `msg()` calls to the host
#'
#' Runs (via `mirai::everywhere`) on every daemon in the pool. On each
#' daemon:
#'
#' 1. Opens a `push` socket dialing the supplied progress URL.
#' 2. Stores it in rtemis.server's `live` environment under
#'    `rtemislive_progress_socket`.
#' 3. Registers a `msg_sink` that reads the current `job_id` from `live`
#'    and forwards each message as a JSON envelope on the socket.
#'
#' Subsequent `msg()` / `msg0()` / `msgstart()` / `msgdone()` calls
#' anywhere in rtemis (or in code running on the daemon) become live
#' progress events automatically.
#'
#' @param url Character. The URL the host's pull socket is listening on
#'   (typically the value returned by `default_progress_url()`).
#'
#' @return Result of `mirai::everywhere()` (typically invisible NULL).
#'
#' @author EDG
#' @keywords internal
#' @noRd
init_daemon_progress <- function(url) {
  rtemis.core::check_dependencies("mirai")
  if (!is.character(url) || length(url) != 1L || !nzchar(url)) {
    rtemis.core::abort("`url` must be a single non-empty character string.")
  }

  # `mirai::everywhere()` persists changes to the daemon's `globalenv()`,
  # loaded packages, and options. It drops writes into a package namespace
  # environment and top-level `<-` assignments made inside its block, so a
  # sink or socket installed here would be NULL on every daemon.
  #
  # `everywhere` therefore only plants the URL in the daemon's `options()`.
  # `.ensure_daemon_sink()` installs the socket and sink at job start, inside
  # a regular `mirai()` task, whose namespace writes persist. Later jobs on the
  # same daemon find the sink installed and skip the setup.
  # NB: pass an UNQUOTED expression. `mirai::everywhere()` runs
  # `substitute()` on `.expr` internally, so handing it `quote({...})`
  # gives it a language object that it never evaluates - the original
  # bug that wedged this whole channel. `everywhere({...})` works.
  mirai::everywhere(
    {
      options(rtemislive.progress_url = url)
    },
    url = url
  )
}


#' Lazily install the daemon-side msg sink + push socket
#'
#' Runs as the first action of every wrapped job expression (see
#' `submit_job` in jobs.R). On the first call per daemon, opens a
#' nanonext push socket dialing the URL planted by
#' `init_daemon_progress` and installs an `rtemis.core::set_msg_sink()`
#' that forwards every `msg()` call as a JSON envelope on the socket.
#' On subsequent calls (sink already installed), short-circuits.
#'
#' The socket is stored in the daemon's copy of rtemis.server's `live`
#' environment, whose writes persist across regular `mirai()` tasks. See
#' `init_daemon_progress()` for why setup happens here.
#'
#' Exported for the per-job expression built by `submit_job()`, which runs on
#' a daemon and calls it as `rtemis.server::.ensure_daemon_sink()`.
#'
#' @return `NULL`, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @export
#' @examples
#' # Without a progress URL in `options(rtemislive.progress_url)`, returns
#' # without opening a socket.
#' .ensure_daemon_sink()
.ensure_daemon_sink <- function() {
  if (!is.null(rtemis.core::get_msg_sink())) {
    return(invisible(NULL))
  }
  url <- getOption("rtemislive.progress_url")
  if (is.null(url) || !nzchar(url)) {
    return(invisible(NULL))
  }
  sock <- nanonext::socket("push", dial = url)
  live[["rtemislive_progress_socket"]] <- sock
  rtemis.core::set_msg_sink(function(m) {
    s <- live[["rtemislive_progress_socket"]]
    if (is.null(s)) {
      return(invisible(NULL))
    }
    # The whole envelope ships. Its fields are producer-defined and additive
    # (see `rtemis.core::set_msg_sink()`), so a whitelist here would silently
    # drop whatever rtemis learns to report next. `text` is renamed `message`
    # for the wire; `route_progress()` on the host renames `caller` to `stage`.
    payload <- c(
      list(
        job_id = live[["rtemislive_job_id"]],
        message = m[["text"]]
      ),
      m[setdiff(names(m), "text")]
    )
    txt <- jsonlite::toJSON(
      payload,
      auto_unbox = TRUE,
      na = "null",
      null = "null"
    )
    nanonext::send(
      s,
      charToRaw(as.character(txt)),
      mode = "raw",
      block = FALSE
    )
  })
  invisible(NULL)
}


#' Set the id of the job running on this process
#'
#' Stores `job_id` in rtemis.server's `live` environment, where the daemon-side
#' `msg()` sink installed by `.ensure_daemon_sink()` reads it to tag each
#' forwarded message.
#'
#' Exported for the per-job expression built by `submit_job()`, which runs on
#' a daemon, sets the job id on entry, and restores the previous id on exit.
#'
#' @param job_id Optional Character: Job id. `NULL` clears the id.
#'
#' @return The previous job id, `NULL` if none was set, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @export
#' @examples
#' prev <- .set_job_id("job-1")
#' .set_job_id(prev)
.set_job_id <- function(job_id) {
  rtemis.core::check_character(job_id)
  prev <- live[["rtemislive_job_id"]]
  live[["rtemislive_job_id"]] <- job_id
  invisible(prev)
}
