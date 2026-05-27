# 2026- EDG rtemis.org

# Progress channel for rtemislive. See specs/rtemislive.md paragraph 9.
#
# Daemon side: each mirai daemon holds a `push` NNG socket dialing the
# host. A `msg()` sink installed on the daemon reads the current job_id
# from `rtemis::live` and forwards each message as a JSON envelope on the
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
    record_job_progress(
      job,
      list(
        stage = m[["caller"]],
        message = clean_msg,
        ts = m[["ts"]],
        level = m[["level"]]
      )
    )
    event <- make_event(
      "job.progress",
      data = list(
        job_id = jid,
        stage = m[["caller"]],
        message = clean_msg,
        ts = m[["ts"]],
        level = m[["level"]]
      )
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
#' 2. Stashes it on rtemis's internal `live` env under
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

  # IMPORTANT: mirai::everywhere() (as of mirai 2.7) only persists
  # changes to the daemon's `globalenv()`, loaded packages, and
  # options - this is documented behavior. Writes into a package's
  # namespace env (e.g. `asNamespace("rtemis")$live$x <- ...`) and
  # plain top-level `<-` assignments inside the everywhere block are
  # silently dropped after the call returns. An earlier version of this
  # function installed the msg sink + socket directly into rtemis's
  # `live` env from inside `everywhere`, which appeared to work but in
  # fact left every daemon with NULL sink/socket - no progress events
  # ever shipped.
  #
  # Workaround: use `everywhere` only to plant the URL in the daemon's
  # `options()` (which IS persisted), then let `ensure_daemon_sink()`
  # install the socket + sink lazily at job start - that runs inside a
  # regular `mirai()` task whose namespace writes DO persist. After the
  # first job on a daemon, subsequent jobs find the sink installed and
  # skip the setup.
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
#' `init_daemon_progress` and installs an `rtemis::set_msg_sink()`
#' that forwards every `msg()` call as a JSON envelope on the socket.
#' On subsequent calls (sink already installed), short-circuits.
#'
#' Lives in the daemon's `live` env (a namespace env in rtemis) - those
#' writes persist across regular `mirai()` tasks but, critically, NOT
#' across `mirai::everywhere()` calls. See `init_daemon_progress` for
#' the full rationale.
#'
#' Exported (rather than internal) so the per-job wrapped expression
#' in `submit_job` can reference it as `rtemis.server::ensure_daemon_sink`
#' without resorting to `:::` (CRAN-discouraged) or `getFromNamespace`
#' (extra per-job lookup). It is not part of the user-facing API.
#'
#' @return Invisible `NULL`.
#'
#' @author EDG
#' @keywords internal
#' @export
ensure_daemon_sink <- function() {
  live_env <- asNamespace("rtemis")[["live"]]
  if (!is.null(live_env[["msg_sink"]])) {
    return(invisible(NULL))
  }
  url <- getOption("rtemislive.progress_url")
  if (is.null(url) || !nzchar(url)) {
    return(invisible(NULL))
  }
  sock <- nanonext::socket("push", dial = url)
  live_env[["rtemislive_progress_socket"]] <- sock
  rtemis::set_msg_sink(function(m) {
    s <- live_env[["rtemislive_progress_socket"]]
    if (is.null(s)) {
      return(invisible(NULL))
    }
    payload <- list(
      job_id = live_env[["rtemislive_job_id"]],
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
      s,
      charToRaw(as.character(txt)),
      mode = "raw",
      block = FALSE
    )
  })
  invisible(NULL)
}


# %% Progress forwarder for rtemis::train ----------------------------------------------------------

#' Forward a `rtemis::train` progress checkpoint over the msg sink
#'
#' Thin adapter passed as `progress = ` to `rtemis::train()`. Calls
#' rtemis's internal `msg()` with `caller = stage`, so the daemon-side
#' sink (installed by `init_daemon_progress`) ships an envelope whose
#' `caller` field carries the structured stage name (e.g.
#' `"outer_fold"`). The host turns that into a `job.progress` event with
#' `data$stage` set, which the UI can route on without text-matching
#' the message.
#'
#' `msg` is unexported from rtemis; the reference is bound at package
#' source-eval time in `00_init.R` via `getFromNamespace`, so calling
#' `msg()` here avoids both `rtemis:::msg` (R CMD check NOTE) and any
#' per-call namespace lookup.
#'
#' Designed to be referenced as `rtemis.server::forward_progress` inside
#' the mirai job expression - mirai loads rtemis.server on the daemon
#' on first use, sourcing `00_init.R` once, so the `msg` binding exists
#' before the callback is ever invoked.
#'
#' @param stage Character scalar: Structured stage name. Becomes the
#'   `caller` field on the wire envelope (e.g. `"outer_fold"`).
#' @param current Integer: 1-based index of the checkpoint. Unused by
#'   this adapter directly (encoded into `message` upstream), kept in
#'   the signature so it matches the rtemis::train `progress` contract.
#' @param total Integer: Total checkpoints. Same as `current` - present
#'   to match the contract.
#' @param message Character scalar: Human-readable line, e.g.
#'   `"Outer fold 2/5"`. Becomes the envelope's `text` field.
#'
#' @return Invisible `NULL`.
#'
#' @author EDG
#' @export
forward_progress <- function(stage, current, total, message) {
  msg(message, caller = stage)
  invisible(NULL)
}
