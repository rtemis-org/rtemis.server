# 2026- EDG rtemis.org

# Session registry and lifecycle for rtemislive. See specs/rtemislive.md paragraph 5.
#
# Storage model
# -------------
# Sessions hold genuinely mutable state - jobs come and go, data handles
# are added and dropped, connections attach and detach, `last_seen` ticks
# on nearly every operation. Rather than reconstructing an immutable S7
# object on every mutation, internal session state is held in plain R
# environments (which have reference semantics). The registry itself is an
# env keyed by session id, stored in rtemis's `live` env.
#
# S7 wrapping is reserved for **wire-shaped snapshots** (the structure the
# frontend sees on `session.join` / `session.list`), produced on demand by
# `session_snapshot()`. This keeps validation at the boundary where it
# matters most (responses going out the WebSocket) without paying for it
# on every internal write.
#
# Each session env holds:
#   id            character - `sess-<hex>`
#   name          character - user-chosen or `untitled-<n>`
#   created_at    POSIXct
#   last_seen     POSIXct - touched on any activity
#   jobs          env - job_id -> job env (filled by rtemislive_jobs.R)
#   data          env - data_handle -> data env (filled by rtemislive_data.R)
#   connections   character - vector of attached connection_ids
#   event_buffer  list - bounded ring of unsent push events
#   events_dropped integer - count of dropped events since last drain
#   max_buffer    integer - capacity of event_buffer

# %% Registry plumbing -------------------------------------------------------

#' Internal session registry env
#'
#' Lazy-initialized env stored under `live[["rtemislive_sessions"]]`.
#' Keys are session ids; values are session envs.
#'
#' @return Environment.
#'
#' @author EDG
#' @keywords internal
#' @noRd
session_registry <- function() {
  reg <- live[["rtemislive_sessions"]]
  if (is.null(reg)) {
    reg <- new.env(parent = emptyenv())
    live[["rtemislive_sessions"]] <- reg
  }
  reg
} # /rtemis::session_registry


#' Clear the session registry
#'
#' Removes every session and its state. Used at server shutdown and in tests.
#'
#' @return `NULL`, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
clear_sessions <- function() {
  reg <- session_registry()
  rm(list = ls(reg, all.names = TRUE), envir = reg)
  invisible(NULL)
} # /rtemis::clear_sessions


# %% Identifier helpers ------------------------------------------------------

#' Generate a session id
#'
#' Returns `sess-<hex16>`. Uses uuid if available (preferred), otherwise
#' falls back to R PRNG.
#'
#' @return Character scalar.
#'
#' @author EDG
#' @keywords internal
#' @noRd
new_session_id <- function() {
  rtemis.core::check_dependencies("uuid")
  hex <- gsub("-", "", uuid::UUIDgenerate(use.time = TRUE), fixed = TRUE)
  paste0("sess-", substr(hex, 1L, 16L))
} # /rtemis::new_session_id


# %% Name validation ---------------------------------------------------------

#' Validate a user-supplied session name
#'
#' Names are 1-64 chars, drawn from `[A-Za-z0-9_.-]`. Throws on violation
#' with `rtemislive_invalid_name` class so handlers can map to the wire
#' error code.
#'
#' @param name Character scalar.
#'
#' @return `name`, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
validate_session_name <- function(name) {
  if (!is.character(name) || length(name) != 1L || is.na(name)) {
    cli::cli_abort(
      "Session name must be a single non-NA character string.",
      class = "rtemislive_invalid_name"
    )
  }
  if (!nzchar(name) || nchar(name) > 64L) {
    cli::cli_abort(
      "Session name must be 1-64 characters.",
      class = "rtemislive_invalid_name"
    )
  }
  if (!grepl("^[A-Za-z0-9_.-]+$", name)) {
    cli::cli_abort(
      "Session name may contain only letters, digits, `.`, `_`, and `-`.",
      class = "rtemislive_invalid_name"
    )
  }
  invisible(name)
} # /rtemis::validate_session_name


#' Generate the next available `untitled-<n>` name
#'
#' Used when `session.create` is called without a name.
#'
#' @return Character scalar.
#'
#' @author EDG
#' @keywords internal
#' @noRd
next_anon_session_name <- function() {
  existing <- vapply(
    ls(session_registry()),
    function(id) session_registry()[[id]][["name"]],
    character(1L)
  )
  i <- 1L
  repeat {
    candidate <- paste0("untitled-", i)
    if (!candidate %in% existing) {
      return(candidate)
    }
    i <- i + 1L
  }
} # /rtemis::next_anon_session_name


# %% Lookup ------------------------------------------------------------------

#' Get a session by id
#'
#' @param id Character scalar.
#'
#' @return Session env, or `NULL` if not found.
#'
#' @author EDG
#' @keywords internal
#' @noRd
get_session_by_id <- function(id) {
  reg <- session_registry()
  if (!is.character(id) || length(id) != 1L || is.na(id)) {
    return(NULL)
  }
  if (!exists(id, envir = reg, inherits = FALSE)) {
    return(NULL)
  }
  reg[[id]]
} # /rtemis::get_session_by_id


#' Get a session by name
#'
#' @param name Character scalar.
#'
#' @return Session env, or `NULL` if no session with that name exists.
#'
#' @author EDG
#' @keywords internal
#' @noRd
get_session_by_name <- function(name) {
  if (!is.character(name) || length(name) != 1L || is.na(name)) {
    return(NULL)
  }
  reg <- session_registry()
  for (id in ls(reg)) {
    s <- reg[[id]]
    if (identical(s[["name"]], name)) {
      return(s)
    }
  }
  NULL
} # /rtemis::get_session_by_name


#' Look up a session by name or id
#'
#' @param key Character scalar - name or id.
#'
#' @return Session env, or `NULL`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
get_session <- function(key) {
  s <- get_session_by_id(key)
  if (!is.null(s)) {
    return(s)
  }
  get_session_by_name(key)
} # /rtemis::get_session


# %% Lifecycle ---------------------------------------------------------------

#' Create and register a new session
#'
#' @param name Optional character scalar. When `NULL`, an `untitled-<n>`
#'   name is generated.
#' @param max_buffer Integer: Capacity of the unsent-event ring buffer
#'   (default 256, spec paragraph 5.8).
#' @param max_sessions Integer: Cap on total sessions in the registry
#'   (default 16, spec paragraph 11.4).
#'
#' @return Session env.
#'
#' @author EDG
#' @keywords internal
#' @noRd
new_session <- function(name = NULL, max_buffer = 256L, max_sessions = 16L) {
  if (length(ls(session_registry())) >= max_sessions) {
    cli::cli_abort(
      "Maximum number of sessions ({max_sessions}) reached.",
      class = "rtemislive_too_many_sessions"
    )
  }

  if (is.null(name)) {
    name <- next_anon_session_name()
  }
  validate_session_name(name)

  if (!is.null(get_session_by_name(name))) {
    cli::cli_abort(
      c(
        "A session named {.val {name}} already exists.",
        "i" = "Use `session.join` to attach to it instead."
      ),
      class = "rtemislive_session_exists"
    )
  }

  now <- Sys.time()
  s <- new.env(parent = emptyenv())
  s[["id"]] <- new_session_id()
  s[["name"]] <- name
  s[["created_at"]] <- now
  s[["last_seen"]] <- now
  s[["jobs"]] <- new.env(parent = emptyenv())
  s[["data"]] <- new.env(parent = emptyenv())
  s[["connections"]] <- character(0L)
  s[["event_buffer"]] <- list()
  s[["events_dropped"]] <- 0L
  s[["max_buffer"]] <- as.integer(max_buffer)

  reg <- session_registry()
  reg[[s[["id"]]]] <- s
  s
} # /rtemis::new_session


#' Update `last_seen` on a session
#'
#' Called from any function that touches session state. Decoupled into a
#' helper so the timestamp logic lives in one place.
#'
#' @param session Session env.
#'
#' @return The session, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
touch_session <- function(session) {
  session[["last_seen"]] <- Sys.time()
  invisible(session)
} # /rtemis::touch_session


#' Delete a session
#'
#' @param id Character scalar: Session id.
#'
#' @return Logical - `TRUE` if a session was removed, `FALSE` if it
#'   didn't exist.
#'
#' @author EDG
#' @keywords internal
#' @noRd
delete_session <- function(id) {
  reg <- session_registry()
  if (!is.character(id) || length(id) != 1L || is.na(id)) {
    return(FALSE)
  }
  if (!exists(id, envir = reg, inherits = FALSE)) {
    return(FALSE)
  }
  rm(list = id, envir = reg)
  TRUE
} # /rtemis::delete_session


#' Rename a session
#'
#' @param session Session env.
#' @param new_name Character scalar.
#'
#' @return The session, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
rename_session <- function(session, new_name) {
  validate_session_name(new_name)
  if (identical(session[["name"]], new_name)) {
    return(invisible(session))
  }
  if (!is.null(get_session_by_name(new_name))) {
    cli::cli_abort(
      "A session named {.val {new_name}} already exists.",
      class = "rtemislive_session_exists"
    )
  }
  session[["name"]] <- new_name
  touch_session(session)
} # /rtemis::rename_session


#' Garbage-collect idle sessions
#'
#' A session is eligible for GC when it has zero attached connections
#' **and** `last_seen` is older than `ttl` seconds. Active sessions are
#' never collected (their jobs may still be running).
#'
#' @param now POSIXct: Reference time (default `Sys.time()`).
#' @param ttl Numeric, seconds: Default 86400 (24 h, spec paragraph 5.7).
#'
#' @return Character vector of ids that were collected.
#'
#' @author EDG
#' @keywords internal
#' @noRd
gc_sessions <- function(now = Sys.time(), ttl = 86400) {
  reg <- session_registry()
  expired <- character(0L)
  for (id in ls(reg)) {
    s <- reg[[id]]
    if (
      length(s[["connections"]]) == 0L &&
        difftime(now, s[["last_seen"]], units = "secs") > ttl
    ) {
      expired <- c(expired, id)
    }
  }
  for (id in expired) {
    rm(list = id, envir = reg)
  }
  expired
} # /rtemis::gc_sessions


# %% Connection attach/detach ------------------------------------------------

#' Attach a connection to a session
#'
#' Idempotent - attaching the same connection_id twice is a no-op.
#'
#' @param session Session env.
#' @param connection_id Character scalar.
#'
#' @return The session, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
attach_connection <- function(session, connection_id) {
  if (!is.character(connection_id) || length(connection_id) != 1L) {
    cli::cli_abort("`connection_id` must be a single character string.")
  }
  session[["connections"]] <- unique(c(session[["connections"]], connection_id))
  touch_session(session)
} # /rtemis::attach_connection


#' Detach a connection from a session
#'
#' @param session Session env.
#' @param connection_id Character scalar.
#'
#' @return The session, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
detach_connection <- function(session, connection_id) {
  session[["connections"]] <- setdiff(session[["connections"]], connection_id)
  touch_session(session)
} # /rtemis::detach_connection


# %% Event buffering ---------------------------------------------------------

#' Push an event onto a session's buffer (when no connections are attached)
#'
#' When the session has zero attached connections, the caller's event would
#' have nowhere to go - instead, it's appended to a bounded ring buffer.
#' On the next `session.join`, the buffer is replayed to the joining
#' connection.
#'
#' When the session **does** have attached connections, this function
#' returns `FALSE` without buffering - the caller is expected to send the
#' event directly on each connection.
#'
#' Buffer is capped at `session$max_buffer` (default 256). Oldest events
#' are dropped first; the drop count is preserved in
#' `session$events_dropped` for client resync.
#'
#' @param session Session env.
#' @param event Any R object - typically the result of `make_event()`.
#'
#' @return Logical - `TRUE` if the event was buffered, `FALSE` if it
#'   should be sent directly because connections are attached.
#'
#' @author EDG
#' @keywords internal
#' @noRd
push_event <- function(session, event) {
  if (length(session[["connections"]]) > 0L) {
    return(FALSE)
  }
  cap <- session[["max_buffer"]]
  buf <- session[["event_buffer"]]
  if (length(buf) >= cap) {
    overflow <- length(buf) - cap + 1L
    buf <- buf[seq.int(overflow + 1L, length(buf))]
    session[["events_dropped"]] <- session[["events_dropped"]] + overflow
  }
  session[["event_buffer"]] <- c(buf, list(event))
  touch_session(session)
  TRUE
} # /rtemis::push_event


#' Drain a session's event buffer
#'
#' Returns the buffered events and the count of events dropped since the
#' last drain, then resets both.
#'
#' @param session Session env.
#'
#' @return Named list with `events` (list of event objects) and `dropped`
#'   (integer count).
#'
#' @author EDG
#' @keywords internal
#' @noRd
drain_event_buffer <- function(session) {
  out <- list(
    events = session[["event_buffer"]],
    dropped = session[["events_dropped"]]
  )
  session[["event_buffer"]] <- list()
  session[["events_dropped"]] <- 0L
  out
} # /rtemis::drain_event_buffer


# %% Wire-shaped views -------------------------------------------------------

#' Summarize all sessions for the `session.list` response
#'
#' @return List of named lists, one per session in the registry.
#'
#' @author EDG
#' @keywords internal
#' @noRd
list_sessions <- function() {
  reg <- session_registry()
  lapply(ls(reg), function(id) {
    s <- reg[[id]]
    list(
      session_id = s[["id"]],
      name = s[["name"]],
      created = iso8601(s[["created_at"]]),
      last_seen = iso8601(s[["last_seen"]]),
      n_connections = length(s[["connections"]]),
      n_jobs = length(ls(s[["jobs"]])),
      jobs_running = count_jobs_running(s)
    )
  })
} # /rtemis::list_sessions


#' Full session snapshot for `session.join` / `session.info`
#'
#' Lists current jobs (status-only summaries), data handles, and basic
#' identity. Used to bring a newly-attached client up to speed.
#'
#' @param session Session env.
#'
#' @return Named list ready to embed in a response `result`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
session_snapshot <- function(session) {
  jobs <- lapply(ls(session[["jobs"]]), function(jid) {
    j <- session[["jobs"]][[jid]]
    list(
      job_id = j[["id"]],
      status = j[["status"]] %||% "unknown",
      stage = j[["stage"]] %||% NULL,
      fraction = j[["fraction"]] %||% NULL
    )
  })
  data_handles <- lapply(ls(session[["data"]]), function(dh) {
    d <- session[["data"]][[dh]]
    list(
      data_handle = d[["handle"]] %||% dh,
      name = d[["name"]] %||% NA_character_,
      rows = d[["rows"]] %||% NA_integer_,
      cols = d[["cols"]] %||% NA_integer_
    )
  })
  list(
    session_id = session[["id"]],
    name = session[["name"]],
    created = iso8601(session[["created_at"]]),
    last_seen = iso8601(session[["last_seen"]]),
    n_connections = length(session[["connections"]]),
    jobs = jobs,
    data = data_handles
  )
} # /rtemis::session_snapshot


# %% Small helpers -----------------------------------------------------------

iso8601 <- function(t) {
  format(t, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
}

count_jobs_running <- function(session) {
  jobs <- session[["jobs"]]
  if (length(ls(jobs)) == 0L) {
    return(0L)
  }
  sum(vapply(
    ls(jobs),
    function(jid) identical(jobs[[jid]][["status"]], "running"),
    logical(1L)
  ))
}

`%||%` <- function(a, b) if (is.null(a)) b else a
