# 2026- EDG rtemis.org

# Host event loop for rtemislive. See specs/rtemislive.md paragraph 13.
#
# The loop is single-threaded and non-blocking. Each tick:
#
# 1. (delegated to serve.R) Accept any newly-arrived WS connections.
# 2. For each open connection: pull bytes from its underlying nanonext
#    stream into its buffer; parse complete frames; dispatch each;
#    encode the response and send it back on the same stream.
# 3. Drain the progress pull socket: route each forwarded message to the
#    owning session as a `job.progress` event (sent to attached
#    connections, or buffered on the session when none are attached).
# 4. Walk all active jobs across all sessions; for any that have just
#    resolved, emit the corresponding `job.complete` / `job.failed` /
#    `job.cancelled` event.
# 5. Periodic ticks: emit per-session `heartbeat` events, GC idle
#    sessions and stale data handles.
#
# The functions in this file are written so each step is independently
# testable. WS-specific I/O (accept, raw read, raw send) is encapsulated
# in tiny helpers that tests can replace with in-memory stand-ins.

# %% Connection registry ----------------------------------------------------

#' Register a connection on the server
#'
#' Stores the connection in `server$connections` keyed by `conn$id`.
#'
#' @param server Server env.
#' @param conn Connection env.
#'
#' @return The connection, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
register_connection <- function(server, conn) {
  server[["connections"]][[conn[["id"]]]] <- conn
  invisible(conn)
}


#' Unregister and clean up a connection
#'
#' Detaches the connection from its session (emitting a
#' `session.connection_left` event to remaining peers), removes it from
#' the server's connection registry, and tries to close the underlying
#' socket if present.
#'
#' @param server Server env.
#' @param conn Connection env.
#'
#' @return `NULL`, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
disconnect_connection <- function(server, conn) {
  cid <- conn[["id"]]
  s <- connection_session(conn)
  if (!is.null(s)) {
    detach_connection(s, cid)
    ev <- make_event(
      "session.connection_left",
      data = list(
        session_id = s[["id"]],
        connection_id = cid,
        n_connections = length(s[["connections"]])
      )
    )
    emit_event_to_session(server, s, ev)
  }
  conn[["session_id"]] <- NULL
  if (!is.null(conn[["socket"]])) {
    tryCatch(close_progress_socket(conn[["socket"]]), error = function(e) NULL)
    conn[["socket"]] <- NULL
  }
  reg <- server[["connections"]]
  if (exists(cid, envir = reg, inherits = FALSE)) {
    rm(list = cid, envir = reg)
  }
  invisible(NULL)
}


# %% Per-connection I/O -----------------------------------------------------

#' Pull bytes from a connection's socket into its read buffer
#'
#' Default implementation reads from `conn$socket` with
#' `nanonext::recv(... block = FALSE)`. Returns `TRUE` if the socket is
#' healthy (regardless of whether any bytes arrived), `FALSE` if the
#' socket has errored / closed.
#'
#' Tests stuff bytes directly into `conn$buffer` and skip this step.
#'
#' @param conn Connection env.
#'
#' @return Logical scalar.
#'
#' @author EDG
#' @keywords internal
#' @noRd
read_into_buffer <- function(conn) {
  sock <- conn[["socket"]]
  if (is.null(sock)) {
    return(TRUE) # nothing to read; not an error
  }
  repeat {
    chunk <- tryCatch(
      nanonext::recv(sock, mode = "raw", block = FALSE),
      error = function(e) NULL
    )
    if (is.null(chunk)) {
      break
    }
    if (inherits(chunk, "errorValue")) {
      # 8 == NNG_ETIMEDOUT (no data right now) is OK; other errors are fatal.
      if (identical(unclass(chunk), 8L)) {
        break
      }
      return(FALSE)
    }
    if (!is.raw(chunk) || length(chunk) == 0L) {
      break
    }
    conn[["buffer"]] <- c(conn[["buffer"]], chunk)
  }
  TRUE
}


#' Send a raw response frame on a connection
#'
#' Calls `conn$send_raw(bytes)` if installed, else attempts
#' `nanonext::send()` on `conn$socket`. Returns `TRUE` on success,
#' `FALSE` on error (caller can treat that as a disconnect signal).
#'
#' @param conn Connection env.
#' @param bytes Raw vector.
#'
#' @return Logical scalar.
#'
#' @author EDG
#' @keywords internal
#' @noRd
write_frame <- function(conn, bytes) {
  if (!is.raw(bytes)) {
    return(FALSE)
  }
  send_fn <- conn[["send_raw"]]
  if (is.function(send_fn)) {
    res <- tryCatch(send_fn(bytes), error = function(e) e)
    return(!inherits(res, "error"))
  }
  sock <- conn[["socket"]]
  if (is.null(sock)) {
    return(FALSE)
  }
  res <- tryCatch(
    nanonext::send(sock, bytes, mode = "raw", block = FALSE),
    error = function(e) e
  )
  if (inherits(res, "error") || inherits(res, "errorValue")) {
    return(FALSE)
  }
  TRUE
}


#' Parse complete frames out of a connection's buffer and dispatch each
#'
#' Reads as many complete frames as the buffer holds, dispatches each,
#' encodes the response, sends it. If a frame is malformed, sends an
#' error response and continues (per spec - malformed input shouldn't
#' kill the connection, but the server logs the issue).
#'
#' Advances `conn$buffer` past consumed bytes after every successful
#' parse.
#'
#' If `conn$close_after_response` becomes TRUE (e.g. 3 failed auth
#' attempts in the handler), the connection is disconnected after the
#' current frame's response is sent.
#'
#' @param conn Connection env.
#' @param server Server env.
#'
#' @return Integer - number of frames dispatched this call.
#'
#' @author EDG
#' @keywords internal
#' @noRd
drain_buffer <- function(conn, server) {
  dispatched <- 0L
  repeat {
    parsed <- tryCatch(
      decode_frame(conn[["buffer"]]),
      error = function(e) {
        write_frame(
          conn,
          encode_frame(
            make_error(NA_character_, "malformed_frame", conditionMessage(e))
          )
        )
        # Reset buffer - we can't safely resume from a corrupted point.
        conn[["buffer"]] <- raw(0L)
        list(complete = FALSE)
      }
    )
    if (!isTRUE(parsed[["complete"]])) {
      break
    }

    consumed <- parsed[["consumed"]]
    if (consumed > 0L && consumed <= length(conn[["buffer"]])) {
      conn[["buffer"]] <- conn[["buffer"]][-(seq_len(consumed))]
    } else if (consumed > 0L) {
      conn[["buffer"]] <- raw(0L)
    }

    response <- dispatch_request(
      conn,
      list(header = parsed[["header"]], payload = parsed[["payload"]]),
      server
    )
    if (!is.null(response)) {
      # Two response shapes:
      # - plain header: a list with `v` at the top level (most handlers)
      # - {header, payload}: from `make_response_payload()` for binary slices
      if (is.null(response[["v"]]) && !is.null(response[["header"]])) {
        bytes <- encode_frame(response[["header"]], response[["payload"]])
      } else {
        bytes <- encode_frame(response)
      }
      ok <- write_frame(conn, bytes)
      if (!ok) {
        disconnect_connection(server, conn)
        return(dispatched)
      }
    }
    dispatched <- dispatched + 1L

    if (isTRUE(conn[["close_after_response"]])) {
      disconnect_connection(server, conn)
      return(dispatched)
    }
  }
  dispatched
}


#' One full read+drain pass for a single connection
#'
#' Pulls bytes from the socket, then dispatches every complete frame.
#' On socket error, disconnects.
#'
#' @param conn Connection env.
#' @param server Server env.
#'
#' @return Integer - frames dispatched.
#'
#' @author EDG
#' @keywords internal
#' @noRd
process_connection <- function(conn, server) {
  if (!read_into_buffer(conn)) {
    disconnect_connection(server, conn)
    return(0L)
  }
  drain_buffer(conn, server)
}


# %% Fan-out to connections -------------------------------------------------

#' Send an event to every connection attached to a session
#'
#' When the session has no connections attached, the event is buffered
#' on the session via `push_event()` for replay on next attach.
#'
#' Errors writing to one connection trigger a disconnect for that
#' connection but don't block delivery to others.
#'
#' @param server Server env.
#' @param session Session env.
#' @param event Event envelope (from `make_event()`).
#'
#' @return Integer - number of connections the event was sent to.
#'
#' @author EDG
#' @keywords internal
#' @noRd
emit_event_to_session <- function(server, session, event) {
  if (length(session[["connections"]]) == 0L) {
    push_event(session, event)
    return(0L)
  }
  buf <- encode_frame(event)
  sent <- 0L
  for (cid in session[["connections"]]) {
    conn <- server[["connections"]][[cid]]
    if (is.null(conn)) {
      next
    }
    ok <- write_frame(conn, buf)
    if (!ok) {
      disconnect_connection(server, conn)
      next
    }
    sent <- sent + 1L
  }
  sent
}


#' Send an event to a specific connection
#'
#' On write failure, disconnects the connection.
#'
#' @param server Server env.
#' @param conn Connection env.
#' @param event Event envelope.
#'
#' @return Logical scalar.
#'
#' @author EDG
#' @keywords internal
#' @noRd
emit_event_to_connection <- function(server, conn, event) {
  ok <- write_frame(conn, encode_frame(event))
  if (!ok) {
    disconnect_connection(server, conn)
  }
  ok
}


# %% Progress drain + routing ----------------------------------------------

#' Drain the progress pull socket and route each message
#'
#' Each routed envelope becomes a `job.progress` event delivered (or
#' buffered) on the owning session.
#'
#' @param server Server env.
#'
#' @return Integer - number of messages routed.
#'
#' @author EDG
#' @keywords internal
#' @noRd
drain_and_route_progress <- function(server) {
  sock <- server[["progress_sock"]]
  if (is.null(sock)) {
    return(0L)
  }
  msgs <- drain_progress_socket(sock)
  if (length(msgs) == 0L) {
    return(0L)
  }
  route_progress(
    msgs,
    send_event = function(session, event) {
      emit_event_to_session(server, session, event)
    }
  )
}


# %% Job resolution polling -------------------------------------------------

#' Build the resolution event for a finalized job
#'
#' @param job Job env.
#'
#' @return Event envelope, or `NULL` if the job didn't actually resolve.
#'
#' @author EDG
#' @keywords internal
#' @noRd
job_resolution_event <- function(job) {
  switch(
    job[["status"]],
    "complete" = make_event(
      "job.complete",
      data = list(
        job_id = job[["id"]],
        summary = job_summary(job)
      )
    ),
    "failed" = make_event(
      "job.failed",
      data = list(
        job_id = job[["id"]],
        error = job[["error"]]
      )
    ),
    "cancelled" = make_event(
      "job.cancelled",
      data = list(
        job_id = job[["id"]]
      )
    ),
    NULL
  )
}


#' Poll every active job across all sessions
#'
#' For each job whose mirai has just resolved, emits the appropriate
#' resolution event to its session's attached connections (or buffers
#' it) and marks the job so the event isn't re-emitted on later ticks.
#'
#' @param server Server env.
#'
#' @return Integer - number of resolution events emitted this tick.
#'
#' @author EDG
#' @keywords internal
#' @noRd
poll_active_jobs <- function(server) {
  reg <- session_registry()
  emitted <- 0L
  for (sid in ls(reg)) {
    s <- reg[[sid]]
    jobs <- s[["jobs"]]
    for (jid in ls(jobs)) {
      job <- jobs[[jid]]
      if (isTRUE(job[["emitted_resolution"]])) {
        next
      }
      # `check_job_resolved()` short-circuits TRUE for already-terminal
      # jobs and polls mirai only when the status is still in-flight.
      # Pre-terminal jobs without an emitted_resolution flag still need
      # to fire their event here (e.g. jobs finalized between server
      # restarts in tests, or finalized via direct `check_job_resolved`
      # by client-driven `job.result` requests).
      if (!check_job_resolved(job)) {
        next
      }
      ev <- job_resolution_event(job)
      if (!is.null(ev)) {
        emit_event_to_session(server, s, ev)
        emitted <- emitted + 1L
      }
      job[["emitted_resolution"]] <- TRUE
    }
  }
  emitted
}


# %% Periodic ticks ---------------------------------------------------------

#' Emit a `heartbeat` event for every session
#'
#' Updates `server$last_heartbeat`.
#'
#' @param server Server env.
#'
#' @return Integer - number of sessions a heartbeat was emitted for.
#'
#' @author EDG
#' @keywords internal
#' @noRd
emit_heartbeats <- function(server) {
  reg <- session_registry()
  ids <- ls(reg)
  for (sid in ids) {
    s <- reg[[sid]]
    ev <- make_event(
      "heartbeat",
      data = list(
        ts = iso8601(Sys.time()),
        daemon_count = daemon_count(),
        jobs_running = count_active_jobs()
      )
    )
    emit_event_to_session(server, s, ev)
  }
  server[["last_heartbeat"]] <- Sys.time()
  length(ids)
}


#' Run the periodic GC pass: idle sessions + stale data handles
#'
#' Updates `server$last_gc`.
#'
#' @param server Server env.
#'
#' @return Named list - `sessions_dropped` (character) and
#'   `data_handles_dropped` (named list per session).
#'
#' @author EDG
#' @keywords internal
#' @noRd
gc_tick <- function(server) {
  now <- Sys.time()
  data_dropped <- list()
  for (sid in ls(session_registry())) {
    s <- session_registry()[[sid]]
    dropped <- gc_data(s, now = now, ttl = server[["data_ttl"]])
    if (length(dropped) > 0L) {
      data_dropped[[sid]] <- dropped
    }
  }
  sessions_dropped <- gc_sessions(now = now, ttl = server[["session_ttl"]])
  server[["last_gc"]] <- now
  list(
    sessions_dropped = sessions_dropped,
    data_handles_dropped = data_dropped
  )
}


#' Run periodic ticks whose intervals have elapsed
#'
#' @param server Server env.
#'
#' @return Named list - `heartbeats_emitted` (integer), `gc_ran` (logical).
#'
#' @author EDG
#' @keywords internal
#' @noRd
maybe_tick_periodic <- function(server) {
  now <- Sys.time()
  out <- list(heartbeats_emitted = 0L, gc_ran = FALSE)
  if (
    difftime(now, server[["last_heartbeat"]], units = "secs") >=
      server[["heartbeat_interval"]]
  ) {
    out[["heartbeats_emitted"]] <- emit_heartbeats(server)
  }
  if (
    difftime(now, server[["last_gc"]], units = "secs") >=
      server[["gc_interval"]]
  ) {
    gc_tick(server)
    out[["gc_ran"]] <- TRUE
  }
  out
}


# %% Main loop --------------------------------------------------------------

#' Run one full tick of the event loop
#'
#' Order matches spec paragraph 13. WS-accept is delegated to the caller (the
#' real `serve()` integrates with `nanonext::stream`); tests
#' construct connections manually.
#'
#' @param server Server env.
#'
#' @return Named list summarising what happened this tick.
#'
#' @author EDG
#' @keywords internal
#' @noRd
loop_tick <- function(server) {
  conn_ids <- ls(server[["connections"]])
  frames_dispatched <- 0L
  for (cid in conn_ids) {
    conn <- server[["connections"]][[cid]]
    if (is.null(conn)) {
      next
    }
    frames_dispatched <- frames_dispatched + process_connection(conn, server)
  }

  progress_routed <- drain_and_route_progress(server)
  jobs_resolved <- poll_active_jobs(server)
  periodic <- maybe_tick_periodic(server)

  list(
    frames_dispatched = frames_dispatched,
    progress_routed = progress_routed,
    jobs_resolved = jobs_resolved,
    heartbeats_emitted = periodic[["heartbeats_emitted"]],
    gc_ran = periodic[["gc_ran"]]
  )
}


#' Drive the event loop until `server$stop_requested` is set
#'
#' @param server Server env.
#' @param tick_ms Numeric. Sleep between ticks in milliseconds (default 5).
#'
#' @return `NULL`, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
run_loop <- function(server, tick_ms = 5) {
  while (!isTRUE(server[["stop_requested"]])) {
    loop_tick(server)
    Sys.sleep(tick_ms / 1000)
  }
  invisible(NULL)
}
