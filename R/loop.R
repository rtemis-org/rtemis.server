# loop.R
# ::rtemis.server::
# 2026- EDG rtemis.org

# Connection handling, event delivery, and periodic host work.
#
# WebSocket messages arrive through the callbacks of `ws_handler()`, which call
# `handle_message()`. `serve()` schedules `host_tick()`, which:
#
# 1. Closes connections that stay unauthenticated.
# 2. Replaces daemons that died.
# 3. Drains the progress socket: each message becomes a `job.progress` event of
#    the owning session.
# 4. Polls active jobs and emits `job.complete` / `job.failed` /
#    `job.cancelled`.
# 5. Starts queued jobs.
# 6. Emits heartbeats and removes idle sessions and stale data handles.

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


#' Close a connection
#'
#' Detaches the connection from its session (emitting a
#' `session.connection_left` event to remaining peers), removes it from
#' the server's connection registry, and closes its WebSocket. The first call
#' does this; later calls for the same connection return at once, as when the
#' WebSocket's close callback follows a close by the server.
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
  if (isTRUE(conn[["closed"]])) {
    return(invisible(NULL))
  }
  conn[["closed"]] <- TRUE
  cid <- conn[["id"]]
  reg <- server[["connections"]]
  if (exists(cid, envir = reg, inherits = FALSE)) {
    rm(list = cid, envir = reg)
  }
  s <- connection_session(conn)
  conn[["session_id"]] <- NULL
  if (is.function(conn[["close_socket"]])) {
    tryCatch(conn[["close_socket"]](), error = function(e) NULL)
  }
  if (!is.null(s)) {
    detach_connection(s, cid)
    # Presence concerns the connections that remain. Without one, the event
    # would occupy the replay buffer of the next `session.join`.
    if (length(s[["connections"]]) > 0L) {
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
  }
  rtemis.core::info(
    "Connection closed: ",
    cid,
    if (!is.null(s)) paste0(" (session ", s[["id"]], ")") else "",
    " (",
    length(server[["connections"]]),
    " active).",
    package = "rtemis.server"
  )
  invisible(NULL)
}


#' Close the connections that stay unauthenticated
#'
#' A connection must authenticate within `.RTEMISLIVE_AUTH_TIMEOUT` seconds of
#' event-loop time. Time during which a request blocked the loop counts for at
#' most one second, because the connection's `auth` message may be waiting
#' behind that request.
#'
#' @param server Server env.
#'
#' @return Integer - number of connections closed.
#'
#' @author EDG
#' @keywords internal
#' @noRd
drop_unauthenticated <- function(server) {
  now <- Sys.time()
  elapsed <- as.numeric(difftime(
    now,
    server[["last_auth_check"]] %||% now,
    units = "secs"
  ))
  elapsed <- min(max(elapsed, 0), 1)
  server[["last_auth_check"]] <- now
  closed <- 0L
  for (cid in ls(server[["connections"]])) {
    conn <- server[["connections"]][[cid]]
    if (is.null(conn) || isTRUE(conn[["authed"]])) {
      next
    }
    conn[["unauthed_seconds"]] <- conn[["unauthed_seconds"]] + elapsed
    if (conn[["unauthed_seconds"]] >= .RTEMISLIVE_AUTH_TIMEOUT) {
      rtemis.core::warn(
        "Closing ",
        cid,
        ": not authenticated within ",
        .RTEMISLIVE_AUTH_TIMEOUT,
        " s.",
        package = "rtemis.server"
      )
      disconnect_connection(server, conn)
      closed <- closed + 1L
    }
  }
  closed
}


# %% Per-connection I/O -----------------------------------------------------

#' Send a raw frame on a connection
#'
#' `ws$send()` reports a closed connection by returning an `errorValue`; it
#' raises no condition.
#'
#' @param conn Connection env.
#' @param bytes Raw vector.
#'
#' @return Logical scalar: `FALSE` on failure, which callers treat as a
#'   disconnect.
#'
#' @author EDG
#' @keywords internal
#' @noRd
write_frame <- function(conn, bytes) {
  send_fn <- conn[["send_raw"]]
  if (!is.raw(bytes) || !is.function(send_fn) || isTRUE(conn[["closed"]])) {
    return(FALSE)
  }
  res <- tryCatch(send_fn(bytes), error = function(e) e)
  !inherits(res, "error") && !inherits(res, "errorValue")
}


#' Encode a handler's response
#'
#' Two response shapes: a plain header (a list with `v` at the top level) from
#' most handlers, and `{header, payload}` from `make_response_payload()`. A
#' response that exceeds the frame limits becomes an error response, so that
#' the client's request resolves.
#'
#' @param response Named list: Response of `dispatch_request()`.
#'
#' @return Raw vector.
#'
#' @author EDG
#' @keywords internal
#' @noRd
encode_response <- function(response) {
  header <- response
  payload <- NULL
  if (is.null(response[["v"]]) && !is.null(response[["header"]])) {
    header <- response[["header"]]
    payload <- response[["payload"]]
  }
  tryCatch(
    encode_frame(header, payload),
    error = function(e) {
      encode_frame(make_error(
        header[["id"]],
        "internal_error",
        paste0("Could not encode the response: ", conditionMessage(e))
      ))
    }
  )
}


#' Handle one WebSocket message
#'
#' One message carries one frame. A malformed message gets an error response.
#' After a response that ends the connection, the WebSocket is closed. Messages
#' of a closed connection are ignored.
#'
#' Before authentication, every message except a failed `auth` below the limit
#' ends the connection. `ws$send()` blocks R until the client reads, so a client
#' without the token must receive no more bytes than the socket buffers hold:
#' the `ready` event and at most `.RTEMISLIVE_AUTH_ATTEMPTS` short replies.
#'
#' @param conn Connection env.
#' @param bytes Raw vector: The message.
#' @param server Server env.
#'
#' @return Logical scalar: `TRUE` if a request was dispatched.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_message <- function(conn, bytes, server) {
  if (isTRUE(conn[["closed"]])) {
    return(FALSE)
  }
  frame <- tryCatch(
    decode_frame(bytes),
    error = function(e) {
      make_error(NA_character_, "malformed_frame", conditionMessage(e))
    }
  )
  decoded <- !is.null(frame[["header"]])
  response <- if (decoded) dispatch_request(conn, frame, server) else frame
  sent <- is.null(response) || write_frame(conn, encode_response(response))
  is_auth <- decoded && identical(frame[["header"]][["method"]], "auth")
  if (
    !sent ||
      isTRUE(conn[["close_after_response"]]) ||
      (!isTRUE(conn[["authed"]]) && !is_auth)
  ) {
    disconnect_connection(server, conn)
  }
  decoded
}


# %% Fan-out to connections -------------------------------------------------

#' Send an event to every connection attached to a session
#'
#' When the session has no connections attached, or the last of them fails
#' during delivery, the event is buffered on the session via `push_event()` for
#' replay on next attach.
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
      detach_connection(session, cid)
      next
    }
    ok <- write_frame(conn, buf)
    if (!ok) {
      disconnect_connection(server, conn)
      next
    }
    sent <- sent + 1L
  }
  if (sent == 0L) {
    # Every write failed: the session has no connection left.
    push_event(session, event)
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


#' Replay a session's buffered events to a joining connection
#'
#' Drains the session's disconnect buffer (events accumulated while no
#' connection was attached) and writes each one, in order, to the freshly
#' joined connection. If the buffer overflowed while detached, a trailing
#' `session.events_dropped` event reports the drop count so the client can
#' resync via `job.list` / `data.list` (spec paragraph 5.8).
#'
#' On the first write failure the connection is disconnected and replay
#' stops - remaining events are lost (the connection is gone).
#'
#' @param server Server env.
#' @param session Session env.
#' @param conn Connection env - the joining connection.
#'
#' @return Integer - number of buffered events written.
#'
#' @author EDG
#' @keywords internal
#' @noRd
replay_buffered_events <- function(server, session, conn) {
  drained <- drain_event_buffer(session)
  sent <- 0L
  for (event in drained[["events"]]) {
    if (!emit_event_to_connection(server, conn, event)) {
      return(sent)
    }
    sent <- sent + 1L
  }
  if (drained[["dropped"]] > 0L) {
    emit_event_to_connection(
      server,
      conn,
      make_event(
        "session.events_dropped",
        list(events_dropped = drained[["dropped"]])
      )
    )
  }
  sent
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
      # Per-job tryCatch: a throw inside check_job_resolved (e.g. a
      # mirai value that fails to deserialize) or inside the emit path
      # must not stall the entire tick. After repeated failures we mark
      # the job as failed and stop retrying so it can't block
      # `count_active_jobs()` forever.
      result <- tryCatch(
        {
          # `check_job_resolved()` short-circuits TRUE for already-
          # terminal jobs and polls mirai only when the status is still
          # in-flight. Pre-terminal jobs without an emitted_resolution
          # flag still need to fire their event here.
          if (!check_job_resolved(job)) {
            "skip"
          } else {
            ev <- job_resolution_event(job)
            if (!is.null(ev)) {
              emit_event_to_session(server, s, ev)
              emitted <- emitted + 1L
            }
            job[["emitted_resolution"]] <- TRUE
            "ok"
          }
        },
        error = function(e) {
          rtemis.core::warn(
            "Poll failed for job ",
            job[["id"]],
            ": ",
            conditionMessage(e),
            package = "rtemis.server"
          )
          job[["poll_error_count"]] <- (job[["poll_error_count"]] %||% 0L) + 1L
          if (job[["poll_error_count"]] >= 3L) {
            job[["status"]] <- "failed"
            job[["completed_at"]] <- Sys.time()
            job[["error"]] <- list(
              code = "internal_error",
              message = paste0(
                "Server failed to finalize job after 3 attempts: ",
                conditionMessage(e)
              )
            )
            job[["emitted_resolution"]] <- TRUE
            tryCatch(
              emit_event_to_session(server, s, job_resolution_event(job)),
              error = function(e) NULL
            )
          }
          "error"
        }
      )
      invisible(result)
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
  # `heartbeat_interval <= 0` (the default) disables emission entirely. The
  # heartbeat carries only live daemon/job counts, which clients fetch on
  # demand via `info`; keep the emit path here so it can be re-enabled by
  # passing a positive interval.
  hb <- server[["heartbeat_interval"]]
  if (
    is.numeric(hb) &&
      length(hb) == 1L &&
      is.finite(hb) &&
      hb > 0 &&
      difftime(now, server[["last_heartbeat"]], units = "secs") >= hb
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


# %% Daemons -----------------------------------------------------------------

#' Replace daemons that died
#'
#' mirai leaves a pool one daemon smaller after a crash. At most three
#' replacements start in any 60 seconds, so a job that kills every daemon it
#' meets does not start processes without bound; queued jobs wait meanwhile. A
#' launched daemon has 30 seconds to connect before it counts as missing.
#'
#' @param server Server env.
#'
#' @return Integer - number of daemons launched.
#'
#' @author EDG
#' @keywords internal
#' @noRd
replace_daemons <- function(server) {
  n_daemons <- server[["n_daemons"]]
  if (is.null(n_daemons)) {
    return(0L)
  }
  now <- Sys.time()
  seconds_since <- function(t) as.numeric(difftime(now, t, units = "secs"))
  # `status()` queries the dispatcher; once a second is enough.
  if (seconds_since(server[["last_daemon_check"]] %||% (now - 1)) < 1) {
    return(0L)
  }
  server[["last_daemon_check"]] <- now
  missing_daemons <- n_daemons - daemon_count()
  if (missing_daemons <= 0L) {
    server[["daemons_ready"]] <- TRUE
    return(0L)
  }
  if (
    !isTRUE(server[["daemons_ready"]]) &&
      seconds_since(server[["last_launch"]]) < 30
  ) {
    return(0L)
  }
  recent <- Filter(function(t) seconds_since(t) < 60, server[["replacements"]])
  n <- min(missing_daemons, 3L - length(recent))
  if (n <= 0L) {
    return(0L)
  }
  rtemis.core::warn(
    missing_daemons,
    " of ",
    n_daemons,
    " daemons ended unexpectedly. Starting ",
    n,
    ".",
    package = "rtemis.server"
  )
  mirai::launch_local(n, .compute = .RTEMISLIVE_COMPUTE)
  server[["replacements"]] <- c(recent, rep(list(now), n))
  server[["last_launch"]] <- now
  server[["daemons_ready"]] <- FALSE
  n
}


# %% Host tick ---------------------------------------------------------------

#' Run one tick of background host work
#'
#' @param server Server env.
#'
#' @return Named list of per-step counts.
#'
#' @author EDG
#' @keywords internal
#' @noRd
host_tick <- function(server) {
  unauthenticated_closed <- drop_unauthenticated(server)
  daemons_launched <- replace_daemons(server)
  progress_routed <- drain_and_route_progress(server)
  jobs_resolved <- poll_active_jobs(server)
  jobs_promoted <- promote_queued_jobs(server)
  periodic <- maybe_tick_periodic(server)
  list(
    unauthenticated_closed = unauthenticated_closed,
    daemons_launched = daemons_launched,
    progress_routed = progress_routed,
    jobs_resolved = jobs_resolved,
    jobs_promoted = jobs_promoted,
    heartbeats_emitted = periodic[["heartbeats_emitted"]],
    gc_ran = periodic[["gc_ran"]]
  )
}
