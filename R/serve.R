# serve.R
# ::rtemis.server::
# 2026- EDG rtemis.org

# Public entry point for the rtemislive backend.
#
# Stack:
#
# - `nanonext::http_server()` + `nanonext::handler_ws()` for the WebSocket
#   layer. `handler_ws` delivers each complete binary WebSocket message to the
#   per-connection callbacks (open / message / close).
# - `mirai::daemons()` for the compute pool, on the compute profile
#   `.RTEMISLIVE_COMPUTE`. Daemons on the caller's other profiles are outside
#   the server's control.
# - `later::later()` for periodic non-WebSocket work: job-resolution polling,
#   progress drain, heartbeats, GC. `run_server()` drives the event loop, which
#   also runs the WebSocket callbacks.

# %% serve ----

#' Start the rtemislive backend
#'
#' Launches a local-only WebSocket server that bridges the rtemislive
#' browser frontend to a persistent R session running rtemis. Provides
#' async training, real-time progress, structured result transfer, and
#' session-aware state across reconnects.
#'
#' Blocks until interrupted (Ctrl-C, or SIGINT to the R process), then returns.
#' Stopping the server closes every connection, stops every job, and discards
#' the sessions with their data and results.
#'
#' The server runs its worker processes on a mirai compute profile of its own,
#' so daemons set by the caller keep running. One server runs per R process.
#'
#' A connection must come from an allowed origin and present the token within
#' 10 seconds. The third failed attempt closes it.
#'
#' @section Waiting for the browser:
#' The server sends each message synchronously. A browser tab that stops
#' reading, such as one paused in a debugger or busy with a long computation,
#' makes the server wait once about 1 MiB is unread, until the tab continues or
#' closes. While it waits, the server handles no request and no interrupt.
#' Running jobs continue. In headless Chromium, tabs that were hidden or frozen
#' kept reading; Firefox, Safari, and the memory and energy saving of Chrome
#' are untested.
#'
#' @param port Optional Integer \[1024, 49151\]: TCP port to listen on. If
#'   `NULL`, defaults to the environment variable `RTEMISLIVE_PORT`, or to the
#'   standard rtemislive port if that is unset.
#' @param host Character \{"127.0.0.1", "localhost"\}: Loopback address to bind.
#' @param n_daemons Integer \[1, Inf): Number of worker processes. rtemis
#'   training jobs parallelize internally, so one worker runs one job at a time
#'   with access to every core. With more workers, simultaneous jobs share the
#'   cores.
#' @param origins Optional Character: Allowed `Origin` headers of the WebSocket
#'   upgrade request. If `NULL`, defaults to `https://live.rtemis.org`,
#'   `http://localhost:3000`, and `http://127.0.0.1:3000`.
#' @param token Optional Character: Token that clients must present, at least 16
#'   printable ASCII characters without spaces. If `NULL`, a random token is
#'   generated and printed. Required when `verbosity` is less than 1.
#' @param heartbeat_interval Numeric \[0, Inf): Seconds between `heartbeat`
#'   events. `0` disables them.
#' @param session_ttl Numeric (0, Inf): Seconds after its last activity at which
#'   a session without a connection or an unfinished job is removed.
#' @param data_ttl Numeric (0, Inf): Seconds after its last use at which an
#'   uploaded dataset is removed.
#' @param gc_interval Numeric (0, Inf): Seconds between removal passes.
#' @param tick_ms Numeric \[1, Inf): Milliseconds between passes of background
#'   work.
#' @param max_concurrent Integer \[1, Inf): Maximum number of jobs that run at the
#'   same time, across all sessions. Further jobs are queued.
#' @param max_sessions Integer \[1, Inf): Maximum number of sessions.
#' @param verbosity Integer \[0, Inf): Verbosity level. `0L` prints nothing.
#'
#' @return `NULL`, invisibly, after the server stops.
#'
#' @author EDG
#' @export
#'
#' @examples
#' \dontrun{
#' # Blocks the session and listens on a local port. Ctrl-C stops the server.
#' serve()
#'
#' # Two workers and a supplied token.
#' serve(
#'   n_daemons = 2L,
#'   origins = "http://localhost:3000",
#'   token = "abcd-1234-ef56-7890"
#' )
#' }
serve <- function(
  port = NULL,
  host = "127.0.0.1",
  n_daemons = 1L,
  origins = NULL,
  token = NULL,
  heartbeat_interval = 0,
  session_ttl = 86400,
  data_ttl = 3600,
  gc_interval = 60,
  tick_ms = 50L,
  max_concurrent = n_daemons,
  max_sessions = 16L,
  verbosity = 1L
) {
  # Arguments
  port <- resolve_port(port)
  if (
    !is.character(host) ||
      length(host) != 1L ||
      !host %in% c("127.0.0.1", "localhost")
  ) {
    rtemis.core::abort(
      "`host` must be \"127.0.0.1\" or \"localhost\": the server accepts ",
      "connections from the same machine only."
    )
  }
  n_daemons <- check_count(n_daemons, "n_daemons")
  max_concurrent <- check_count(max_concurrent, "max_concurrent")
  max_sessions <- check_count(max_sessions, "max_sessions")
  verbosity <- check_count(verbosity, "verbosity", min = 0L)
  check_seconds(heartbeat_interval, "heartbeat_interval", allow_zero = TRUE)
  check_seconds(session_ttl, "session_ttl")
  check_seconds(data_ttl, "data_ttl")
  check_seconds(gc_interval, "gc_interval")
  tick_ms <- check_count(tick_ms, "tick_ms")
  origins <- normalize_origins(origins)
  token_generated <- is.null(token)
  if (token_generated) {
    if (verbosity < 1L) {
      rtemis.core::abort(
        "`token` is required when `verbosity` is 0: a generated token is ",
        "printed, and clients need it to connect."
      )
    }
    token <- generate_token()
  } else {
    check_supplied_token(token)
  }
  if (mirai::daemons_set(.compute = .RTEMISLIVE_COMPUTE)) {
    rtemis.core::abort(
      "The mirai compute profile \"",
      .RTEMISLIVE_COMPUTE,
      "\" is in use. One server runs per R process."
    )
  }

  # `info()`, `warn()`, and `success()` read this option.
  old_options <- options(rtemis.server.verbosity = verbosity)
  on.exit(options(old_options), add = TRUE)

  # `stop_server()` runs before the option is restored, and is registered
  # before the first resource is created.
  server <- new_server_state(
    token = token,
    origins = origins,
    max_concurrent = max_concurrent,
    max_sessions = max_sessions,
    heartbeat_interval = heartbeat_interval,
    session_ttl = session_ttl,
    data_ttl = data_ttl,
    gc_interval = gc_interval
  )
  on.exit(stop_server(server), add = TRUE, after = FALSE)

  # The port is bound first: a busy port fails before any process starts.
  # Connections made from here on are handled when `run_server()` starts.
  start_listener(server, host, port)
  start_workers(server, n_daemons)
  schedule_tick(server, tick_ms / 1000)

  server[["running"]] <- TRUE
  rtemis.core::success(
    "rtemislive listening on ws://",
    host,
    ":",
    port,
    package = "rtemis.server"
  )
  rtemis.core::info(
    "Allowed origins: ",
    paste(origins, collapse = ", "),
    package = "rtemis.server"
  )
  if (token_generated) {
    rtemis.core::info(
      "Connection token: ",
      token,
      package = "rtemis.server"
    )
  }
  rtemis.core::info("Press Ctrl-C to stop.", package = "rtemis.server")

  run_server(server)
  invisible(NULL)
}


# %% resolve_port ----
#' Resolve the port to listen on
#'
#' @param port Optional Integer: Value of the `port` argument of `serve()`.
#'
#' @return Integer scalar.
#'
#' @author EDG
#' @keywords internal
#' @noRd
resolve_port <- function(port) {
  name <- "`port`"
  if (is.null(port)) {
    env_port <- Sys.getenv("RTEMISLIVE_PORT", unset = "")
    if (!nzchar(env_port)) {
      return(5757L)
    }
    name <- "Environment variable `RTEMISLIVE_PORT`"
    port <- suppressWarnings(as.numeric(env_port))
  }
  if (
    !is.numeric(port) ||
      length(port) != 1L ||
      !is.finite(port) ||
      port != round(port) ||
      port < 1024 ||
      port > 49151
  ) {
    rtemis.core::abort(name, " must be a whole number from 1024 to 49151.")
  }
  as.integer(port)
}


# %% check_count ----
#' Check that an argument is a whole number
#'
#' @param x Value of the argument.
#' @param name Character: Argument name, for the message.
#' @param min Integer: Smallest allowed value.
#'
#' @return Integer scalar.
#'
#' @author EDG
#' @keywords internal
#' @noRd
check_count <- function(x, name, min = 1L) {
  if (
    !is.numeric(x) ||
      length(x) != 1L ||
      !is.finite(x) ||
      x != round(x) ||
      x < min ||
      x > .Machine[["integer.max"]]
  ) {
    rtemis.core::abort(
      "`",
      name,
      "` must be a whole number of at least ",
      min,
      "."
    )
  }
  as.integer(x)
}


# %% check_seconds ----
#' Check that an argument is a duration in seconds
#'
#' @param x Value of the argument.
#' @param name Character: Argument name, for the message.
#' @param allow_zero Logical: If `TRUE`, zero is allowed.
#'
#' @return `x`, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
check_seconds <- function(x, name, allow_zero = FALSE) {
  if (
    !is.numeric(x) ||
      length(x) != 1L ||
      is.na(x) ||
      x < 0 ||
      (!allow_zero && x == 0)
  ) {
    rtemis.core::abort(
      "`",
      name,
      "` must be a single ",
      if (allow_zero) "non-negative" else "positive",
      " number of seconds."
    )
  }
  invisible(x)
}


# %% start_listener ----
#' Bind the WebSocket listener
#'
#' Stores the HTTP server as `server$http`.
#'
#' @param server Server env.
#' @param host Character: Loopback address.
#' @param port Integer: TCP port.
#'
#' @return `NULL`, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
start_listener <- function(server, host, port) {
  e <- tryCatch(
    {
      server[["http"]] <- nanonext::http_server(
        url = paste0("http://", host, ":", port),
        handlers = list(ws_handler(server))
      )
      server[["http"]]$start()
      NULL
    },
    error = identity
  )
  if (!is.null(e)) {
    rtemis.core::abort(
      "Could not listen on ",
      host,
      ":",
      port,
      " (",
      sub("^[0-9]+ \\| ", "", conditionMessage(e)),
      ").\nStop the process that uses the port, or choose another `port`.",
      parent = e
    )
  }
  invisible(NULL)
}


# %% start_workers ----
#' Start the worker processes and the progress channel
#'
#' @param server Server env.
#' @param n_daemons Integer: Number of worker processes.
#'
#' @return `NULL`, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
start_workers <- function(server, n_daemons) {
  rtemis.core::info(
    "Starting ",
    n_daemons,
    " mirai daemon",
    if (n_daemons == 1L) "" else "s",
    "...",
    package = "rtemis.server"
  )
  url <- default_progress_url()
  server[["progress_sock"]] <- bind_progress_socket(url)
  live[["rtemislive_progress_url"]] <- url
  server[["n_daemons"]] <- n_daemons
  server[["workers_started"]] <- TRUE
  mirai::daemons(n_daemons, .compute = .RTEMISLIVE_COMPUTE)
  server[["last_launch"]] <- Sys.time()
  invisible(NULL)
}


# %% schedule_tick ----
#' Schedule the recurring background work
#'
#' Stores the function that cancels the pending tick as `server$cancel_tick`.
#'
#' @param server Server env.
#' @param seconds Numeric: Delay between ticks.
#'
#' @return `NULL`, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
schedule_tick <- function(server, seconds) {
  tick <- function() {
    guard_callback(server, "tick", host_tick(server))
    if (!isTRUE(server[["stop_requested"]])) {
      server[["cancel_tick"]] <- later::later(tick, delay = seconds)
    }
  }
  server[["cancel_tick"]] <- later::later(tick, delay = seconds)
  invisible(NULL)
}


# %% guard_callback ----
#' Evaluate the body of an event-loop callback
#'
#' An error becomes a warning, so one failing callback leaves the server
#' running. An interrupt requests a stop: the loop's own handler is out of
#' reach when a callback runs in a C-level context.
#'
#' @param server Server env.
#' @param what Character: Callback name, for the warning.
#' @param expr Expression to evaluate.
#'
#' @return `NULL`, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
guard_callback <- function(server, what, expr) {
  tryCatch(
    expr,
    error = function(e) {
      rtemis.core::warn(
        what,
        " error: ",
        conditionMessage(e),
        package = "rtemis.server"
      )
    },
    interrupt = function(e) {
      server[["stop_requested"]] <- TRUE
    }
  )
  invisible(NULL)
}


# %% ws_handler ----
#' Build the WebSocket handler of a server
#'
#' @param server Server env.
#'
#' @return Handler object from `nanonext::handler_ws()`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
ws_handler <- function(server) {
  # ws$id -> connection env. The id namespace of nanonext is independent of
  # `conn$id`, which keys `server$connections`.
  ws_lookup <- new.env(parent = emptyenv())

  nanonext::handler_ws(
    path = "/",
    textframes = FALSE,
    on_open = function(ws, req) {
      guard_callback(server, "open", {
        conn <- open_connection(server, ws, req)
        if (!is.null(conn)) {
          ws_lookup[[as.character(ws[["id"]])]] <- conn
        }
      })
    },
    on_message = function(ws, data) {
      guard_callback(server, "message", {
        conn <- ws_lookup[[as.character(ws[["id"]])]]
        if (!is.null(conn)) {
          handle_message(conn, data, server)
        }
      })
    },
    on_close = function(ws) {
      guard_callback(server, "close", {
        key <- as.character(ws[["id"]])
        conn <- ws_lookup[[key]]
        if (!is.null(conn)) {
          rm(list = key, envir = ws_lookup)
          disconnect_connection(server, conn)
        }
      })
    }
  )
}


# %% open_connection ----
#' Admit or reject a newly opened WebSocket
#'
#' @param server Server env.
#' @param ws nanonext WebSocket connection.
#' @param req List: Upgrade request with `uri` and `headers`.
#'
#' @return Connection env, or `NULL` when the origin is not allowed.
#'
#' @author EDG
#' @keywords internal
#' @noRd
open_connection <- function(server, ws, req) {
  origin <- request_header(req[["headers"]], "Origin")
  if (!check_origin(origin, server[["origins"]])) {
    rtemis.core::warn(
      "Connection rejected: disallowed origin '",
      origin %||% "<none>",
      "'.",
      package = "rtemis.server"
    )
    ws$close()
    return(NULL)
  }
  conn <- new_connection(
    send_raw = function(bytes) ws$send(bytes),
    close_socket = function() ws$close()
  )
  register_connection(server, conn)
  rtemis.core::info(
    "Connection opened: ",
    conn[["id"]],
    " (origin '",
    origin,
    "', ",
    length(server[["connections"]]),
    " active).",
    package = "rtemis.server"
  )
  emit_event_to_connection(
    server,
    conn,
    make_event(
      "ready",
      data = list(
        v = 1L,
        server = "rtemislive",
        rtemis_version = tryCatch(
          as.character(utils::packageVersion("rtemis")),
          error = function(e) NA_character_
        ),
        # rtemis's node-kind palette, so the browser colors live progress and
        # timeline bars from the one definition in R. The client needs it
        # before the first job reports.
        kind_colors = tryCatch(
          as.list(rtemis::session_kind_colors()),
          error = function(e) NULL
        )
      )
    )
  )
  conn
}


# %% run_server ----
#' Run the event loop until a stop is requested
#'
#' Returns when `server$stop_requested` is set or on an interrupt.
#'
#' @param server Server env.
#'
#' @return `NULL`, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
run_server <- function(server) {
  tryCatch(
    while (!isTRUE(server[["stop_requested"]])) {
      # The loop is shared with the rest of the R session, whose callbacks may
      # raise errors.
      tryCatch(
        later::run_now(1),
        error = function(e) {
          rtemis.core::warn(
            "event loop error: ",
            conditionMessage(e),
            package = "rtemis.server"
          )
        }
      )
    },
    interrupt = function(e) {
      server[["stop_requested"]] <- TRUE
    }
  )
  invisible(NULL)
}


# %% stop_server ----
#' Stop a server and release everything it holds
#'
#' Runs on every exit from `serve()`, including an error during startup, and
#' releases what exists at that point. Sessions end with the server, so jobs
#' are stopped without a grace period.
#'
#' @param server Server env.
#'
#' @return `NULL`, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
stop_server <- function(server) {
  # A second Ctrl-C must not leave the port bound or the daemons running.
  suspendInterrupts({
    server[["stop_requested"]] <- TRUE
    if (is.function(server[["cancel_tick"]])) {
      server[["cancel_tick"]]()
    }
    n_jobs <- sum(vapply(
      as.list(session_registry()),
      count_unfinished_jobs,
      integer(1L)
    ))
    if (!is.null(server[["http"]])) {
      tryCatch(server[["http"]]$close(), error = function(e) NULL)
      server[["http"]] <- NULL
    }
    if (isTRUE(server[["workers_started"]])) {
      tryCatch(
        mirai::daemons(0L, .compute = .RTEMISLIVE_COMPUTE),
        error = function(e) NULL
      )
    }
    close_dialog(server)
    close_progress_socket(server[["progress_sock"]])
    server[["progress_sock"]] <- NULL
    live[["rtemislive_progress_url"]] <- NULL
    clear_sessions()
    # nanonext runs the close callbacks and frees the server in callbacks of
    # the event loop. Until they run, the port stays unavailable to a new
    # server in this R process.
    tryCatch(later::run_now(0), error = function(e) NULL)
  })
  if (isTRUE(server[["running"]])) {
    server[["running"]] <- FALSE
    rtemis.core::info(
      "Server stopped.",
      if (n_jobs > 0L) {
        paste0(" Unfinished jobs stopped: ", n_jobs, ".")
      },
      package = "rtemis.server"
    )
  }
  invisible(NULL)
}
