# 2026- EDG rtemis.org

# Public entry point for the rtemislive backend.
#
# Stack:
#
# - `nanonext::http_server()` + `nanonext::handler_ws()` for the
#   browser-compatible WebSocket layer. `handler_ws` accepts unlimited
#   concurrent clients, exposes per-connection callbacks (open / message
#   / close), and delivers raw binary frames straight to the browser
#   `WebSocket` API.
# - `mirai::daemons()` for the async compute pool. Daemons are
#   pre-warmed with rtemis loaded and a `msg_sink` that forwards
#   `msg()` calls back to the host via the progress channel.
# - `later::later()` for periodic non-WS work - job-resolution polling,
#   progress drain, heartbeats, GC - scheduled on the same event loop
#   `http_server$serve()` already drives.
#
# All component-level testing is in test_rtemislive_*.R. A smoke
# integration test (separate R process for the server + a client
# connecting via `nanonext::stream`) lives in
# test_serve_integration.R.

# %% serve --------------------------------------------------------

#' Start the rtemislive backend
#'
#' Launches a local-only WebSocket server that bridges the rtemislive
#' browser frontend to a persistent R session running rtemis. Provides
#' async training, real-time progress, structured result transfer, and
#' session-aware state across reconnects.
#'
#' Blocks until the user interrupts (Ctrl-C) or another mechanism sets
#' `server$stop_requested`. See `specs/rtemislive.md` for the wire
#' protocol and architectural details.
#'
#' @param port Integer: TCP port to listen on. Must be in `1024:49151`.
#'   Defaults to `5757`, or the value of `RTEMISLIVE_PORT` if set.
#' @param host Character: Bind address. Defaults to `"127.0.0.1"`.
#'   Server refuses to start on any other address.
#' @param n_daemons Integer: Number of mirai worker processes. Default
#'   `1L`: rtemis training jobs already parallelise internally (OpenMP
#'   for gradient boosters, parallel tuning, parallel outer resampling),
#'   so a single daemon runs one job at a time with full core access.
#'   Increase only when you want multiple simultaneous jobs and accept
#'   that each gets a fraction of the cores.
#' @param origins Optional character vector: Allowed `Origin` headers
#'   on the WS upgrade. `NULL` uses the spec defaults
#'   (`live.rtemis.org`, `localhost:3000`, etc.).
#' @param token Optional character scalar: Auth token clients must
#'   present. `NULL` generates a fresh 8-byte random token.
#' @param heartbeat_interval Numeric, seconds: Per-session `heartbeat`
#'   tick rate. `0` (the default) disables heartbeat emission; pass a
#'   positive value to re-enable it. The heartbeat carries only live
#'   daemon/job counts, which clients fetch on demand via `info`.
#' @param session_ttl Numeric, seconds: Idle-session GC TTL.
#' @param data_ttl Numeric, seconds: Idle-data-handle GC TTL.
#' @param gc_interval Numeric, seconds: How often GC runs.
#' @param tick_ms Integer milliseconds: Background tick rate for the
#'   non-WS periodic work scheduled via `later`. Default `50`.
#' @param max_concurrent Integer: Cap on concurrent jobs across all
#'   sessions. Defaults to `n_daemons`: no point queuing more running
#'   jobs than there are workers.
#' @param max_sessions Integer: Cap on the number of sessions.
#' @param verbosity Integer: `>= 1L` prints the startup banner.
#'
#' @return Server env, invisibly. Returned after the loop exits so
#'   callers (notably tests running the server on a mirai task) can
#'   inspect state.
#'
#' @author EDG
#' @export
#'
#' @seealso [shutdown()]
#'
#' @examples
#' \dontrun{
#' # Run on the default port; Ctrl-C to stop.
#' serve()
#'
#' # Multiple workers for a multi-user setup (each job gets fewer cores).
#' serve(
#'   port = 5757L,
#'   n_daemons = 4L,
#'   origins = c("http://localhost:3000"),
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
  # %% Dependency checks -----
  rtemis.core::check_dependencies("nanonext", "mirai", "later", "jsonlite")

  # %% Arg validation -----
  if (is.null(port)) {
    env_port <- Sys.getenv("RTEMISLIVE_PORT", unset = "")
    port <- if (nzchar(env_port)) {
      suppressWarnings(as.integer(env_port))
    } else {
      5757L
    }
  }
  port <- as.integer(port)
  if (is.na(port) || port < 1024L || port > 49151L) {
    rtemis.core::abort(
      "`port` must be an integer in 1024:49151 (got ",
      port,
      ")."
    )
  }
  if (
    !is.character(host) ||
      length(host) != 1L ||
      !host %in% c("127.0.0.1", "localhost", "::1")
  ) {
    rtemis.core::abort(
      "`host` must be a loopback address.\n",
      "Got '",
      host,
      "'; rtemislive only binds to 127.0.0.1 / localhost / ::1."
    )
  }
  origins <- normalize_origins(origins)
  if (is.null(token)) {
    token <- generate_token()
  } else if (!is.character(token) || length(token) != 1L || !nzchar(token)) {
    rtemis.core::abort(
      "`token` must be a single non-empty character string."
    )
  }
  n_daemons <- as.integer(n_daemons)
  if (is.na(n_daemons) || n_daemons < 1L) {
    rtemis.core::abort("`n_daemons` must be a positive integer.")
  }

  # Push the requested verbosity into the option so info() / warn() / etc.
  # called anywhere downstream gate themselves correctly without each
  # callsite having to thread the value through.
  options(rtemis.server.verbosity = as.integer(verbosity))

  # %% Daemons + progress channel -----
  rtemis.core::info(
    "Starting ",
    n_daemons,
    " mirai daemon",
    if (n_daemons == 1L) "" else "s",
    "...",
    package = "rtemis.server"
  )
  mirai::daemons(n_daemons)

  progress_url <- default_progress_url()
  progress_sock <- bind_progress_socket(progress_url)
  rtemis.core::info(
    "Initialising daemon-side progress sink...",
    package = "rtemis.server"
  )
  init_daemon_progress(progress_url)

  # %% Server state -----
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
  server[["progress_sock"]] <- progress_sock

  # ws$id -> our connection env. Kept outside `server$connections` because
  # nanonext's id namespace is independent of our `conn_id`s.
  ws_lookup <- new.env(parent = emptyenv())

  # %% WebSocket handler -----
  ws_handler <- nanonext::handler_ws(
    path = "/",
    textframes = FALSE,
    on_open = function(ws, req) {
      headers <- req[["headers"]]
      origin <- if (is.list(headers)) headers[["Origin"]] else headers["Origin"]
      if (!check_origin(origin, server[["origins"]])) {
        rtemis.core::warn(
          "Connection rejected: disallowed origin '",
          origin %||% "<none>",
          "'.",
          package = "rtemis.server"
        )
        ws$close()
        return(invisible(NULL))
      }
      ws_id <- ws[["id"]]
      conn <- new_connection(send_raw = function(b) {
        if (!is.raw(b)) {
          b <- charToRaw(as.character(b))
        }
        ws$send(b)
      })
      conn[["ws_id"]] <- ws_id
      register_connection(server, conn)
      ws_lookup[[as.character(ws_id)]] <- conn
      rtemis.core::info(
        "Connection opened: ",
        conn[["id"]],
        " (origin '",
        origin %||% "<none>",
        "', ",
        length(server[["connections"]]),
        " active).",
        package = "rtemis.server"
      )

      ev <- make_event(
        "ready",
        data = list(
          v = 1L,
          server = "rtemislive",
          rtemis_version = tryCatch(
            as.character(utils::packageVersion("rtemis")),
            error = function(e) NA_character_
          )
        )
      )
      tryCatch(
        write_frame(conn, encode_frame(ev)),
        error = function(e) NULL
      )
      invisible(NULL)
    },
    on_message = function(ws, data) {
      conn <- ws_lookup[[as.character(ws[["id"]])]]
      if (is.null(conn)) {
        return(invisible(NULL))
      }
      if (is.raw(data)) {
        conn[["buffer"]] <- c(conn[["buffer"]], data)
      }
      tryCatch(
        drain_buffer(conn, server),
        error = function(e) {
          rtemis.core::warn(
            "drain error: ",
            conditionMessage(e),
            package = "rtemis.server"
          )
        }
      )
      invisible(NULL)
    },
    on_close = function(ws) {
      key <- as.character(ws[["id"]])
      conn <- ws_lookup[[key]]
      if (!is.null(conn)) {
        disconnect_connection(server, conn)
        rm(list = key, envir = ws_lookup)
      }
      invisible(NULL)
    }
  )

  http <- nanonext::http_server(
    url = paste0("http://", host, ":", port),
    handlers = list(ws_handler)
  )

  # %% Periodic background work -----
  # Scheduled on `later`'s event loop, which is the same loop
  # `http_server$serve()` drives.
  tick_seconds <- as.numeric(tick_ms) / 1000
  schedule_tick <- function() {
    if (isTRUE(server[["stop_requested"]])) {
      tryCatch(http$close(), error = function(e) NULL)
      return(invisible(NULL))
    }
    tryCatch(
      host_tick(server),
      error = function(e) {
        rtemis.core::warn(
          "tick error: ",
          conditionMessage(e),
          package = "rtemis.server"
        )
      }
    )
    later::later(schedule_tick, delay = tick_seconds)
  }
  later::later(schedule_tick, delay = 0.01)

  # %% Banner -----
  rtemis.core::success(
    "rtemislive listening on ws://",
    host,
    ":",
    port,
    package = "rtemis.server"
  )
  rtemis.core::info(
    "Allowed origins: ",
    paste(server[["origins"]], collapse = ", "),
    package = "rtemis.server"
  )
  rtemis.core::info(
    "Connection token: ",
    token,
    package = "rtemis.server"
  )

  # %% Run the loop -----
  on.exit(
    {
      tryCatch(http$close(), error = function(e) NULL)
      close_progress_socket(progress_sock)
      tryCatch(mirai::daemons(0L), error = function(e) NULL)
    },
    add = TRUE
  )
  http$serve()
  invisible(server)
}


# %% shutdown ---------------------------------------------------------

#' Signal a running rtemislive server to stop
#'
#' Sets `server$stop_requested`. On its next tick the loop will close
#' the HTTP listener (which causes `http_server$serve()` to return) and
#' `serve()` cleans up daemons and sockets via `on.exit`.
#'
#' Useful when the server runs on a mirai task or a separate R process
#' that can be passed the server env (e.g. in integration tests). For a
#' server running in the user's foreground R session, just press
#' Ctrl-C.
#'
#' @param server Server env returned (or shared) from [serve()].
#'
#' @return `NULL`, invisibly.
#'
#' @author EDG
#' @export
#'
#' @examples
#' \dontrun{
#' # Start the server on a mirai task so it doesn't block the session.
#' task <- mirai::mirai({
#'   rtemis.server::serve(port = 5757L, verbosity = 0L)
#' })
#'
#' # ... do work ...
#'
#' # Signal the server to stop gracefully.
#' rtemis.server::shutdown(task$data)
#' }
shutdown <- function(server) {
  if (!is.environment(server)) {
    rtemis.core::abort("`server` must be the env returned by `serve()`.")
  }
  server[["stop_requested"]] <- TRUE
  invisible(NULL)
}
