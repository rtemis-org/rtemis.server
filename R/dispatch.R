# rtemislive_dispatch.R
# ::rtemis::
# 2026- EDG rtemis.org

# Method dispatch table and request handlers for rtemislive. See
# specs/rtemislive.md §5 (sessions) and §6 (methods).
#
# This file contains:
#
# - Connection state (`new_connection` and helpers)
# - The dispatcher (`dispatch_request`) — maps wire `method` to handler,
#   checks auth / attachment requirements, translates classed errors
#   thrown by lower modules into wire error envelopes
# - Connection-level handlers: `auth`, `ping`, `info`, `algorithms`
# - Session-level handlers: `session.list`, `session.create`,
#   `session.join`, `session.detach`, `session.rename`, `session.delete`,
#   `session.info`
#
# Data and job handlers (data.*, train, job.*) live in a follow-up
# turn and will plug into the same table.

# %% Connection state --------------------------------------------------------

#' Generate a connection id
#'
#' @return Character scalar — `conn-<hex16>`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
new_connection_id <- function() {
  hex <- if (requireNamespace("uuid", quietly = TRUE)) {
    gsub("-", "", uuid::UUIDgenerate(use.time = TRUE), fixed = TRUE)
  } else {
    paste0(
      sprintf("%02x", sample.int(256L, 16L, replace = TRUE) - 1L),
      collapse = ""
    )
  }
  paste0("conn-", substr(hex, 1L, 16L))
} # /rtemis::new_connection_id


#' Create a new connection state object
#'
#' Connections are plain envs (mutable state). `send_raw` is an injectable
#' closure the host event loop installs to deliver outbound frames on the
#' underlying nanonext stream. In tests it can be `NULL` (handlers don't
#' use it directly — they return the response envelope to the caller).
#'
#' @param id Character scalar or `NULL`: Auto-generated if omitted.
#' @param send_raw Function or `NULL`: Sends a raw vector on the wire.
#'
#' @return Connection env.
#'
#' @author EDG
#' @keywords internal
#' @noRd
new_connection <- function(id = NULL, send_raw = NULL) {
  c_env <- new.env(parent = emptyenv())
  c_env[["id"]] <- if (is.null(id)) new_connection_id() else id
  c_env[["authed"]] <- FALSE
  c_env[["session_id"]] <- NULL
  c_env[["created_at"]] <- Sys.time()
  c_env[["last_seen"]] <- Sys.time()
  c_env[["auth_attempts"]] <- 0L
  c_env[["close_after_response"]] <- FALSE
  c_env[["buffer"]] <- raw(0L)
  c_env[["send_raw"]] <- send_raw
  c_env
} # /rtemis::new_connection


#' Update `last_seen` on a connection
#'
#' @param conn Connection env.
#'
#' @return The connection, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
touch_connection <- function(conn) {
  conn[["last_seen"]] <- Sys.time()
  invisible(conn)
} # /rtemis::touch_connection


#' Get the session a connection is currently attached to
#'
#' @param conn Connection env.
#'
#' @return Session env or `NULL`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
connection_session <- function(conn) {
  sid <- conn[["session_id"]]
  if (is.null(sid)) {
    return(NULL)
  }
  get_session_by_id(sid)
} # /rtemis::connection_session


# %% Dispatcher --------------------------------------------------------------

#' Default server state object
#'
#' An environment (reference semantics) holding the server's mutable
#' state: configuration knobs the dispatcher reads, the connection
#' registry, timer state, and handles to the WS listener and progress
#' pull socket.
#'
#' Tests construct this directly; the real `serve()` builds
#' it once at startup and feeds the same env to every tick of the loop.
#'
#' @param token Character. Expected auth token.
#' @param origins Character vector. Allowed WS origins.
#' @param max_concurrent Integer. Cap on concurrent jobs.
#' @param max_sessions Integer. Cap on sessions.
#' @param heartbeat_interval Numeric, seconds. How often `heartbeat`
#'   events are emitted per session.
#' @param session_ttl Numeric, seconds. Idle session TTL for GC.
#' @param data_ttl Numeric, seconds. Idle data_handle TTL for GC.
#' @param gc_interval Numeric, seconds. How often GC runs.
#' @param started_at POSIXct. For uptime in `info`.
#'
#' @return Environment.
#'
#' @author EDG
#' @keywords internal
#' @noRd
new_server_state <- function(
  token = "",
  origins = .RTEMISLIVE_DEFAULT_ORIGINS,
  max_concurrent = 8L,
  max_sessions = 16L,
  heartbeat_interval = 5,
  session_ttl = 86400,
  data_ttl = 3600,
  gc_interval = 60,
  started_at = Sys.time()
) {
  e <- new.env(parent = emptyenv())
  e[["token"]] <- token
  e[["origins"]] <- origins
  e[["max_concurrent"]] <- max_concurrent
  e[["max_sessions"]] <- max_sessions
  e[["heartbeat_interval"]] <- heartbeat_interval
  e[["session_ttl"]] <- session_ttl
  e[["data_ttl"]] <- data_ttl
  e[["gc_interval"]] <- gc_interval
  e[["started_at"]] <- started_at
  # Mutable loop state — initialised by the loop, not the dispatcher.
  e[["connections"]] <- new.env(parent = emptyenv()) # conn_id → conn env
  e[["ws_listener"]] <- NULL
  e[["progress_sock"]] <- NULL
  e[["last_heartbeat"]] <- started_at
  e[["last_gc"]] <- started_at
  e[["stop_requested"]] <- FALSE
  e
} # /rtemis::new_server_state


#' Dispatch a single request frame
#'
#' Looks up the method in the dispatch table, checks the connection's
#' auth/attachment requirements, calls the handler. Classed errors
#' thrown by handlers (or by the lower modules they call) are caught
#' and translated into wire error envelopes per spec §15.
#'
#' @param conn Connection env.
#' @param frame Named list with `header` (decoded JSON) and `payload`
#'   (raw vector or NULL). Output of `decode_frame()`.
#' @param server Server state from `new_server_state()`.
#'
#' @return Named list — the response envelope, ready for `encode_frame()`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
dispatch_request <- function(conn, frame, server) {
  touch_connection(conn)
  header <- frame[["header"]]
  if (!is.list(header)) {
    return(make_error(NA_character_, "malformed_frame", "Frame has no header."))
  }
  req_id <- header[["id"]]
  if (is.null(req_id)) {
    req_id <- NA_character_
  }
  method <- header[["method"]]
  if (is.null(method) || !is.character(method) || length(method) != 1L) {
    return(make_error(
      req_id,
      "malformed_frame",
      "Missing or invalid `method` field."
    ))
  }

  entry <- .METHOD_TABLE[[method]]
  if (is.null(entry)) {
    return(make_error(
      req_id,
      "unknown_method",
      paste0("Unknown method: ", method)
    ))
  }

  # Requirement gates
  if ("authed" %in% entry$requires && !isTRUE(conn[["authed"]])) {
    return(make_error(req_id, "unauthorized", "Authenticate first."))
  }
  if ("attached" %in% entry$requires && is.null(conn[["session_id"]])) {
    return(make_error(
      req_id,
      "not_attached",
      "Attach to a session first (session.create / session.join)."
    ))
  }
  if ("unattached" %in% entry$requires && !is.null(conn[["session_id"]])) {
    return(make_error(
      req_id,
      "invalid_params",
      "Already attached to a session; detach first."
    ))
  }

  tryCatch(
    entry$handler(conn, frame, server),
    rtemislive_unauthorized = function(e) {
      make_error(req_id, "unauthorized", conditionMessage(e))
    },
    rtemislive_not_attached = function(e) {
      make_error(req_id, "not_attached", conditionMessage(e))
    },
    rtemislive_session_exists = function(e) {
      make_error(req_id, "session_exists", conditionMessage(e))
    },
    rtemislive_session_not_found = function(e) {
      make_error(req_id, "session_not_found", conditionMessage(e))
    },
    rtemislive_invalid_name = function(e) {
      make_error(req_id, "invalid_name", conditionMessage(e))
    },
    rtemislive_invalid_params = function(e) {
      make_error(req_id, "invalid_params", conditionMessage(e))
    },
    rtemislive_not_found = function(e) {
      make_error(req_id, "not_found", conditionMessage(e))
    },
    rtemislive_too_many = function(e) {
      make_error(req_id, "too_many", conditionMessage(e))
    },
    rtemislive_too_many_sessions = function(e) {
      make_error(req_id, "too_many", conditionMessage(e))
    },
    error = function(e) {
      make_error(req_id, "internal_error", conditionMessage(e))
    }
  )
} # /rtemis::dispatch_request


# %% Connection-level handlers ----------------------------------------------

#' `auth` handler
#'
#' Validates the supplied token against the server's expected token using
#' the constant-time `check_token()`. On success, sets `conn$authed`.
#' Three failed attempts mark the connection for closure
#' (`conn$close_after_response`).
#'
#' @param conn,frame,server Standard handler triple.
#'
#' @return Response envelope.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_auth <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  presented <- params[["token"]]
  if (!check_token(presented %||% "", server[["token"]] %||% "")) {
    conn[["auth_attempts"]] <- conn[["auth_attempts"]] + 1L
    if (conn[["auth_attempts"]] >= 3L) {
      conn[["close_after_response"]] <- TRUE
    }
    return(make_error(req_id, "unauthorized", "Invalid token."))
  }
  conn[["authed"]] <- TRUE
  conn[["auth_attempts"]] <- 0L
  make_response(req_id, list(connection_id = conn[["id"]]))
} # /rtemis::handle_auth


#' `ping` handler
#'
#' Liveness check. Returns `{ts}`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_ping <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  make_response(req_id, list(ts = iso8601(Sys.time())))
} # /rtemis::handle_ping


#' `info` handler
#'
#' Returns server metadata: rtemis version, R version, daemon count,
#' uptime in seconds.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_info <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  daemons <- daemon_count()
  rtemis_v <- tryCatch(
    as.character(utils::packageVersion("rtemis")),
    error = function(e) NA_character_
  )
  uptime <- as.numeric(
    difftime(Sys.time(), server[["started_at"]], units = "secs")
  )
  make_response(
    req_id,
    list(
      server = "rtemislive",
      rtemis_version = rtemis_v,
      r_version = R.version.string,
      daemons = daemons,
      uptime_seconds = uptime,
      n_sessions = length(ls(session_registry())),
      n_jobs_running = count_active_jobs()
    )
  )
} # /rtemis::handle_info


#' Return the current mirai daemon count, or 0 if mirai isn't loaded
#'
#' @author EDG
#' @keywords internal
#' @noRd
daemon_count <- function() {
  if (!requireNamespace("mirai", quietly = TRUE)) {
    return(0L)
  }
  status <- tryCatch(mirai::status(), error = function(e) NULL)
  if (is.null(status)) {
    return(0L)
  }
  d <- status[["daemons"]]
  if (is.null(d)) {
    return(0L)
  }
  if (is.matrix(d) || is.data.frame(d)) {
    return(nrow(d))
  }
  length(d)
} # /rtemis::daemon_count


#' `algorithms` handler
#'
#' Returns the catalogue of supervised learning algorithms. Each entry:
#' `{ name, description, supports_classification, supports_regression,
#' supports_survival }`. Per-algorithm hyperparameter schemas are fetched
#' separately via `handle_algorithm_describe()` (`algorithm.describe`).
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_algorithms <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  tbl <- asNamespace("rtemis")[["supervised_algorithms"]]
  if (!is.data.frame(tbl)) {
    return(make_response(req_id, list(algorithms = list())))
  }
  algorithms <- lapply(seq_len(nrow(tbl)), function(i) {
    list(
      name = as.character(tbl[i, "Name"]),
      description = as.character(tbl[i, "Description"]),
      supports_classification = isTRUE(as.logical(tbl[i, "Class"])),
      supports_regression = isTRUE(as.logical(tbl[i, "Reg"])),
      supports_survival = isTRUE(as.logical(tbl[i, "Surv"]))
    )
  })
  make_response(req_id, list(algorithms = algorithms))
} # /rtemis::handle_algorithms


# Type-name from a default value. NULL becomes "null" (which the UI
# renders as a free-text input). Length is ignored — a `c(...)` default
# is treated as a choice set elsewhere.
.hp_type_of <- function(v) {
  if (is.null(v)) {
    "null"
  } else if (is.logical(v)) {
    "logical"
  } else if (is.integer(v)) {
    "integer"
  } else if (is.double(v)) {
    "double"
  } else if (is.character(v)) {
    "character"
  } else {
    "other"
  }
}


# Build the schema for one setup_*() function.
#
# Walks the formals, classifying each arg as:
# - choices = c("a", "b", ...) → enum with the first value as default
# - any other multi-element default → flattened to its first element
# - single-value default (or NULL) → reported as-is
#
# `tunable_set` is the set of arg names considered tunable. For
# algorithm hyperparameters this is `Hyperparameters@tunable_hyperparameters`;
# for resampler / other configs pass `character(0)`.
.live_build_schema <- function(
  setup_fn,
  hp_values = list(),
  tunable_set = character()
) {
  fmls <- formals(setup_fn)
  lapply(names(fmls), function(arg) {
    raw <- fmls[[arg]]
    # Enumerated choices: `c("a", "b", ...)` — keep choices, default is first.
    choices <- NULL
    default <- tryCatch(
      eval(raw, envir = asNamespace("rtemis")),
      error = function(e) NULL
    )
    if (is.character(default) && length(default) > 1L) {
      choices <- as.list(default)
      default <- default[[1L]]
    } else if (length(default) > 1L) {
      default <- default[[1L]]
    }
    # Fall back to the constructed Hyperparameters value when the formal
    # has no usable default (missing-arg sentinel, unresolved symbol).
    if (
      missing(raw) ||
        (is.symbol(raw) &&
          !exists(as.character(raw), envir = asNamespace("rtemis")))
    ) {
      if (!is.null(hp_values[[arg]])) {
        default <- hp_values[[arg]]
      }
    }
    entry <- list(
      name = arg,
      type = .hp_type_of(default),
      default = default,
      tunable = arg %in% tunable_set
    )
    if (!is.null(choices)) {
      entry[["choices"]] <- choices
    }
    entry
  })
}


#' `algorithm.describe` handler
#'
#' Returns the hyperparameter schema for one algorithm so the client can
#' render a configuration form. The schema is built by calling
#' `setup_<Name>()` with defaults, then walking the formals of that
#' function:
#'
#' - `name`: formal argument name.
#' - `type`: inferred from the default value's R type.
#' - `default`: the default value (NULL serialises as JSON null).
#' - `tunable`: TRUE if the arg appears in the constructed
#'   `Hyperparameters` object's `@tunable_hyperparameters`.
#'
#' Wire response:
#' `{ name, description, hyperparameters: [{name, type, default, tunable}, ...] }`
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_algorithm_describe <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  name <- params[["name"]]
  if (is.null(name) || !is.character(name) || length(name) != 1L) {
    cli::cli_abort(
      "`name` is required and must be a single algorithm name.",
      class = "rtemislive_invalid_params"
    )
  }

  alg_name <- tryCatch(
    rtemis:::get_alg_name(name),
    error = function(e) {
      cli::cli_abort(
        paste0("Unknown algorithm `", name, "`."),
        class = "rtemislive_not_found"
      )
    }
  )

  setup_fn_name <- paste0("setup_", alg_name)
  setup_fn <- tryCatch(
    get(setup_fn_name, envir = asNamespace("rtemis")),
    error = function(e) {
      cli::cli_abort(
        paste0("No setup function for `", alg_name, "`."),
        class = "rtemislive_not_found"
      )
    }
  )

  # Build defaults instance for tunable-list lookup.
  hp <- tryCatch(
    setup_fn(),
    error = function(e) {
      cli::cli_abort(
        paste0("`", setup_fn_name, "()` failed: ", conditionMessage(e)),
        class = "rtemislive_internal_error"
      )
    }
  )
  tunable_set <- if (S7_inherits(hp, rtemis:::Hyperparameters)) {
    prop(hp, "tunable_hyperparameters")
  } else {
    character()
  }
  hp_values <- if (S7_inherits(hp, rtemis:::Hyperparameters)) {
    prop(hp, "hyperparameters")
  } else {
    list()
  }

  hyperparameters <- .live_build_schema(setup_fn, hp_values, tunable_set)

  alg_row <- tryCatch(
    asNamespace("rtemis")[["supervised_algorithms"]],
    error = function(e) NULL
  )
  description <- NA_character_
  if (is.data.frame(alg_row)) {
    hit <- which(alg_row[["Name"]] == alg_name)[1L]
    if (!is.na(hit)) {
      description <- as.character(alg_row[hit, "Description"])
    }
  }

  make_response(
    req_id,
    list(
      name = alg_name,
      description = description,
      hyperparameters = hyperparameters
    )
  )
} # /rtemis::handle_algorithm_describe


#' `resampler.describe` handler
#'
#' Returns the schema for `setup_Resampler()` so the client can render a
#' resampler configuration form. Same shape as `algorithm.describe`
#' but with no tunable flags — resampler parameters are fixed once
#' chosen. The `type` arg surfaces its enumerated choices via the
#' `choices` field.
#'
#' Wire response: `{ parameters: [{ name, type, default, tunable,
#' choices? }, ...] }`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_resampler_describe <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  parameters <- .live_build_schema(setup_Resampler)
  make_response(req_id, list(parameters = parameters))
} # /rtemis::handle_resampler_describe


# %% Session-level handlers --------------------------------------------------

#' `session.list` handler
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_session_list <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  make_response(req_id, list(sessions = list_sessions()))
} # /rtemis::handle_session_list


#' `session.create` handler
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_session_create <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  name <- params[["name"]] # may be NULL → auto-generated
  s <- new_session(
    name = name,
    max_sessions = server[["max_sessions"]] %||% 16L
  )
  attach_connection(s, conn[["id"]])
  conn[["session_id"]] <- s[["id"]]
  make_response(req_id, session_snapshot(s))
} # /rtemis::handle_session_create


#' `session.join` handler
#'
#' Accepts either `name` or `id`. Throws `rtemislive_session_not_found`
#' if neither resolves.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_session_join <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  s <- NULL
  if (!is.null(params[["id"]])) {
    s <- get_session_by_id(params[["id"]])
  }
  if (is.null(s) && !is.null(params[["name"]])) {
    s <- get_session_by_name(params[["name"]])
  }
  if (is.null(s)) {
    cli::cli_abort(
      "Session not found.",
      class = "rtemislive_session_not_found"
    )
  }
  attach_connection(s, conn[["id"]])
  conn[["session_id"]] <- s[["id"]]
  make_response(req_id, session_snapshot(s))
} # /rtemis::handle_session_join


#' `session.detach` handler
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_session_detach <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  s <- connection_session(conn)
  if (!is.null(s)) {
    detach_connection(s, conn[["id"]])
  }
  conn[["session_id"]] <- NULL
  make_response(req_id, list(detached = TRUE))
} # /rtemis::handle_session_detach


#' `session.rename` handler
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_session_rename <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  new_name <- params[["name"]]
  if (is.null(new_name)) {
    cli::cli_abort(
      "`name` is required.",
      class = "rtemislive_invalid_params"
    )
  }
  s <- connection_session(conn)
  if (is.null(s)) {
    cli::cli_abort(
      "Not attached to a session.",
      class = "rtemislive_not_attached"
    )
  }
  rename_session(s, new_name)
  make_response(req_id, list(session_id = s[["id"]], name = s[["name"]]))
} # /rtemis::handle_session_rename


#' `session.delete` handler
#'
#' Deletes the connection's currently attached session and detaches.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_session_delete <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  s <- connection_session(conn)
  if (is.null(s)) {
    cli::cli_abort(
      "Not attached to a session.",
      class = "rtemislive_not_attached"
    )
  }
  sid <- s[["id"]]
  delete_session(sid)
  conn[["session_id"]] <- NULL
  make_response(req_id, list(deleted = TRUE, session_id = sid))
} # /rtemis::handle_session_delete


#' `session.info` handler
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_session_info <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  s <- connection_session(conn)
  if (is.null(s)) {
    cli::cli_abort(
      "Not attached to a session.",
      class = "rtemislive_not_attached"
    )
  }
  make_response(req_id, session_snapshot(s))
} # /rtemis::handle_session_info


# %% Data handlers ----------------------------------------------------------

#' `data.upload` handler — single-frame upload
#'
#' Requires a binary payload (Arrow IPC stream) on the frame.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_data_upload <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  name <- params[["name"]]
  if (is.null(name)) {
    cli::cli_abort(
      "`name` is required.",
      class = "rtemislive_invalid_params"
    )
  }
  payload <- frame[["payload"]]
  if (is.null(payload) || !is.raw(payload) || length(payload) == 0L) {
    cli::cli_abort(
      "Arrow IPC payload is required for data.upload.",
      class = "rtemislive_invalid_params"
    )
  }
  s <- connection_session(conn)
  summary <- new_data_handle(s, name = name, bytes = payload)
  make_response(req_id, summary)
} # /rtemis::handle_data_upload


#' `data.upload.begin` handler
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_data_upload_begin <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  if (
    is.null(params[["name"]]) ||
      is.null(params[["total_bytes"]]) ||
      is.null(params[["n_chunks"]])
  ) {
    cli::cli_abort(
      "`name`, `total_bytes`, and `n_chunks` are required.",
      class = "rtemislive_invalid_params"
    )
  }
  s <- connection_session(conn)
  upload_id <- begin_upload(
    s,
    name = params[["name"]],
    total_bytes = params[["total_bytes"]],
    n_chunks = params[["n_chunks"]]
  )
  make_response(req_id, list(upload_id = upload_id))
} # /rtemis::handle_data_upload_begin


#' `data.upload.chunk` handler
#'
#' Requires the chunk bytes in the frame payload.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_data_upload_chunk <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  if (is.null(params[["upload_id"]]) || is.null(params[["chunk_index"]])) {
    cli::cli_abort(
      "`upload_id` and `chunk_index` are required.",
      class = "rtemislive_invalid_params"
    )
  }
  payload <- frame[["payload"]]
  if (is.null(payload) || !is.raw(payload)) {
    cli::cli_abort(
      "Chunk payload is required.",
      class = "rtemislive_invalid_params"
    )
  }
  s <- connection_session(conn)
  progress <- chunk_upload(
    s,
    upload_id = params[["upload_id"]],
    chunk_index = params[["chunk_index"]],
    bytes = payload
  )
  make_response(req_id, progress)
} # /rtemis::handle_data_upload_chunk


#' `data.upload.end` handler
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_data_upload_end <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  upload_id <- params[["upload_id"]]
  if (is.null(upload_id)) {
    cli::cli_abort(
      "`upload_id` is required.",
      class = "rtemislive_invalid_params"
    )
  }
  s <- connection_session(conn)
  summary <- end_upload(s, upload_id)
  make_response(req_id, summary)
} # /rtemis::handle_data_upload_end


#' `data.upload.cancel` handler
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_data_upload_cancel <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  upload_id <- params[["upload_id"]]
  if (is.null(upload_id)) {
    cli::cli_abort(
      "`upload_id` is required.",
      class = "rtemislive_invalid_params"
    )
  }
  s <- connection_session(conn)
  cancelled <- cancel_upload(s, upload_id)
  make_response(req_id, list(cancelled = cancelled))
} # /rtemis::handle_data_upload_cancel


#' `data.list` handler
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_data_list <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  s <- connection_session(conn)
  make_response(req_id, list(handles = list_data_handles(s)))
} # /rtemis::handle_data_list


#' `data.describe` handler
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_data_describe <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  handle <- params[["data_handle"]]
  if (is.null(handle)) {
    cli::cli_abort(
      "`data_handle` is required.",
      class = "rtemislive_invalid_params"
    )
  }
  s <- connection_session(conn)
  make_response(req_id, describe_data(s, handle))
} # /rtemis::handle_data_describe


#' `data.delete` handler
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_data_delete <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  handle <- params[["data_handle"]]
  if (is.null(handle)) {
    cli::cli_abort(
      "`data_handle` is required.",
      class = "rtemislive_invalid_params"
    )
  }
  s <- connection_session(conn)
  deleted <- delete_data(s, handle)
  make_response(req_id, list(deleted = deleted))
} # /rtemis::handle_data_delete


# %% Job handlers ------------------------------------------------------------

#' `train` handler
#'
#' Submits a supervised-learning job. Builds a `SuperConfigLive` from the
#' wire params (with in-memory data resolved from `data_handle`) and
#' dispatches through `train()`. This wires preprocessor / tuner /
#' resampler / execution config through end-to-end.
#'
#' Wire params (all optional except `data_handle`, `algorithm`):
#'
#' - `data_handle` — id of a previously-uploaded dataset on this session
#' - `algorithm` — character, see `algorithms` method
#' - `hyperparameters` — JSON object matching one of the `setup_*` shapes
#' - `preprocessor_config` — JSON object accepted by `setup_Preprocessor()`
#' - `tuner_config` — JSON object accepted by `rtemis::.list_to_TunerConfig()`
#' - `outer_resampling_config` — JSON object accepted by
#'   `rtemis::.list_to_ResamplerConfig()`
#' - `execution_config` — JSON object accepted by `setup_ExecutionConfig()`
#' - `weights` — character; column name in the dataset used as weights
#' - `question` — character; user-provided label for the run
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_train <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()

  data_handle <- params[["data_handle"]]
  algorithm <- params[["algorithm"]]
  if (is.null(data_handle) || is.null(algorithm)) {
    cli::cli_abort(
      "`data_handle` and `algorithm` are required.",
      class = "rtemislive_invalid_params"
    )
  }

  s <- connection_session(conn)
  data_dt <- get_data(s, data_handle)

  parse_or_abort <- function(value, builder, what) {
    if (is.null(value)) {
      return(NULL)
    }
    tryCatch(
      builder(value),
      error = function(e) {
        cli::cli_abort(
          paste0("Could not parse ", what, "."),
          parent = e,
          class = "rtemislive_invalid_params"
        )
      }
    )
  }

  # `list_to_Hyperparameters` expects `{ algorithm, hyperparameters }`,
  # but on the wire the algorithm is already a sibling param and
  # `hyperparameters` is just the flat name→value map produced by the UI.
  # Bundle them before parsing.
  hp <- if (is.null(params[["hyperparameters"]])) {
    NULL
  } else {
    parse_or_abort(
      list(
        algorithm = algorithm,
        hyperparameters = params[["hyperparameters"]]
      ),
      rtemis::.list_to_Hyperparameters,
      "hyperparameters"
    )
  }
  prp <- parse_or_abort(
    params[["preprocessor_config"]],
    function(v) do.call(setup_Preprocessor, v),
    "preprocessor_config"
  )
  tn <- parse_or_abort(
    params[["tuner_config"]],
    rtemis::.list_to_TunerConfig,
    "tuner_config"
  )
  # UI form is built from `setup_Resampler()` formals (n_resamples, type,
  # train_p, …), so dispatch through `setup_Resampler()` rather than
  # `list_to_ResamplerConfig()` which expects the post-construction
  # property names (`n`, …).
  rs <- parse_or_abort(
    params[["outer_resampling_config"]],
    function(v) do.call(setup_Resampler, v),
    "outer_resampling_config"
  )
  ec <- parse_or_abort(
    params[["execution_config"]],
    function(v) do.call(setup_ExecutionConfig, v),
    "execution_config"
  )
  if (is.null(ec)) {
    ec <- setup_ExecutionConfig()
  }

  cfg <- setup_SuperConfigLive(
    dat_training = data_dt,
    weights = params[["weights"]],
    preprocessor_config = prp,
    algorithm = algorithm,
    hyperparameters = hp,
    tuner_config = tn,
    outer_resampling_config = rs,
    execution_config = ec,
    question = params[["question"]],
    outdir = NULL,
    verbosity = 1L
  )

  job <- submit_job(
    session = s,
    type = "train",
    params = params,
    expr = quote(rtemis::train(cfg)),
    env = list(cfg = cfg),
    max_concurrent = server[["max_concurrent"]] %||% 8L
  )

  make_response(req_id, list(job_id = job[["id"]]))
} # /rtemis::handle_train


#' `job.list` handler
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_job_list <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  s <- connection_session(conn)
  make_response(req_id, list(jobs = list_jobs(s)))
} # /rtemis::handle_job_list


#' `job.status` handler
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_job_status <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  job_id <- params[["job_id"]]
  if (is.null(job_id)) {
    cli::cli_abort(
      "`job_id` is required.",
      class = "rtemislive_invalid_params"
    )
  }
  s <- connection_session(conn)
  job <- get_job(s, job_id)
  if (is.null(job)) {
    cli::cli_abort(
      "Unknown job_id.",
      class = "rtemislive_not_found"
    )
  }
  make_response(req_id, job_summary(job))
} # /rtemis::handle_job_status


#' `job.cancel` handler
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_job_cancel <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  job_id <- params[["job_id"]]
  if (is.null(job_id)) {
    cli::cli_abort(
      "`job_id` is required.",
      class = "rtemislive_invalid_params"
    )
  }
  s <- connection_session(conn)
  cancelled <- cancel_job(s, job_id)
  make_response(req_id, list(cancelled = cancelled))
} # /rtemis::handle_job_cancel


#' `job.result` handler
#'
#' Slices (spec §6.5):
#'
#' - `summary`: lightweight JSON envelope — `to_json(result)` with the
#'   heavy tabular fields (`varimp`, `varimp_per_resample`, per-resample
#'   `res_metrics`) stripped. Use the dedicated slices below for those.
#' - `raw`: full `to_json(result)` JSON, no stripping (debug / escape
#'   hatch — may be very large for resampled fits or wide varimp).
#' - `varimp`: small JSON pointer (`{rows, cols, columns}`) + Arrow IPC
#'   payload of the variable-importance table.
#' - `predictions`: small JSON pointer + Arrow IPC of the long-format
#'   predictions table.
#' - `metrics`: structured JSON for `metrics_training` /
#'   `metrics_validation` / `metrics_test`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_job_result <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  job_id <- params[["job_id"]]
  if (is.null(job_id)) {
    cli::cli_abort(
      "`job_id` is required.",
      class = "rtemislive_invalid_params"
    )
  }
  slice <- params[["slice"]] %||% "summary"

  s <- connection_session(conn)
  job <- get_job(s, job_id)
  if (is.null(job)) {
    cli::cli_abort(
      "Unknown job_id.",
      class = "rtemislive_not_found"
    )
  }

  if (!identical(job[["status"]], "complete")) {
    cli::cli_abort(
      paste0(
        "Job status is `",
        job[["status"]],
        "`; no result available."
      ),
      class = "rtemislive_invalid_params"
    )
  }

  result <- job[["result"]]
  if (slice == "summary") {
    return(make_response(req_id, summary_json(result)))
  }
  if (slice == "raw") {
    return(make_response(req_id, to_json(result)))
  }
  if (slice == "varimp") {
    vi_dt <- varimp_table(result)
    if (is.null(vi_dt) || NROW(vi_dt) == 0L) {
      # No varimp available for this algorithm: return an empty pointer
      # (no payload) so the client can disambiguate from a transport error.
      return(make_response(
        req_id,
        list(rows = 0L, cols = 0L, columns = list(), format = "arrow-ipc")
      ))
    }
    payload <- encode_arrow_ipc(vi_dt)
    return(make_response_payload(
      req_id,
      list(
        rows = NROW(vi_dt),
        cols = NCOL(vi_dt),
        columns = names(vi_dt),
        format = "arrow-ipc"
      ),
      payload
    ))
  }
  if (slice == "predictions") {
    pred_dt <- predictions_table(result)
    payload <- encode_arrow_ipc(pred_dt)
    return(make_response_payload(
      req_id,
      list(
        rows = NROW(pred_dt),
        cols = NCOL(pred_dt),
        columns = names(pred_dt),
        format = "arrow-ipc"
      ),
      payload
    ))
  }
  if (slice == "metrics") {
    if (!S7_inherits(result, rtemis:::Supervised)) {
      cli::cli_abort(
        "`metrics` slice requires a `Supervised` result.",
        class = "rtemislive_invalid_params"
      )
    }
    out <- list(
      training = to_json(prop(result, "metrics_training"))
    )
    mv <- prop(result, "metrics_validation")
    if (!is.null(mv)) {
      out[["validation"]] <- to_json(mv)
    }
    mt <- prop(result, "metrics_test")
    if (!is.null(mt)) {
      out[["test"]] <- to_json(mt)
    }
    return(make_response(req_id, out))
  }
  cli::cli_abort(
    paste0(
      "Unsupported slice `",
      slice,
      "`. Use `summary`, `raw`, `varimp`, `predictions`, or `metrics`."
    ),
    class = "rtemislive_invalid_params"
  )
} # /rtemis::handle_job_result


#' `job.delete` handler
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_job_delete <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  job_id <- params[["job_id"]]
  if (is.null(job_id)) {
    cli::cli_abort(
      "`job_id` is required.",
      class = "rtemislive_invalid_params"
    )
  }
  s <- connection_session(conn)
  deleted <- delete_job(s, job_id)
  make_response(req_id, list(deleted = deleted))
} # /rtemis::handle_job_delete


# %% Method table ------------------------------------------------------------

# Entries: list(handler = fn, requires = character[])
# Requirements: "authed" (auth must have succeeded), "attached" (a
# session is currently attached), "unattached" (no session attached).
.METHOD_TABLE <- list(
  "auth" = list(
    handler = handle_auth,
    requires = character(0L)
  ),
  "ping" = list(
    handler = handle_ping,
    requires = "authed"
  ),
  "info" = list(
    handler = handle_info,
    requires = "authed"
  ),
  "algorithms" = list(
    handler = handle_algorithms,
    requires = "authed"
  ),
  "algorithm.describe" = list(
    handler = handle_algorithm_describe,
    requires = "authed"
  ),
  "resampler.describe" = list(
    handler = handle_resampler_describe,
    requires = "authed"
  ),
  "session.list" = list(
    handler = handle_session_list,
    requires = "authed"
  ),
  "session.create" = list(
    handler = handle_session_create,
    requires = c("authed", "unattached")
  ),
  "session.join" = list(
    handler = handle_session_join,
    requires = c("authed", "unattached")
  ),
  "session.detach" = list(
    handler = handle_session_detach,
    requires = c("authed", "attached")
  ),
  "session.rename" = list(
    handler = handle_session_rename,
    requires = c("authed", "attached")
  ),
  "session.delete" = list(
    handler = handle_session_delete,
    requires = c("authed", "attached")
  ),
  "session.info" = list(
    handler = handle_session_info,
    requires = c("authed", "attached")
  ),
  "data.upload" = list(
    handler = handle_data_upload,
    requires = c("authed", "attached")
  ),
  "data.upload.begin" = list(
    handler = handle_data_upload_begin,
    requires = c("authed", "attached")
  ),
  "data.upload.chunk" = list(
    handler = handle_data_upload_chunk,
    requires = c("authed", "attached")
  ),
  "data.upload.end" = list(
    handler = handle_data_upload_end,
    requires = c("authed", "attached")
  ),
  "data.upload.cancel" = list(
    handler = handle_data_upload_cancel,
    requires = c("authed", "attached")
  ),
  "data.list" = list(
    handler = handle_data_list,
    requires = c("authed", "attached")
  ),
  "data.describe" = list(
    handler = handle_data_describe,
    requires = c("authed", "attached")
  ),
  "data.delete" = list(
    handler = handle_data_delete,
    requires = c("authed", "attached")
  ),
  "train" = list(
    handler = handle_train,
    requires = c("authed", "attached")
  ),
  "job.list" = list(
    handler = handle_job_list,
    requires = c("authed", "attached")
  ),
  "job.status" = list(
    handler = handle_job_status,
    requires = c("authed", "attached")
  ),
  "job.cancel" = list(
    handler = handle_job_cancel,
    requires = c("authed", "attached")
  ),
  "job.result" = list(
    handler = handle_job_result,
    requires = c("authed", "attached")
  ),
  "job.delete" = list(
    handler = handle_job_delete,
    requires = c("authed", "attached")
  )
)
