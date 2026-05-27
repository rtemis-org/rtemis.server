# 2026- EDG rtemis.org

# Method dispatch table and request handlers for rtemislive. See
# specs/rtemislive.md paragraph 5 (sessions) and paragraph 6 (methods).
#
# This file contains:
#
# - Connection state (`new_connection` and helpers)
# - The dispatcher (`dispatch_request`) - maps wire `method` to handler,
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
#' @return Character scalar - `conn-<hex16>`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
new_connection_id <- function() {
  rtemis.core::check_dependencies("uuid")
  hex <- gsub("-", "", uuid::UUIDgenerate(use.time = TRUE), fixed = TRUE)
  paste0("conn-", substr(hex, 1L, 16L))
}


#' Create a new connection state object
#'
#' Connections are plain envs (mutable state). `send_raw` is an injectable
#' closure the host event loop installs to deliver outbound frames on the
#' underlying nanonext stream. In tests it can be `NULL` (handlers don't
#' use it directly - they return the response envelope to the caller).
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
}


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
}


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
}


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
  # Mutable loop state - initialised by the loop, not the dispatcher.
  e[["connections"]] <- new.env(parent = emptyenv()) # conn_id -> conn env
  e[["ws_listener"]] <- NULL
  e[["progress_sock"]] <- NULL
  e[["last_heartbeat"]] <- started_at
  e[["last_gc"]] <- started_at
  e[["stop_requested"]] <- FALSE
  e
}


#' Dispatch a single request frame
#'
#' Looks up the method in the dispatch table, checks the connection's
#' auth/attachment requirements, calls the handler. Classed errors
#' thrown by handlers (or by the lower modules they call) are caught
#' and translated into wire error envelopes per spec paragraph 15.
#'
#' @param conn Connection env.
#' @param frame Named list with `header` (decoded JSON) and `payload`
#'   (raw vector or NULL). Output of `decode_frame()`.
#' @param server Server state from `new_server_state()`.
#'
#' @return Named list - the response envelope, ready for `encode_frame()`.
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
}


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
    rtemis.core::warn(
      "Auth failed for ",
      conn[["id"]],
      " (attempt ",
      conn[["auth_attempts"]],
      "/3).",
      package = "rtemis.server"
    )
    if (conn[["auth_attempts"]] >= 3L) {
      conn[["close_after_response"]] <- TRUE
    }
    return(make_error(req_id, "unauthorized", "Invalid token."))
  }
  conn[["authed"]] <- TRUE
  conn[["auth_attempts"]] <- 0L
  rtemis.core::info(
    "Auth ok for ",
    conn[["id"]],
    ".",
    package = "rtemis.server"
  )
  make_response(req_id, list(connection_id = conn[["id"]]))
}


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
}


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
  rtemis_server_v <- tryCatch(
    as.character(utils::packageVersion("rtemis.server")),
    error = function(e) NA_character_
  )
  uptime <- as.numeric(
    difftime(Sys.time(), server[["started_at"]], units = "secs")
  )
  make_response(
    req_id,
    list(
      server = "rtemislive",
      rtemis_server_version = rtemis_server_v,
      rtemis_version = rtemis_v,
      r_version = R.version.string,
      daemons = daemons,
      max_concurrent = server[["max_concurrent"]] %||% 8L,
      uptime_seconds = uptime,
      n_sessions = length(ls(session_registry())),
      n_jobs_running = count_active_jobs()
    )
  )
}


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
}


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
}


#' `decomp.algorithms` handler
#'
#' Returns the catalogue of decomposition algorithms. Each entry:
#' `{ name, description }`. Per-algorithm config schemas are fetched
#' separately via `handle_decomp_algorithm_describe()`
#' (`decomp.algorithm.describe`).
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_decomp_algorithms <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  tbl <- asNamespace("rtemis")[["decom_algorithms"]]
  if (!is.data.frame(tbl)) {
    return(make_response(req_id, list(algorithms = list())))
  }
  algorithms <- lapply(seq_len(nrow(tbl)), function(i) {
    list(
      name = as.character(tbl[i, 1L]),
      description = as.character(tbl[i, 2L])
    )
  })
  make_response(req_id, list(algorithms = algorithms))
}


# Type-name from a default value. NULL becomes "null" (which the UI
# renders as a free-text input). Length is ignored - a `c(...)` default
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
# - choices = c("a", "b", ...) -> enum with the first value as default
# - any other multi-element default -> flattened to its first element
# - single-value default (or NULL) -> reported as-is
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
    # Enumerated choices: `c("a", "b", ...)` - keep choices, default is first.
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
    rtemis.core::abort(
      "`name` is required and must be a single algorithm name.",
      class = "rtemislive_invalid_params"
    )
  }

  alg_name <- tryCatch(
    get_alg_name(name),
    error = function(e) {
      rtemis.core::abort(
        paste0("Unknown algorithm `", name, "`."),
        class = "rtemislive_not_found"
      )
    }
  )

  setup_fn_name <- paste0("setup_", alg_name)
  setup_fn <- tryCatch(
    get(setup_fn_name, envir = asNamespace("rtemis")),
    error = function(e) {
      rtemis.core::abort(
        paste0("No setup function for `", alg_name, "`."),
        class = "rtemislive_not_found"
      )
    }
  )

  # Build defaults instance for tunable-list lookup.
  hp <- tryCatch(
    setup_fn(),
    error = function(e) {
      rtemis.core::abort(
        paste0("`", setup_fn_name, "()` failed: ", conditionMessage(e)),
        class = "rtemislive_internal_error"
      )
    }
  )
  tunable_set <- if (inherits(hp, "rtemis::Hyperparameters")) {
    prop(hp, "tunable_hyperparameters")
  } else {
    character()
  }
  hp_values <- if (inherits(hp, "rtemis::Hyperparameters")) {
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
}


#' `decomp.algorithm.describe` handler
#'
#' Returns the config schema for one decomposition algorithm so the
#' client can render a configuration form. Schema is built by calling
#' `setup_<Name>()` with defaults and walking its formals via
#' `.live_build_schema()`.
#'
#' Decomposition configs have no tunable concept (no `Hyperparameters`
#' S7 class), so `tunable_set = character()`. Default fallbacks are
#' pulled from the constructed `<Algo>DecompositionConfig`'s `config`
#' list (the S7 prop holding the resolved values).
#'
#' Wire response:
#' `{ name, description, hyperparameters: [{name, type, default, tunable}, ...] }`
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_decomp_algorithm_describe <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  name <- params[["name"]]
  if (is.null(name) || !is.character(name) || length(name) != 1L) {
    rtemis.core::abort(
      "`name` is required and must be a single algorithm name.",
      class = "rtemislive_invalid_params"
    )
  }

  alg_name <- tryCatch(
    get_decom_name(name),
    error = function(e) {
      rtemis.core::abort(
        paste0("Unknown decomposition algorithm `", name, "`."),
        class = "rtemislive_not_found"
      )
    }
  )

  setup_fn_name <- paste0("setup_", alg_name)
  setup_fn <- tryCatch(
    get(setup_fn_name, envir = asNamespace("rtemis")),
    error = function(e) {
      rtemis.core::abort(
        paste0("No setup function for `", alg_name, "`."),
        class = "rtemislive_not_found"
      )
    }
  )

  cfg <- tryCatch(
    setup_fn(),
    error = function(e) {
      rtemis.core::abort(
        paste0("`", setup_fn_name, "()` failed: ", conditionMessage(e)),
        class = "rtemislive_internal_error"
      )
    }
  )
  cfg_values <- if (inherits(cfg, "rtemis::DecompositionConfig")) {
    prop(cfg, "config")
  } else {
    list()
  }

  hyperparameters <- .live_build_schema(
    setup_fn,
    cfg_values,
    tunable_set = character()
  )

  alg_tbl <- tryCatch(
    asNamespace("rtemis")[["decom_algorithms"]],
    error = function(e) NULL
  )
  description <- NA_character_
  if (is.data.frame(alg_tbl)) {
    hit <- which(alg_tbl[, 1L] == alg_name)[1L]
    if (!is.na(hit)) {
      description <- as.character(alg_tbl[hit, 2L])
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
}


#' `cluster.algorithms` handler
#'
#' Returns the catalogue of clustering algorithms. Each entry:
#' `{ name, description }`. Per-algorithm config schemas are fetched
#' separately via `handle_cluster_algorithm_describe()`
#' (`cluster.algorithm.describe`). Parallel to
#' `handle_decomp_algorithms()`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_cluster_algorithms <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  tbl <- asNamespace("rtemis")[["clust_algorithms"]]
  if (!is.data.frame(tbl)) {
    return(make_response(req_id, list(algorithms = list())))
  }
  algorithms <- lapply(seq_len(nrow(tbl)), function(i) {
    list(
      name = as.character(tbl[i, 1L]),
      description = as.character(tbl[i, 2L])
    )
  })
  make_response(req_id, list(algorithms = algorithms))
}


#' `cluster.algorithm.describe` handler
#'
#' Returns the config schema for one clustering algorithm so the
#' client can render a configuration form. Schema is built by calling
#' `setup_<Name>()` with defaults and walking its formals via
#' `.live_build_schema()`.
#'
#' Clustering configs have no tunable concept, so
#' `tunable_set = character()`. Default fallbacks are pulled from the
#' constructed `<Algo>ClusteringConfig`'s `config` list. Parallel to
#' `handle_decomp_algorithm_describe()`.
#'
#' Wire response:
#' `{ name, description, hyperparameters: [{name, type, default, tunable}, ...] }`
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_cluster_algorithm_describe <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  name <- params[["name"]]
  if (is.null(name) || !is.character(name) || length(name) != 1L) {
    rtemis.core::abort(
      "`name` is required and must be a single algorithm name.",
      class = "rtemislive_invalid_params"
    )
  }

  alg_name <- tryCatch(
    asNamespace("rtemis")[["get_clust_name"]](name),
    error = function(e) {
      rtemis.core::abort(
        paste0("Unknown clustering algorithm `", name, "`."),
        class = "rtemislive_not_found"
      )
    }
  )

  setup_fn_name <- paste0("setup_", alg_name)
  setup_fn <- tryCatch(
    get(setup_fn_name, envir = asNamespace("rtemis")),
    error = function(e) {
      rtemis.core::abort(
        paste0("No setup function for `", alg_name, "`."),
        class = "rtemislive_not_found"
      )
    }
  )

  cfg <- tryCatch(
    setup_fn(),
    error = function(e) {
      rtemis.core::abort(
        paste0("`", setup_fn_name, "()` failed: ", conditionMessage(e)),
        class = "rtemislive_internal_error"
      )
    }
  )
  cfg_values <- if (inherits(cfg, "rtemis::ClusteringConfig")) {
    prop(cfg, "config")
  } else {
    list()
  }

  hyperparameters <- .live_build_schema(
    setup_fn,
    cfg_values,
    tunable_set = character()
  )

  alg_tbl <- tryCatch(
    asNamespace("rtemis")[["clust_algorithms"]],
    error = function(e) NULL
  )
  description <- NA_character_
  if (is.data.frame(alg_tbl)) {
    hit <- which(alg_tbl[, 1L] == alg_name)[1L]
    if (!is.na(hit)) {
      description <- as.character(alg_tbl[hit, 2L])
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
}


#' `resampler.describe` handler
#'
#' Returns the schema for `setup_Resampler()` so the client can render a
#' resampler configuration form. Same shape as `algorithm.describe`
#' but with no tunable flags - resampler parameters are fixed once
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
}


# %% Session-level handlers --------------------------------------------------

#' `session.list` handler
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_session_list <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  make_response(req_id, list(sessions = list_sessions()))
}


#' `session.create` handler
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_session_create <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  name <- params[["name"]] # may be NULL -> auto-generated
  s <- new_session(
    name = name,
    max_sessions = server[["max_sessions"]] %||% 16L
  )
  attach_connection(s, conn[["id"]])
  conn[["session_id"]] <- s[["id"]]
  make_response(req_id, session_snapshot(s))
}


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
    rtemis.core::abort(
      "Session not found.",
      class = "rtemislive_session_not_found"
    )
  }
  attach_connection(s, conn[["id"]])
  conn[["session_id"]] <- s[["id"]]
  make_response(req_id, session_snapshot(s))
}


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
}


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
    rtemis.core::abort(
      "`name` is required.",
      class = "rtemislive_invalid_params"
    )
  }
  s <- connection_session(conn)
  if (is.null(s)) {
    rtemis.core::abort(
      "Not attached to a session.",
      class = "rtemislive_not_attached"
    )
  }
  rename_session(s, new_name)
  make_response(req_id, list(session_id = s[["id"]], name = s[["name"]]))
}


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
    rtemis.core::abort(
      "Not attached to a session.",
      class = "rtemislive_not_attached"
    )
  }
  sid <- s[["id"]]
  delete_session(sid)
  conn[["session_id"]] <- NULL
  make_response(req_id, list(deleted = TRUE, session_id = sid))
}


#' `session.info` handler
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_session_info <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  s <- connection_session(conn)
  if (is.null(s)) {
    rtemis.core::abort(
      "Not attached to a session.",
      class = "rtemislive_not_attached"
    )
  }
  make_response(req_id, session_snapshot(s))
}


# %% Data handlers ----------------------------------------------------------

#' `data.upload` handler - single-frame upload
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
    rtemis.core::abort(
      "`name` is required.",
      class = "rtemislive_invalid_params"
    )
  }
  payload <- frame[["payload"]]
  if (is.null(payload) || !is.raw(payload) || length(payload) == 0L) {
    rtemis.core::abort(
      "Arrow IPC payload is required for data.upload.",
      class = "rtemislive_invalid_params"
    )
  }
  s <- connection_session(conn)
  summary <- new_data_handle(s, name = name, bytes = payload)
  make_response(req_id, summary)
}


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
    rtemis.core::abort(
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
}


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
    rtemis.core::abort(
      "`upload_id` and `chunk_index` are required.",
      class = "rtemislive_invalid_params"
    )
  }
  payload <- frame[["payload"]]
  if (is.null(payload) || !is.raw(payload)) {
    rtemis.core::abort(
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
}


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
    rtemis.core::abort(
      "`upload_id` is required.",
      class = "rtemislive_invalid_params"
    )
  }
  s <- connection_session(conn)
  summary <- end_upload(s, upload_id)
  make_response(req_id, summary)
}


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
    rtemis.core::abort(
      "`upload_id` is required.",
      class = "rtemislive_invalid_params"
    )
  }
  s <- connection_session(conn)
  cancelled <- cancel_upload(s, upload_id)
  make_response(req_id, list(cancelled = cancelled))
}


#' `data.list` handler
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_data_list <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  s <- connection_session(conn)
  make_response(req_id, list(handles = list_data_handles(s)))
}


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
    rtemis.core::abort(
      "`data_handle` is required.",
      class = "rtemislive_invalid_params"
    )
  }
  s <- connection_session(conn)
  make_response(req_id, describe_data(s, handle))
}


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
    rtemis.core::abort(
      "`data_handle` is required.",
      class = "rtemislive_invalid_params"
    )
  }
  s <- connection_session(conn)
  deleted <- delete_data(s, handle)
  make_response(req_id, list(deleted = deleted))
}


# %% Job handlers ------------------------------------------------------------

#' Collapse list-of-scalars values to atomic vectors
#'
#' Frame-level JSON decode uses `simplifyVector = FALSE` (heterogeneous
#' payloads survive intact), so a JSON array like `[100, 500, 1000]`
#' arrives as an R *list* of length-1 atomics, not a numeric vector.
#' rtemis's `setup_<Algorithm>()` validators and downstream tuner
#' (which branches on `length(x) > 1`) need atomic vectors.
#'
#' Walks the named list and, for every value that is a non-empty list
#' whose elements are all length-1 atomics, replaces it with the
#' corresponding atomic vector (`unlist`, no names). One-element lists
#' (e.g. user typed a single value with Tune toggled on) collapse to a
#' scalar; multi-element lists become a proper vector that drives the
#' tuner. Values that aren't list-of-scalars are returned unchanged -
#' notably nested lists like `inbag` survive verbatim.
#'
#' Module-scope (not nested inside `handle_train`) so tests can verify
#' the wire-shape collapse directly without standing up a full handler.
#'
#' @param hp Named list as decoded from the wire `hyperparameters`
#'   payload.
#'
#' @return Named list with the same keys; values either left as-is or
#'   unlisted from scalar-list to atomic vector.
#'
#' @author EDG
#' @keywords internal
#' @noRd
.collapse_scalar_lists <- function(hp) {
  if (!is.list(hp)) {
    return(hp)
  }
  lapply(hp, function(v) {
    if (
      is.list(v) &&
        length(v) > 0L &&
        all(vapply(
          v,
          function(x) is.atomic(x) && length(x) == 1L,
          logical(1)
        ))
    ) {
      unlist(v, use.names = FALSE)
    } else {
      v
    }
  })
}


#' `train` handler
#'
#' Submits a supervised-learning job. Builds a `SuperConfigLive` from the
#' wire params (with in-memory data resolved from `data_handle`) and
#' dispatches through `train()`. This wires preprocessor / tuner /
#' resampler / execution config through end-to-end.
#'
#' Wire params (all optional except `data_handle`, `algorithm`):
#'
#' - `data_handle` - id of a previously-uploaded dataset on this session
#' - `algorithm` - character, see `algorithms` method
#' - `hyperparameters` - JSON object matching one of the `setup_*` shapes
#' - `preprocessor_config` - JSON object accepted by `setup_Preprocessor()`
#' - `tuner_config` - JSON object accepted by `rtemis::.list_to_TunerConfig()`
#' - `outer_resampling_config` - JSON object accepted by
#'   `rtemis::.list_to_ResamplerConfig()`
#' - `execution_config` - JSON object accepted by `setup_ExecutionConfig()`
#' - `weights` - character; column name in the dataset used as weights
#' - `question` - character; user-provided label for the run
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
    rtemis.core::abort(
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
        # Include the parent's message in the wire text so the browser
        # surfaces the specific reason (e.g. which hyperparameter failed
        # to parse). The condition object still carries `parent = e` for
        # programmatic handlers.
        rtemis.core::abort(
          "Could not parse ",
          what,
          ": ",
          conditionMessage(e),
          parent = e,
          class = "rtemislive_invalid_params"
        )
      }
    )
  }

  # `list_to_Hyperparameters` expects `{ algorithm, hyperparameters }`,
  # but on the wire the algorithm is already a sibling param and
  # `hyperparameters` is just the flat name->value map produced by the UI.
  # Bundle them before parsing.
  hp <- if (is.null(params[["hyperparameters"]])) {
    NULL
  } else {
    parse_or_abort(
      list(
        algorithm = algorithm,
        hyperparameters = .collapse_scalar_lists(params[["hyperparameters"]])
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
  # train_p, ...), so dispatch through `setup_Resampler()` rather than
  # `list_to_ResamplerConfig()` which expects the post-construction
  # property names (`n`, ...).
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

  # `progress` callback: `forward_progress` calls rtemis's internal
  # `msg()` with `caller = stage`, so the daemon-side msg sink (set up
  # by `init_daemon_progress`) ships an envelope with the structured
  # stage name. The wire arrives at the client as
  # `{stage: "outer_fold", message: "Outer fold 2/5", ...}`. Referencing
  # `rtemis.server::forward_progress` in the quoted expression makes
  # mirai load rtemis.server on the daemon, which runs `.onLoad` once
  # and caches the `msg` lookup - no per-call namespace work.
  job <- submit_job(
    session = s,
    type = "train",
    params = params,
    expr = quote(
      rtemis::train(cfg, progress = rtemis.server::forward_progress)
    ),
    env = list(cfg = cfg),
    max_concurrent = server[["max_concurrent"]] %||% 8L
  )

  resp <- list(job_id = job[["id"]], status = job[["status"]])
  if (identical(job[["status"]], "queued")) {
    resp[["queue_position"]] <- job_queue_position(job)
  }
  make_response(req_id, resp)
}


#' `decomp` handler
#'
#' Submits an unsupervised decomposition job. Builds a
#' `<Algo>DecompositionConfig` from the wire params, optionally subsets
#' the dataset to a feature list, and dispatches through
#' `rtemis::decomp()`.
#'
#' Wire params (all optional except `data_handle`, `algorithm`):
#'
#' - `data_handle` - id of a previously-uploaded dataset on this session
#' - `algorithm` - character, one of `decomp.algorithms`
#' - `hyperparameters` - JSON object accepted by `setup_<Algo>()`
#' - `features` - character[]; subset of columns to decompose. When
#'   omitted, all columns are used.
#' - `question` - character; user-provided label for the run
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_decomp <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()

  data_handle <- params[["data_handle"]]
  algorithm <- params[["algorithm"]]
  if (is.null(data_handle) || is.null(algorithm)) {
    rtemis.core::abort(
      "`data_handle` and `algorithm` are required.",
      class = "rtemislive_invalid_params"
    )
  }

  alg_name <- tryCatch(
    get_decom_name(algorithm),
    error = function(e) {
      rtemis.core::abort(
        paste0("Unknown decomposition algorithm `", algorithm, "`."),
        class = "rtemislive_not_found"
      )
    }
  )

  s <- connection_session(conn)
  data_dt <- get_data(s, data_handle)

  features <- params[["features"]]
  if (!is.null(features)) {
    features <- unlist(features, use.names = FALSE)
    if (
      !is.character(features) ||
        length(features) == 0L ||
        any(!nzchar(features))
    ) {
      rtemis.core::abort(
        "`features` must be a non-empty character vector.",
        class = "rtemislive_invalid_params"
      )
    }
    missing_cols <- setdiff(features, colnames(data_dt))
    if (length(missing_cols) > 0L) {
      rtemis.core::abort(
        paste0(
          "Features not in dataset: ",
          paste(missing_cols, collapse = ", "),
          "."
        ),
        class = "rtemislive_invalid_params"
      )
    }
    x <- data_dt[, features, with = FALSE]
  } else {
    x <- data_dt
  }

  # Build `<Algo>DecompositionConfig` via `setup_<Algo>(...)`. The wire
  # `hyperparameters` payload is a flat name->value map matching the
  # setup function's formals; scalar JSON arrays are collapsed before
  # `do.call` so atomic args arrive as scalars rather than length-1
  # lists.
  setup_fn_name <- paste0("setup_", alg_name)
  setup_fn <- tryCatch(
    get(setup_fn_name, envir = asNamespace("rtemis")),
    error = function(e) {
      rtemis.core::abort(
        paste0("No setup function for `", alg_name, "`."),
        class = "rtemislive_internal_error"
      )
    }
  )
  hp <- if (is.null(params[["hyperparameters"]])) {
    list()
  } else {
    .collapse_scalar_lists(params[["hyperparameters"]])
  }
  cfg <- tryCatch(
    do.call(setup_fn, as.list(hp)),
    error = function(e) {
      rtemis.core::abort(
        "Could not build decomposition config: ",
        conditionMessage(e),
        parent = e,
        class = "rtemislive_invalid_params"
      )
    }
  )

  # No `progress` callback: rtemis::decomp() has no fold-boundary
  # checkpoints. The daemon-side msg sink (set up by
  # `init_daemon_progress`) still ships every internal `msg()` call from
  # `decomp()` (data summary, "Decomposing with PCA...", outro) as a
  # progress envelope, so the browser gets inline status without any
  # per-handler wiring.
  job <- submit_job(
    session = s,
    type = "decomp",
    params = params,
    expr = quote(
      rtemis::decomp(x, algorithm = alg_name, config = cfg, verbosity = 1L)
    ),
    env = list(x = x, alg_name = alg_name, cfg = cfg),
    max_concurrent = server[["max_concurrent"]] %||% 8L
  )

  resp <- list(job_id = job[["id"]], status = job[["status"]])
  if (identical(job[["status"]], "queued")) {
    resp[["queue_position"]] <- job_queue_position(job)
  }
  make_response(req_id, resp)
}


#' `cluster` handler
#'
#' Submits an unsupervised clustering job. Builds a
#' `<Algo>ClusteringConfig` from the wire params, optionally subsets
#' the dataset to a feature list, and dispatches through
#' `rtemis::cluster()`. Parallel to `handle_decomp()`.
#'
#' Wire params (all optional except `data_handle`, `algorithm`):
#'
#' - `data_handle` - id of a previously-uploaded dataset on this session
#' - `algorithm` - character, one of `cluster.algorithms`
#' - `hyperparameters` - JSON object accepted by `setup_<Algo>()`
#' - `features` - character[]; subset of columns to cluster on. When
#'   omitted, all columns are used.
#' - `question` - character; user-provided label for the run
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_cluster <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()

  data_handle <- params[["data_handle"]]
  algorithm <- params[["algorithm"]]
  if (is.null(data_handle) || is.null(algorithm)) {
    rtemis.core::abort(
      "`data_handle` and `algorithm` are required.",
      class = "rtemislive_invalid_params"
    )
  }

  alg_name <- tryCatch(
    asNamespace("rtemis")[["get_clust_name"]](algorithm),
    error = function(e) {
      rtemis.core::abort(
        paste0("Unknown clustering algorithm `", algorithm, "`."),
        class = "rtemislive_not_found"
      )
    }
  )

  s <- connection_session(conn)
  data_dt <- get_data(s, data_handle)

  features <- params[["features"]]
  if (!is.null(features)) {
    features <- unlist(features, use.names = FALSE)
    if (
      !is.character(features) ||
        length(features) == 0L ||
        any(!nzchar(features))
    ) {
      rtemis.core::abort(
        "`features` must be a non-empty character vector.",
        class = "rtemislive_invalid_params"
      )
    }
    missing_cols <- setdiff(features, colnames(data_dt))
    if (length(missing_cols) > 0L) {
      rtemis.core::abort(
        paste0(
          "Features not in dataset: ",
          paste(missing_cols, collapse = ", "),
          "."
        ),
        class = "rtemislive_invalid_params"
      )
    }
    x <- data_dt[, features, with = FALSE]
  } else {
    x <- data_dt
  }

  setup_fn_name <- paste0("setup_", alg_name)
  setup_fn <- tryCatch(
    get(setup_fn_name, envir = asNamespace("rtemis")),
    error = function(e) {
      rtemis.core::abort(
        paste0("No setup function for `", alg_name, "`."),
        class = "rtemislive_internal_error"
      )
    }
  )
  hp <- if (is.null(params[["hyperparameters"]])) {
    list()
  } else {
    .collapse_scalar_lists(params[["hyperparameters"]])
  }
  cfg <- tryCatch(
    do.call(setup_fn, as.list(hp)),
    error = function(e) {
      rtemis.core::abort(
        "Could not build clustering config: ",
        conditionMessage(e),
        parent = e,
        class = "rtemislive_invalid_params"
      )
    }
  )

  job <- submit_job(
    session = s,
    type = "cluster",
    params = params,
    expr = quote(
      rtemis::cluster(x, algorithm = alg_name, config = cfg, verbosity = 1L)
    ),
    env = list(x = x, alg_name = alg_name, cfg = cfg),
    max_concurrent = server[["max_concurrent"]] %||% 8L
  )

  resp <- list(job_id = job[["id"]], status = job[["status"]])
  if (identical(job[["status"]], "queued")) {
    resp[["queue_position"]] <- job_queue_position(job)
  }
  make_response(req_id, resp)
}


#' `job.list` handler
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_job_list <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  s <- connection_session(conn)
  make_response(req_id, list(jobs = list_jobs(s)))
}


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
    rtemis.core::abort(
      "`job_id` is required.",
      class = "rtemislive_invalid_params"
    )
  }
  s <- connection_session(conn)
  job <- get_job(s, job_id)
  if (is.null(job)) {
    rtemis.core::abort(
      "Unknown job_id '",
      job_id,
      "'.",
      class = "rtemislive_not_found"
    )
  }
  make_response(req_id, job_summary(job))
}


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
    rtemis.core::abort(
      "`job_id` is required.",
      class = "rtemislive_invalid_params"
    )
  }
  s <- connection_session(conn)
  cancelled <- cancel_job(s, job_id)
  make_response(req_id, list(cancelled = cancelled))
}


#' `job.result` handler
#'
#' Slices (spec paragraph 6.5):
#'
#' - `summary`: lightweight JSON envelope - `to_json(result)` with the
#'   heavy tabular fields (`varimp`, `varimp_per_resample`, per-resample
#'   `res_metrics`) stripped. Use the dedicated slices below for those.
#' - `raw`: full `to_json(result)` JSON, no stripping (debug / escape
#'   hatch - may be very large for resampled fits or wide varimp).
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
    rtemis.core::abort(
      "`job_id` is required.",
      class = "rtemislive_invalid_params"
    )
  }
  slice <- params[["slice"]] %||% "summary"

  s <- connection_session(conn)
  job <- get_job(s, job_id)
  if (is.null(job)) {
    rtemis.core::abort(
      "Unknown job_id '",
      job_id,
      "'.",
      class = "rtemislive_not_found"
    )
  }

  if (!identical(job[["status"]], "complete")) {
    rtemis.core::abort(
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
    if (!inherits(result, "rtemis::Supervised")) {
      rtemis.core::abort(
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
  if (slice == "transformed") {
    if (!inherits(result, "rtemis::Decomposition")) {
      rtemis.core::abort(
        "`transformed` slice requires a `Decomposition` result.",
        class = "rtemislive_invalid_params"
      )
    }
    tr_dt <- transformed_table(result)
    if (is.null(tr_dt) || NROW(tr_dt) == 0L) {
      return(make_response(
        req_id,
        list(rows = 0L, cols = 0L, columns = list(), format = "arrow-ipc")
      ))
    }
    payload <- encode_arrow_ipc(tr_dt)
    return(make_response_payload(
      req_id,
      list(
        rows = NROW(tr_dt),
        cols = NCOL(tr_dt),
        columns = names(tr_dt),
        format = "arrow-ipc"
      ),
      payload
    ))
  }
  if (slice == "assignments") {
    if (!inherits(result, "rtemis::Clustering")) {
      rtemis.core::abort(
        "`assignments` slice requires a `Clustering` result.",
        class = "rtemislive_invalid_params"
      )
    }
    as_dt <- assignments_table(result)
    if (is.null(as_dt) || NROW(as_dt) == 0L) {
      return(make_response(
        req_id,
        list(rows = 0L, cols = 0L, columns = list(), format = "arrow-ipc")
      ))
    }
    payload <- encode_arrow_ipc(as_dt)
    return(make_response_payload(
      req_id,
      list(
        rows = NROW(as_dt),
        cols = NCOL(as_dt),
        columns = names(as_dt),
        format = "arrow-ipc"
      ),
      payload
    ))
  }
  if (slice == "loadings") {
    if (!inherits(result, "rtemis::Decomposition")) {
      rtemis.core::abort(
        "`loadings` slice requires a `Decomposition` result.",
        class = "rtemislive_invalid_params"
      )
    }
    ld_dt <- loadings_table(result)
    if (is.null(ld_dt) || NROW(ld_dt) == 0L) {
      # Algorithm has no loadings concept (UMAP / tSNE / Isomap) or the
      # backend didn't expose them. Same empty-pointer convention as
      # `varimp` for algorithms without varimp.
      return(make_response(
        req_id,
        list(rows = 0L, cols = 0L, columns = list(), format = "arrow-ipc")
      ))
    }
    payload <- encode_arrow_ipc(ld_dt)
    return(make_response_payload(
      req_id,
      list(
        rows = NROW(ld_dt),
        cols = NCOL(ld_dt),
        columns = names(ld_dt),
        format = "arrow-ipc"
      ),
      payload
    ))
  }
  rtemis.core::abort(
    paste0(
      "Unsupported slice `",
      slice,
      "`. Use `summary`, `raw`, `varimp`, `predictions`, `metrics`, ",
      "`transformed`, `loadings`, or `assignments`."
    ),
    class = "rtemislive_invalid_params"
  )
}


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
    rtemis.core::abort(
      "`job_id` is required.",
      class = "rtemislive_invalid_params"
    )
  }
  s <- connection_session(conn)
  deleted <- delete_job(s, job_id)
  make_response(req_id, list(deleted = deleted))
}


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
  "decomp.algorithms" = list(
    handler = handle_decomp_algorithms,
    requires = "authed"
  ),
  "decomp.algorithm.describe" = list(
    handler = handle_decomp_algorithm_describe,
    requires = "authed"
  ),
  "cluster.algorithms" = list(
    handler = handle_cluster_algorithms,
    requires = "authed"
  ),
  "cluster.algorithm.describe" = list(
    handler = handle_cluster_algorithm_describe,
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
  "decomp" = list(
    handler = handle_decomp,
    requires = c("authed", "attached")
  ),
  "cluster" = list(
    handler = handle_cluster,
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
