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
#' @param heartbeat_interval Numeric, seconds. Per-session `heartbeat`
#'   tick rate. `0` (the default) disables emission; a positive value
#'   re-enables it.
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
  heartbeat_interval = 0,
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
    # The only arm carrying structured data. A refused run has findings to
    # report -- each with a code, plain-language text, evidence and where one
    # exists a patch that fixes it -- and a caller made to parse them back out
    # of a sentence cannot act on them.
    rtemislive_invalid_config = function(e) {
      make_error(
        req_id,
        "invalid_config",
        conditionMessage(e),
        details = list(diagnostics = e[["diagnostics"]])
      )
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


#' Assert a rtemis catalogue table has the columns we index by name
#'
#' `[.data.frame` returns `character(0)` for a missing column rather than
#' erroring, so a rtemis-side column rename would otherwise ship entries with
#' empty fields to the client instead of failing here.
#'
#' @author EDG
#' @keywords internal
#' @noRd
.require_cols <- function(tbl, cols, what) {
  missing <- setdiff(cols, colnames(tbl))
  if (length(missing) > 0L) {
    rtemis.core::abort(
      paste0(
        "rtemis `",
        what,
        "` is missing expected column(s): ",
        paste0("`", missing, "`", collapse = ", "),
        "."
      ),
      class = "rtemislive_internal_error"
    )
  }
  invisible(tbl)
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
  .require_cols(
    tbl,
    c("name", "description", "classification", "regression", "survival"),
    "supervised_algorithms"
  )
  algorithms <- lapply(seq_len(nrow(tbl)), function(i) {
    list(
      name = as.character(tbl[i, "name"]),
      description = as.character(tbl[i, "description"]),
      supports_classification = isTRUE(as.logical(tbl[i, "classification"])),
      supports_regression = isTRUE(as.logical(tbl[i, "regression"])),
      supports_survival = isTRUE(as.logical(tbl[i, "survival"]))
    )
  })
  make_response(req_id, list(algorithms = algorithms))
}


#' `decomp.algorithms` handler
#'
#' Returns the catalogue of decomposition algorithms. Each entry:
#' `{ name, description, applicable }`, where `applicable` is `TRUE` when the
#' algorithm's fitted transform can be applied to new data (the requirement for
#' use as a `train()` `decomposition_config`). Per-algorithm config schemas are
#' fetched separately via `handle_decomp_algorithm_describe()`
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
  applicable <- asNamespace("rtemis")[["decom_algorithms_applicable"]] %||%
    character()
  algorithms <- lapply(seq_len(nrow(tbl)), function(i) {
    name <- as.character(tbl[i, 1L])
    list(
      name = name,
      description = as.character(tbl[i, 2L]),
      applicable = name %in% applicable
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


# Enumerated choices declared by a config object's S7 properties.
#
# rtemis declares an argument's admissible values once, on the property
# (`prop_string(enum = ...)`), and validates against that list there. A
# `setup_*()` formal usually carries only the default value, so reading the
# enum off the built object is what lets `describe` report every choice — with
# no second copy of the list to drift, and no arg left rendering as a free-text
# box in the client just because its signature is a scalar.
prop_enums <- function(config) {
  if (!inherits(config, "S7_object")) {
    return(list())
  }
  Filter(
    length,
    lapply(S7::S7_class(config)@properties, function(p) {
      # NULL unless built by a `prop_*` factory; rtemis carries the spec as a
      # plain named list, so read it with `[[` rather than an S7 accessor.
      spec <- p[["spec"]]
      if (is.null(spec)) NULL else as.list(spec[["enum"]])
    })
  )
}


# Build the schema for one setup_*() function.
#
# Walks the formals, classifying each arg as:
# - enumerated (property enum, or a `c("a", "b", ...)` formal) -> `choices`,
#   with the first value as the default
# - any other multi-element default -> flattened to its first element
# - single-value default (or NULL) -> reported as-is
#
# `tunable_set` is the set of arg names considered tunable. For
# algorithm hyperparameters this is `Hyperparameters@tunable_hyperparameters`;
# for resampler / other configs pass `character(0)`.
#
# `config` is the object `setup_fn()` builds, when the caller has one; it
# carries the property enums (see `prop_enums`).
.live_build_schema <- function(
  setup_fn,
  hp_values = list(),
  tunable_set = character(),
  config = NULL
) {
  enums <- prop_enums(config)
  fmls <- formals(setup_fn)
  lapply(names(fmls), function(arg) {
    raw <- fmls[[arg]]
    # A formal whose default simply names another formal (e.g.
    # `center = scale`) should report that formal's concrete default,
    # not the captured function the bare symbol would evaluate to.
    if (is.symbol(raw) && as.character(raw) %in% names(fmls)) {
      raw <- fmls[[as.character(raw)]]
    }
    default <- tryCatch(
      eval(raw, envir = asNamespace("rtemis")),
      error = function(e) NULL
    )
    # Enumerated choices come from the property, which is where rtemis declares
    # and validates them; the `match.arg`-style `c("a", "b", ...)` formal is
    # the fallback for schemas built without an object -- a dispatcher whose
    # `type` selects the subclass has no single object to read against.
    choices <- enums[[arg]] %||%
      if (is.character(default) && length(default) > 1L) as.list(default)
    # A multi-value default is the `match.arg` idiom: `setup_fn()` uses the
    # first element.
    if (length(default) > 1L) {
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

  hyperparameters <- .live_build_schema(setup_fn, hp_values, tunable_set, hp)

  alg_row <- tryCatch(
    asNamespace("rtemis")[["supervised_algorithms"]],
    error = function(e) NULL
  )
  description <- NA_character_
  if (is.data.frame(alg_row)) {
    .require_cols(alg_row, c("name", "description"), "supervised_algorithms")
    hit <- which(alg_row[["name"]] == alg_name)[1L]
    if (!is.na(hit)) {
      description <- as.character(alg_row[hit, "description"])
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
    tunable_set = character(),
    config = cfg
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
    tunable_set = character(),
    config = cfg
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


#' `preprocessor.describe` handler
#'
#' Returns the schema for `setup_SupervisedPreprocessor()` so the client can
#' render a preprocessing configuration form. Same shape and machinery as
#' `algorithm.describe`. `impute_missRanger_params` is a nested list with
#' no scalar control, so it is omitted here and left to the server-side
#' default; the matching `train` handler still accepts it.
#'
#' The supervised type, not `setup_Preprocessor()`. A preprocessor `train()`
#' fits is replayed at predict time, so it cannot drop rows, and it cannot
#' learn which columns to drop from a fold's own training subset -- four
#' operations `SuperConfigLive` has no property for. Describing the wider type
#' here would offer a client four settings whose submission `build_super_config()`
#' rejects, and leave every consumer to re-derive which four.
#'
#' Wire response: `{ parameters: [{ name, type, default, tunable,
#' choices? }, ...] }`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_preprocessor_describe <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  skip <- "impute_missRanger_params"
  parameters <- Filter(
    function(p) !(p[["name"]] %in% skip),
    .live_build_schema(
      setup_SupervisedPreprocessor,
      config = setup_SupervisedPreprocessor()
    )
  )
  make_response(req_id, list(parameters = parameters))
}


#' `config.validate` handler
#'
#' Validates one config -- against the published schema, and against a dataset
#' when `data_handle` names one -- and returns what is wrong with it.
#'
#' Synchronous, unlike `train` / `decomp` / `cluster`. Validation is a
#' millisecond of work on data already in this session, so routing it through
#' the job store would buy nothing and cost the client a second round trip
#' before it could show the user why their plan will not run.
#'
#' The whole check is `rtemis::validate_config()`. This handler translates the
#' wire and nothing else, which is two things: the frame decodes with
#' `simplifyVector = FALSE`, so JSON arrays arrive as lists of length-1 atomics
#' (`.collapse_scalar_lists()`); and `schema_id` travels beside the config
#' rather than inside it, so it is spliced back in as the `$schema` the config
#' document declares. `.from_wire()` is deliberately not used: it translates the
#' *flat* `train` params, and this method receives a schema-shaped document.
#'
#' Wire params:
#'
#' - `schema_id` - the config's schema URL, e.g.
#'   `https://schema.rtemis.org/supervised/v1/schema.json`
#' - `config` - the config document, as a JSON object
#' - `data_handle` - optional; id of an uploaded dataset on this session.
#'   Omitted, only the schema is checked.
#' - `outcome` - optional; name of the outcome column. Omitted, rtemis's
#'   convention applies and the last column is the outcome.
#' - `step` - optional; this config's position in the plan, recorded on every
#'   finding so a client can attribute it without tracking the request.
#'
#' Wire response: `{ diagnostics: [{ code, severity, step, message, plain,
#' evidence, fix }, ...] }`, empty when the config is clean. A finding is a
#' report rather than a failure, so a config with errors still answers `ok`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_config_validate <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()

  schema_id <- params[["schema_id"]]
  config <- params[["config"]]
  if (is.null(schema_id) || is.null(config)) {
    rtemis.core::abort(
      "`schema_id` and `config` are required.",
      class = "rtemislive_invalid_params"
    )
  }
  if (!is.list(config)) {
    rtemis.core::abort(
      "`config` must be a JSON object.",
      class = "rtemislive_invalid_params"
    )
  }

  data_dt <- NULL
  if (!is.null(params[["data_handle"]])) {
    data_dt <- get_data(connection_session(conn), params[["data_handle"]])
  }

  document <- .collapse_scalar_lists(config)
  document[["$schema"]] <- schema_id

  # An empty `outcome` is a client's "no selection" -- a select with nothing
  # chosen -- and means unset, as an empty `positive_class` does in
  # `.from_wire()`. Left as `""` it would be reported as a column that is not
  # in the data, which is true but useless.
  outcome <- params[["outcome"]]
  if (is.character(outcome) && length(outcome) == 1L && !nzchar(outcome)) {
    outcome <- NULL
  }

  diagnostics <- rtemis::validate_config(
    config = document,
    data = data_dt,
    outcome = outcome,
    step = params[["step"]]
  )
  # `to_json()` walks the published properties of each finding, so the wire
  # shape follows `diagnostic/v1/schema.json` without being restated here.
  make_response(
    req_id,
    list(diagnostics = rtemis::to_json(diagnostics)[["diagnostics"]])
  )
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
  # Spec (implementation.md): `{ name }` or `{ session_id }`.
  join_id <- params[["session_id"]]
  if (!is.null(join_id)) {
    s <- get_session_by_id(join_id)
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
  replay_buffered_events(server, s, conn)
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

#' `train` handler
#'
#' Submits a supervised-learning job. Hands the wire params to
#' `build_super_config()` — which delegates to rtemis's own config
#' reconstructor and binds the in-memory data resolved from `data_handle` —
#' then dispatches through `train()`.
#'
#' Wire params mirror `SuperConfig`'s properties one for one, so the block
#' shapes are documented by the schemas at schema.rtemis.org rather than
#' restated here. All optional except `data_handle`:
#'
#' - `data_handle` - id of a previously-uploaded dataset on this session
#' - `algorithm` - character, see `algorithms` method. Omit it when
#'   `hyperparameters` is a variants set, which names the algorithm inside each
#'   variant, or when no learner is being named at all and rtemis's default
#'   should apply.
#' - `hyperparameters` - flat `name -> value` map, the nested
#'   `{ algorithm, hyperparameters }` shape, or the variants set
#'   `{ variants: { <name>: { algorithm, hyperparameters } } }` -- the third is
#'   `supervised/v1`'s second form for the block, and reaches
#'   `.list_to_HyperparametersSet()` untouched because `.nest_hyperparameters()`
#'   only folds when a top-level `algorithm` is present.
#' - `preprocessor_config`, `decomposition_config`, `tuner_config`,
#'   `outer_resampling_config`, `execution_config` - JSON objects
#' - `weights` - character; column name in the dataset used as weights
#' - `positive_class` - character; binary-classification positive class
#' - `question` - character; user-provided label for the run
#'
#' The config is checked against the data before a job is created, and a run
#' that cannot answer the question asked is refused rather than queued: an
#' `invalid_config` error carrying the findings in `details`, in the shape
#' `config.validate` returns them. A caller repairing a plan then never spends
#' a job slot to learn what is wrong with it.
#'
#' Findings that do not stop the run travel with the accepted job as
#' `diagnostics`, so a warning reaches the submitter instead of only the daemon
#' log. `train()` checks a `SuperConfigLive` again on the other side -- that is
#' the type's own guarantee rather than this handler's, and is what makes "no
#' run reaches R unchecked" true of every submitter and not just this one.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_train <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()

  data_handle <- params[["data_handle"]]
  # Only the data is required. A config names its learner in one of three ways
  # and all three are valid: a top-level `algorithm` with a flat map, a
  # `hyperparameters` variants set that names the algorithm inside each member,
  # or nothing at all -- `hyperparameters` is nullable in `supervised/v1` and
  # `train()` has its own default, so "unset" means "rtemis chooses" exactly as
  # it does for every other block.
  #
  # This handler used to require `algorithm`, which made two of the three
  # unsubmittable: a caller holding a schema-valid config had nothing to put
  # here, and one that sent an empty string to get past the check was refused
  # much later by a message naming a learner nobody had written. Nothing is
  # lost by dropping the check -- a misspelled key is caught by
  # `.list_to_SuperConfig()`, which rejects anything it does not model.
  if (is.null(data_handle)) {
    rtemis.core::abort(
      "`data_handle` is required.",
      class = "rtemislive_invalid_params"
    )
  }

  s <- connection_session(conn)
  data_dt <- get_data(s, data_handle)

  cfg <- tryCatch(
    build_super_config(params, data_dt),
    error = function(e) {
      # Include the parent's message in the wire text so the browser surfaces
      # the specific reason (which block, which hyperparameter). The condition
      # object still carries `parent = e` for programmatic handlers.
      rtemis.core::abort(
        "Could not build training config: ",
        conditionMessage(e),
        parent = e,
        class = "rtemislive_invalid_params"
      )
    }
  )

  # The gate. `validate_config()` reads a profile of the data rather than the
  # rows, so this costs a fraction of a second at any size a browser uploaded,
  # and it is the same call `train()` makes on the other side -- the answer here
  # and the daemon's cannot differ.
  diagnostics <- rtemis::validate_config(cfg, data = data_dt)
  findings <- rtemis::to_json(diagnostics)[["diagnostics"]]
  blocking <- Filter(function(d) identical(d[["severity"]], "error"), findings)
  if (length(blocking) > 0L) {
    rtemis.core::abort(
      "Configuration cannot run on this data.",
      class = "rtemislive_invalid_config",
      data = list(diagnostics = findings)
    )
  }

  # No progress plumbing here: `train()` reports its nested progress through the
  # rtemis.core msg sink, which `ensure_daemon_sink()` has already installed on this
  # daemon. Each `progress_begin`/`update`/`end` ships an envelope carrying node ids,
  # counters and label, and `route_progress()` turns those into nested
  # `job.progress` events. (Until rtemis 1.3.5 a `progress = ` callback duplicated a
  # single fold counter on the side; the sink reports every level of the run.)
  job <- submit_job(
    session = s,
    type = "train",
    params = params,
    expr = quote(rtemis::train(cfg)),
    env = list(cfg = cfg),
    max_concurrent = server[["max_concurrent"]] %||% 8L
  )

  resp <- list(job_id = job[["id"]], status = job[["status"]])
  if (identical(job[["status"]], "queued")) {
    resp[["queue_position"]] <- job_queue_position(job)
  }
  # Always present, empty when the config is clean: a caller reading
  # `length()` should not have to tell an absent field from an empty one.
  resp[["diagnostics"]] <- findings
  make_response(req_id, resp)
}


#' Unsupervised (`decomp` / `cluster`) handler
#'
#' `decomp` and `cluster` are the same job: resolve the algorithm, optionally
#' subset the dataset to a feature list, build the algorithm's config from the
#' wire params, and submit. Only the algorithm catalogue and the rtemis entry
#' point differ, so both live here and `handle_decomp()` / `handle_cluster()`
#' are one line each.
#'
#' Wire params (all optional except `data_handle`, `algorithm`):
#'
#' - `data_handle` - id of a previously-uploaded dataset on this session
#' - `algorithm` - character, one of `<kind>.algorithms`
#' - `hyperparameters` - flat `name -> value` map accepted by `setup_<Algo>()`.
#'   A canonical `DecompositionConfig` / `ClusteringConfig` nests the same map
#'   under `config`; both keys are read.
#' - `features` - character[]; subset of columns to use. Omitted = all columns.
#' - `question` - character; user-provided label for the run
#'
#' @param kind Character: `"decomp"` or `"cluster"`, keying `unsupervised_kinds`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_unsupervised <- function(conn, frame, server, kind) {
  spec <- unsupervised_kinds[[kind]]
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

  # Normalizes casing and rejects anything not in the catalogue.
  alg_name <- tryCatch(
    spec[["get_name"]](algorithm),
    error = function(e) {
      rtemis.core::abort(
        paste0("Unknown ", spec[["label"]], " algorithm `", algorithm, "`."),
        class = "rtemislive_not_found"
      )
    }
  )

  s <- connection_session(conn)
  x <- subset_features(get_data(s, data_handle), params[["features"]])

  # The wire sends the form's flat name -> value map as `hyperparameters`; a
  # canonical config nests the same map under `config`. `.drop_meta_keys()`
  # strips `$`-prefixed document metadata (`$schema`) that a config lifted from
  # a schema.rtemis.org file carries; any other unknown key still errors.
  hp <- params[["config"]] %||% params[["hyperparameters"]] %||% list()
  cfg <- tryCatch(
    do.call(
      # rtemis exports every `setup_<Algo>()`; `alg_name` came from the
      # catalogue, so the lookup cannot miss.
      getExportedValue("rtemis", paste0("setup_", alg_name)),
      as.list(rtemis::.drop_meta_keys(.collapse_scalar_lists(hp)))
    ),
    error = function(e) {
      rtemis.core::abort(
        "Could not build ",
        spec[["label"]],
        " config: ",
        conditionMessage(e),
        parent = e,
        class = "rtemislive_invalid_params"
      )
    }
  )

  # No `progress` callback: neither entry point has fold-boundary checkpoints.
  # The daemon-side msg sink (set up by `init_daemon_progress`) still ships
  # every internal `msg()` call (data summary, "Decomposing with PCA...",
  # outro) as a progress envelope, so the browser gets inline status without
  # any per-handler wiring.
  job <- submit_job(
    session = s,
    type = kind,
    params = params,
    expr = spec[["expr"]],
    env = list(x = x, alg_name = alg_name, cfg = cfg),
    max_concurrent = server[["max_concurrent"]] %||% 8L
  )

  resp <- list(job_id = job[["id"]], status = job[["status"]])
  if (identical(job[["status"]], "queued")) {
    resp[["queue_position"]] <- job_queue_position(job)
  }
  make_response(req_id, resp)
}


#' Per-kind slots for `handle_unsupervised()`
#'
#' @author EDG
#' @keywords internal
#' @noRd
unsupervised_kinds <- list(
  decomp = list(
    label = "decomposition",
    get_name = function(algorithm) get_decom_name(algorithm),
    expr = quote(
      rtemis::decomp(x, algorithm = alg_name, config = cfg, verbosity = 1L)
    )
  ),
  cluster = list(
    label = "clustering",
    get_name = function(algorithm) get_clust_name(algorithm),
    expr = quote(
      rtemis::cluster(x, algorithm = alg_name, config = cfg, verbosity = 1L)
    )
  )
)


#' Restrict a dataset to a requested feature subset
#'
#' @param data_dt `data.table`: Dataset resolved from a `data_handle`.
#' @param features Character[] or NULL: Requested columns. NULL = all columns.
#'
#' @return `data.table`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
subset_features <- function(data_dt, features) {
  if (is.null(features)) {
    return(data_dt)
  }
  features <- unlist(features, use.names = FALSE)
  if (
    !is.character(features) || length(features) == 0L || any(!nzchar(features))
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
  data_dt[, features, with = FALSE]
}


#' `decomp` handler
#'
#' Submits an unsupervised decomposition job through `rtemis::decomp()`.
#' See `handle_unsupervised()` for the wire params.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_decomp <- function(conn, frame, server) {
  handle_unsupervised(conn, frame, server, "decomp")
}


#' `cluster` handler
#'
#' Submits an unsupervised clustering job through `rtemis::cluster()`.
#' See `handle_unsupervised()` for the wire params.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_cluster <- function(conn, frame, server) {
  handle_unsupervised(conn, frame, server, "cluster")
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
#' - `record`: the rtemis run record (`rtemis::record()`) as JSON, validating
#'   against `https://schema.rtemis.org/<family>/v1/record.json`: every config
#'   field with the origin it came from, the values each fold resolved, the
#'   tuning grid, and provenance (versions, platform, timing, data
#'   fingerprints). Where `summary` says what the result *is*, this says what
#'   the run *did*. Returned bare so the client can copy, download and validate
#'   the document as-is; when there is no record to give, a small
#'   `{available: false, reason}` pointer instead (`reason` is `unsupported`,
#'   `no_input_config`, or `too_large`).
#' - `varimp`: small JSON pointer (`{rows, cols, columns}`) + Arrow IPC
#'   payload of the variable-importance table.
#' - `predictions`: small JSON pointer + Arrow IPC of the long-format
#'   predictions table.
#' - `roc`: small JSON pointer + Arrow IPC of the long-format ROC-curve
#'   table (`split`, `class`, `fold`, `fpr`, `tpr`, `auc`) for classification
#'   fits, computed by `rtemis::roc_curve`; `fold` is `"aggregate"` for the
#'   pooled curve and per-resample labels otherwise. Empty pointer otherwise.
#' - `session`: small JSON pointer + Arrow IPC of the execution-timeline
#'   table (`label`, `start`, `end`, `kind`, `status`, `failed`, `tip`) built
#'   by `rtemis::session_timeline` from the model's observability session;
#'   the pointer carries a `colors` map (`kind` -> hex fill, from
#'   `rtemis::session_kind_colors`). Empty pointer when the result has no
#'   recorded session (non-supervised results, or models from older rtemis).
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
  if (slice == "record") {
    return(make_response(req_id, record_json(result)))
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
  if (slice == "roc") {
    roc_dt <- roc_table(result)
    if (is.null(roc_dt) || NROW(roc_dt) == 0L) {
      # Not a classification fit, or no predicted probabilities: empty
      # pointer (no payload) so the client distinguishes it from an error.
      return(make_response(
        req_id,
        list(rows = 0L, cols = 0L, columns = list(), format = "arrow-ipc")
      ))
    }
    payload <- encode_arrow_ipc(roc_dt)
    return(make_response_payload(
      req_id,
      list(
        rows = NROW(roc_dt),
        cols = NCOL(roc_dt),
        columns = names(roc_dt),
        format = "arrow-ipc"
      ),
      payload
    ))
  }
  if (slice == "session") {
    session_dt <- session_table(result)
    if (is.null(session_dt) || NROW(session_dt) == 0L) {
      # No observability session on this result (non-supervised result, or a
      # model trained before session capture existed): empty pointer (no
      # payload) so the client distinguishes "not available" from an error.
      return(make_response(
        req_id,
        list(rows = 0L, cols = 0L, columns = list(), format = "arrow-ipc")
      ))
    }
    payload <- encode_arrow_ipc(session_dt)
    return(make_response_payload(
      req_id,
      list(
        rows = NROW(session_dt),
        cols = NCOL(session_dt),
        columns = names(session_dt),
        format = "arrow-ipc",
        # Fixed kind -> hex fill map shared with rtemis.draw's Gantt widget,
        # keyed in first-seen (depth-first) display order.
        colors = as.list(rtemis::session_kind_colors(
          unique(session_dt[["kind"]])
        ))
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
      "`. Use `summary`, `raw`, `record`, `varimp`, `predictions`, `roc`, ",
      "`session`, `metrics`, `transformed`, `loadings`, or `assignments`."
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


#' `job.load` handler
#'
#' The counterpart of `job.save`: register an uploaded `.rds` as a completed
#' job, so every other job.* RPC serves it exactly as it would one this
#' server trained itself. `job.result`/`job.save`/`job.status` only ever
#' look at `job[["result"]]` and `job[["status"]]` -- nothing checks how the
#' job env was built, which is what makes this a registration step rather
#' than a parallel implementation of the slicing logic those already have.
#' See `load_model_job()` for the read, validate, and register.
#'
#' Single-shot: the whole `.rds` in one frame. Large enough to exceed a
#' WebSocket frame -- confirmed in practice, not just in principle -- and the
#' connection closes with code 1009 before this handler ever runs; a client
#' that cannot bound a model's size in advance should use `job.load.begin` /
#' `.chunk` / `.end` instead, mirroring `data.upload`'s own two paths for the
#' same reason.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_job_load <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  payload <- frame[["payload"]]
  if (is.null(payload) || !is.raw(payload) || length(payload) == 0L) {
    rtemis.core::abort(
      "An `.rds` payload is required for job.load.",
      class = "rtemislive_invalid_params"
    )
  }
  s <- connection_session(conn)
  job <- load_model_job(s, payload)
  make_response(req_id, job_summary(job))
}


#' `job.load.begin` handler
#'
#' The chunked counterpart of `job.load`, for a model whose serialized size
#' exceeds a single WebSocket frame -- confirmed to bite in practice: an
#' actual fitted `Supervised` object (as opposed to the small fixtures this
#' package's own tests train) closed the connection with code 1009 ("message
#' too large") on the single-shot path. Reuses `begin_upload` unchanged --
#' chunk bookkeeping does not care what the bytes decode to.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_job_load_begin <- function(conn, frame, server) {
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


#' `job.load.chunk` handler
#'
#' Requires the chunk bytes in the frame payload. Reuses `chunk_upload`
#' unchanged, for `handle_job_load_begin`'s reason.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_job_load_chunk <- function(conn, frame, server) {
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


#' `job.load.end` handler
#'
#' Reassembles the chunks (`assemble_chunked_upload`, shared with
#' `end_upload`) and finalizes into a job exactly as `handle_job_load` does
#' for the single-shot path (`load_model_job`) -- the two ways of getting the
#' bytes here converge on one finalization, so a rejection means the same
#' thing whichever path a client used.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_job_load_end <- function(conn, frame, server) {
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
  assembled <- assemble_chunked_upload(s, upload_id)
  job <- load_model_job(s, assembled[["bytes"]])
  make_response(req_id, job_summary(job))
}


#' `job.load.cancel` handler
#'
#' Reuses `cancel_upload` unchanged, for `handle_job_load_begin`'s reason.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_job_load_cancel <- function(conn, frame, server) {
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


#' `job.save` handler
#'
#' Serialize a completed job's full result object to an `.rds` file on the
#' server's local filesystem via [saveRDS()]. The client supplies a target
#' directory (created if missing) and an optional filename; the complete
#' rtemis object (`Supervised` / `Decomposition` / `Clustering`) is written
#' so it can be reloaded in R with [readRDS()] for prediction or inspection.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_job_save <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  job_id <- params[["job_id"]]
  if (is.null(job_id)) {
    rtemis.core::abort(
      "`job_id` is required.",
      class = "rtemislive_invalid_params"
    )
  }
  dir <- params[["dir"]]
  if (is.null(dir) || !is.character(dir) || length(dir) != 1L || !nzchar(dir)) {
    rtemis.core::abort(
      "`dir` is required.",
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
  if (is.null(result)) {
    rtemis.core::abort(
      "Job has no result to save.",
      class = "rtemislive_invalid_params"
    )
  }

  dir <- path.expand(dir)
  if (!dir.exists(dir)) {
    created <- dir.create(dir, recursive = TRUE, showWarnings = FALSE)
    if (!created) {
      rtemis.core::abort(
        "Could not create directory '",
        dir,
        "'.",
        class = "rtemislive_io_error"
      )
    }
  }

  filename <- params[["filename"]]
  if (
    is.null(filename) ||
      !is.character(filename) ||
      length(filename) != 1L ||
      !nzchar(filename)
  ) {
    filename <- paste0(job[["type"]] %||% "rtemis_object", "_", job_id)
  }
  # `basename()` strips any directory components a client might smuggle into
  # `filename`, keeping the write confined to the requested `dir`.
  filename <- basename(filename)
  if (!grepl("\\.rds$", filename, ignore.case = TRUE)) {
    filename <- paste0(filename, ".rds")
  }
  path <- file.path(dir, filename)

  saveRDS(result, path)

  make_response(
    req_id,
    list(
      path = normalizePath(path, mustWork = FALSE),
      bytes = as.numeric(file.size(path))
    )
  )
}


#' Open a native OS directory picker and return the chosen path
#'
#' The rtemis server runs on the user's own machine, so it can present a
#' native folder chooser (Finder on macOS, the shell picker on Windows,
#' `zenity` on Linux). Returns the selected directory, or `NULL` if the
#' dialog was cancelled or no picker is available. The call blocks the
#' server loop while the dialog is open; this is acceptable for the local
#' single-user case.
#'
#' @param prompt Character scalar shown in the dialog title.
#'
#' @return Character scalar path, or `NULL`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
choose_directory <- function(prompt = "Select a folder") {
  os <- Sys.info()[["sysname"]]
  path <- tryCatch(
    suppressWarnings({
      if (identical(os, "Darwin")) {
        script <- sprintf(
          'POSIX path of (choose folder with prompt "%s")',
          gsub('"', "", prompt, fixed = TRUE)
        )
        system2(
          "osascript",
          c("-e", shQuote(script)),
          stdout = TRUE,
          stderr = FALSE
        )
      } else if (identical(os, "Windows")) {
        utils::choose.dir(caption = prompt)
      } else {
        system2(
          "zenity",
          c("--file-selection", "--directory", paste0("--title=", prompt)),
          stdout = TRUE,
          stderr = FALSE
        )
      }
    }),
    error = function(e) NULL
  )
  if (length(path) == 0L) {
    return(NULL)
  }
  path <- trimws(path[[1L]])
  if (is.na(path) || !nzchar(path)) {
    return(NULL)
  }
  path
}


#' `dialog.choose_dir` handler
#'
#' Pops a native directory chooser on the server machine and returns the
#' selected path (or `cancelled = TRUE`). Used by the client to populate a
#' save destination without the user typing a path.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_choose_dir <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  prompt <- params[["prompt"]] %||% "Select a folder"
  path <- choose_directory(prompt)
  if (is.null(path)) {
    return(make_response(req_id, list(cancelled = TRUE)))
  }
  make_response(req_id, list(path = path, cancelled = FALSE))
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
  "preprocessor.describe" = list(
    handler = handle_preprocessor_describe,
    requires = "authed"
  ),
  # Attached, not merely authed: a `data_handle` is session-scoped, so the
  # data half of validation has nowhere to resolve one without a session.
  "config.validate" = list(
    handler = handle_config_validate,
    requires = c("authed", "attached")
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
  ),
  "job.save" = list(
    handler = handle_job_save,
    requires = c("authed", "attached")
  ),
  "job.load" = list(
    handler = handle_job_load,
    requires = c("authed", "attached")
  ),
  "job.load.begin" = list(
    handler = handle_job_load_begin,
    requires = c("authed", "attached")
  ),
  "job.load.chunk" = list(
    handler = handle_job_load_chunk,
    requires = c("authed", "attached")
  ),
  "job.load.end" = list(
    handler = handle_job_load_end,
    requires = c("authed", "attached")
  ),
  "job.load.cancel" = list(
    handler = handle_job_load_cancel,
    requires = c("authed", "attached")
  ),
  "dialog.choose_dir" = list(
    handler = handle_choose_dir,
    requires = "authed"
  )
)
