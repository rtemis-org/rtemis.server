# data.R
# ::rtemis.server::
# 2026- EDG rtemis.org

# Data-handle store and Arrow upload assembly for rtemislive.
# spec: rtemislive.md 6.2 and 7
#
# Each session env holds a `data` sub-env keyed by data_handle id. Each
# value is itself an env containing:
#
#   handle      character - `data-<hex>`
#   name        character - user-friendly label
#   created_at  POSIXct
#   last_used   POSIXct - bumped on every access
#   data        data.table - the actual tabular data
#   rows        integer
#   cols        integer
#
# Chunked uploads (`data.upload.begin` / `chunk` / `end`) use a separate
# `pending_uploads` sub-env on the session, since assembly is in-progress
# state that doesn't become a data_handle until `end_upload()` succeeds. An
# upload that neither ends nor is canceled is dropped when the connection that
# began it leaves the session, or `.UPLOAD_TTL` seconds after its last chunk.

# Seconds after the last chunk at which an unfinished upload is dropped.
.UPLOAD_TTL <- 600

# Unfinished uploads that a session holds at the same time.
.UPLOADS_MAX <- 4L

# %% Identifier helpers ------------------------------------------------------

#' Generate a data_handle id
#'
#' Returns `data-<hex16>`.
#'
#' @return Character scalar.
#'
#' @author EDG
#' @keywords internal
#' @noRd
new_data_handle_id <- function() {
  rtemis.core::check_dependencies("uuid")
  hex <- gsub("-", "", uuid::UUIDgenerate(use.time = TRUE), fixed = TRUE)
  paste0("data-", substr(hex, 1L, 16L))
}


#' Generate an upload_id (for chunked uploads in progress)
#'
#' @return Character scalar - `upload-<hex16>`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
new_upload_id <- function() {
  rtemis.core::check_dependencies("uuid")
  hex <- gsub("-", "", uuid::UUIDgenerate(use.time = TRUE), fixed = TRUE)
  paste0("upload-", substr(hex, 1L, 16L))
}


# %% Wire parameter checks ---------------------------------------------------

#' Check that a wire parameter is a non-empty string
#'
#' @param x Value of the parameter.
#' @param name Character: Parameter name, for the message.
#'
#' @return `x`, invisibly. Aborts with class `rtemislive_invalid_params`
#'   otherwise.
#'
#' @author EDG
#' @keywords internal
#' @noRd
check_wire_string <- function(x, name) {
  if (!is.character(x) || length(x) != 1L || is.na(x) || !nzchar(x)) {
    rtemis.core::abort(
      "`",
      name,
      "` must be a single non-empty character string.",
      class = "rtemislive_invalid_params"
    )
  }
  invisible(x)
}


#' Check that a wire parameter is a positive whole number
#'
#' @param x Value of the parameter.
#' @param name Character: Parameter name, for the message.
#'
#' @return `x`, invisibly. Aborts with class `rtemislive_invalid_params`
#'   otherwise.
#'
#' @author EDG
#' @keywords internal
#' @noRd
check_wire_whole <- function(x, name) {
  if (
    !is.numeric(x) ||
      length(x) != 1L ||
      !is.finite(x) ||
      x < 1 ||
      x != round(x)
  ) {
    rtemis.core::abort(
      "`",
      name,
      "` must be a positive whole number.",
      class = "rtemislive_invalid_params"
    )
  }
  invisible(x)
}


# %% Arrow IPC decoding ------------------------------------------------------

#' Decode Arrow IPC stream bytes into a data.table
#'
#' Thin wrapper around `arrow::read_ipc_stream()` plus a `data.table`
#' coercion. Provides clear error messages so handlers can map to wire
#' `invalid_params`.
#'
#' @param bytes Raw vector: Arrow IPC stream payload.
#'
#' @return data.table.
#'
#' @author EDG
#' @keywords internal
#' @noRd
decode_arrow_ipc <- function(bytes) {
  if (!is.raw(bytes)) {
    rtemis.core::abort("Arrow IPC payload must be a raw vector.")
  }
  if (length(bytes) == 0L) {
    rtemis.core::abort("Arrow IPC payload is empty.")
  }
  rtemis.core::check_dependencies("arrow")
  tbl <- tryCatch(
    arrow::read_ipc_stream(bytes, as_data_frame = FALSE),
    error = function(e) {
      rtemis.core::abort(
        "Could not decode Arrow IPC stream: ",
        conditionMessage(e),
        parent = e,
        class = "rtemislive_invalid_params"
      )
    }
  )
  # Arrow string columns land as `character` in R; rtemis's ML pipeline
  # expects categoricals as `factor`. DuckDB-WASM keeps text columns as
  # plain strings, and this is the single place that converts them. Numeric,
  # integer, and logical columns are unaffected.
  x <- data.table::as.data.table(as.data.frame(tbl))
  # Convert all character columns to factors in-place
  x[,
    names(.SD) := lapply(.SD, as.factor),
    .SDcols = sapply(x, is.character)
  ]
}


# %% Single-frame upload -----------------------------------------------------

#' Register a new data_handle from Arrow IPC bytes
#'
#' Used by `data.upload` (single-frame upload).
#'
#' @param session Session env.
#' @param name Character scalar: User-friendly label.
#' @param bytes Raw vector: Arrow IPC payload.
#' @param max_handles Integer: Per-session cap (default 16, spec paragraph 11.4).
#'
#' @return Named list - wire-shaped data_handle summary for the response.
#'
#' @author EDG
#' @keywords internal
#' @noRd
new_data_handle <- function(session, name, bytes, max_handles = 16L) {
  check_wire_string(name, "name")
  data_env <- session[["data"]]
  if (length(ls(data_env)) >= max_handles) {
    rtemis.core::abort(
      "Maximum number of data handles per session (",
      max_handles,
      ") reached.",
      class = "rtemislive_too_many"
    )
  }

  dt <- decode_arrow_ipc(bytes)

  h <- new.env(parent = emptyenv())
  h[["handle"]] <- new_data_handle_id()
  h[["name"]] <- name
  now <- Sys.time()
  h[["created_at"]] <- now
  h[["last_used"]] <- now
  h[["data"]] <- dt
  h[["rows"]] <- nrow(dt)
  h[["cols"]] <- ncol(dt)

  data_env[[h[["handle"]]]] <- h
  touch_session(session)

  data_handle_summary(h)
}


# %% Chunked upload assembly -------------------------------------------------

#' Initialize a session's pending_uploads env on first use
#'
#' @param session Session env.
#'
#' @return Environment.
#'
#' @author EDG
#' @keywords internal
#' @noRd
pending_uploads <- function(session) {
  pu <- session[["pending_uploads"]]
  if (is.null(pu)) {
    pu <- new.env(parent = emptyenv())
    session[["pending_uploads"]] <- pu
  }
  pu
}


#' Begin a chunked upload
#'
#' @param session Session env.
#' @param name Character scalar: User-friendly label.
#' @param total_bytes Numeric \[1, Inf): Expected total payload size, a whole
#'   number.
#' @param n_chunks Numeric \[1, total_bytes\]: Number of chunks the client will
#'   send, a whole number.
#' @param connection_id Optional Character: Id of the connection that begins the
#'   upload, whose end drops it.
#'
#' @return Character scalar - the upload_id.
#'
#' @author EDG
#' @keywords internal
#' @noRd
begin_upload <- function(
  session,
  name,
  total_bytes,
  n_chunks,
  connection_id = NULL
) {
  check_wire_string(name, "name")
  check_wire_whole(total_bytes, "total_bytes")
  check_wire_whole(n_chunks, "n_chunks")
  if (n_chunks > total_bytes) {
    rtemis.core::abort(
      "`n_chunks` (",
      n_chunks,
      ") must not exceed `total_bytes` (",
      total_bytes,
      ").",
      class = "rtemislive_invalid_params"
    )
  }
  n_chunks <- as.integer(n_chunks)
  pu <- pending_uploads(session)
  if (length(ls(pu)) >= .UPLOADS_MAX) {
    rtemis.core::abort(
      "The session has ",
      .UPLOADS_MAX,
      " unfinished uploads, the maximum. Wait for one to end, or cancel it.",
      class = "rtemislive_too_many"
    )
  }

  u <- new.env(parent = emptyenv())
  u[["upload_id"]] <- new_upload_id()
  u[["name"]] <- name
  u[["total_bytes"]] <- total_bytes
  u[["n_chunks"]] <- n_chunks
  u[["chunks"]] <- vector("list", n_chunks)
  # Double, as is `total_bytes`: an upload may exceed `.Machine$integer.max`
  # bytes, where an integer sum is NA.
  u[["received_bytes"]] <- 0
  u[["received_count"]] <- 0L
  u[["created_at"]] <- Sys.time()
  u[["last_used"]] <- u[["created_at"]]
  u[["connection_id"]] <- connection_id

  pu[[u[["upload_id"]]]] <- u
  touch_session(session)
  u[["upload_id"]]
}


#' Receive a chunk of an in-progress upload
#'
#' @param session Session env.
#' @param upload_id Character scalar.
#' @param chunk_index Integer. 1-based.
#' @param bytes Raw vector.
#'
#' @return Named list - `received_count` (cumulative chunks received),
#'   `received_bytes`, `total_bytes`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
chunk_upload <- function(session, upload_id, chunk_index, bytes) {
  if (!is.raw(bytes) || length(bytes) == 0L) {
    rtemis.core::abort(
      "Chunk payload must be a non-empty raw vector.",
      class = "rtemislive_invalid_params"
    )
  }
  check_wire_string(upload_id, "upload_id")
  check_wire_whole(chunk_index, "chunk_index")
  pu <- pending_uploads(session)
  u <- pu[[upload_id]]
  if (is.null(u)) {
    rtemis.core::abort(
      "Unknown upload_id '",
      upload_id,
      "'. An unfinished upload is dropped ",
      .UPLOAD_TTL,
      " s after its last chunk, and when its connection leaves the session.",
      class = "rtemislive_not_found"
    )
  }
  if (chunk_index > u[["n_chunks"]]) {
    rtemis.core::abort(
      "`chunk_index` must be between 1 and ",
      u[["n_chunks"]],
      " (got ",
      chunk_index,
      ").",
      class = "rtemislive_invalid_params"
    )
  }
  chunk_index <- as.integer(chunk_index)
  if (!is.null(u[["chunks"]][[chunk_index]])) {
    rtemis.core::abort(
      "Chunk ",
      chunk_index,
      " already received.",
      class = "rtemislive_invalid_params"
    )
  }
  if (u[["received_bytes"]] + length(bytes) > u[["total_bytes"]]) {
    rtemis.core::abort(
      "Chunk ",
      chunk_index,
      " exceeds the declared size of ",
      u[["total_bytes"]],
      " bytes.",
      class = "rtemislive_invalid_params"
    )
  }
  u[["chunks"]][[chunk_index]] <- bytes
  u[["received_bytes"]] <- u[["received_bytes"]] + length(bytes)
  u[["received_count"]] <- u[["received_count"]] + 1L
  u[["last_used"]] <- Sys.time()

  touch_session(session)
  list(
    received_count = u[["received_count"]],
    received_bytes = u[["received_bytes"]],
    total_bytes = u[["total_bytes"]]
  )
}


#' Reassemble a chunked upload's bytes, and clear its pending state
#'
#' Shared by the `*.upload.end` handlers. `end_upload()` decodes the bytes as
#' Arrow IPC data, and `job.load.end` as an `.rds` file.
#'
#' @param session Session env.
#' @param upload_id Character scalar.
#'
#' @return Named list - `bytes` (raw vector, the concatenated upload) and
#'   `name` (the label `begin_upload` was given).
#'
#' @author EDG
#' @keywords internal
#' @noRd
assemble_chunked_upload <- function(session, upload_id) {
  check_wire_string(upload_id, "upload_id")
  pu <- pending_uploads(session)
  u <- pu[[upload_id]]
  if (is.null(u)) {
    rtemis.core::abort(
      "Unknown upload_id '",
      upload_id,
      "'. An unfinished upload is dropped ",
      .UPLOAD_TTL,
      " s after its last chunk, and when its connection leaves the session.",
      class = "rtemislive_not_found"
    )
  }
  on.exit(rm(list = upload_id, envir = pu), add = TRUE)

  if (u[["received_count"]] != u[["n_chunks"]]) {
    rtemis.core::abort(
      "Upload incomplete: received ",
      u[["received_count"]],
      " of ",
      u[["n_chunks"]],
      " chunks.",
      class = "rtemislive_invalid_params"
    )
  }
  if (u[["received_bytes"]] != u[["total_bytes"]]) {
    rtemis.core::abort(
      "Upload size mismatch: declared ",
      u[["total_bytes"]],
      " bytes, ",
      "received ",
      u[["received_bytes"]],
      ".",
      class = "rtemislive_invalid_params"
    )
  }

  list(bytes = do.call(c, u[["chunks"]]), name = u[["name"]])
}


#' Finalize a chunked upload
#'
#' Concatenates received chunks in order, decodes as Arrow IPC, and
#' registers a new data_handle. Removes the pending-upload state regardless
#' of success.
#'
#' @param session Session env.
#' @param upload_id Character scalar.
#' @param max_handles Integer: Per-session data_handle cap.
#'
#' @return Named list - wire-shaped data_handle summary.
#'
#' @author EDG
#' @keywords internal
#' @noRd
end_upload <- function(session, upload_id, max_handles = 16L) {
  assembled <- assemble_chunked_upload(session, upload_id)
  new_data_handle(
    session,
    assembled[["name"]],
    assembled[["bytes"]],
    max_handles = max_handles
  )
}


#' Cancel an in-progress chunked upload
#'
#' Idempotent - canceling an unknown upload is a no-op returning `FALSE`.
#'
#' @param session Session env.
#' @param upload_id Character scalar.
#'
#' @return Logical - `TRUE` if a pending upload was canceled.
#'
#' @author EDG
#' @keywords internal
#' @noRd
cancel_upload <- function(session, upload_id) {
  check_wire_string(upload_id, "upload_id")
  pu <- pending_uploads(session)
  if (!exists(upload_id, envir = pu, inherits = FALSE)) {
    return(FALSE)
  }
  rm(list = upload_id, envir = pu)
  touch_session(session)
  TRUE
}


#' Drop the unfinished uploads that a connection began
#'
#' @param session Session env.
#' @param connection_id Character: Connection id.
#'
#' @return Character vector of the upload ids dropped.
#'
#' @author EDG
#' @keywords internal
#' @noRd
drop_uploads <- function(session, connection_id) {
  pu <- pending_uploads(session)
  dropped <- Filter(
    function(id) identical(pu[[id]][["connection_id"]], connection_id),
    ls(pu)
  )
  rm(list = dropped, envir = pu)
  dropped
}


#' Drop the uploads that received no chunk for `ttl` seconds
#'
#' @param session Session env.
#' @param now POSIXct: Current time.
#' @param ttl Numeric: Seconds.
#'
#' @return Character vector of the upload ids dropped.
#'
#' @author EDG
#' @keywords internal
#' @noRd
gc_uploads <- function(session, now = Sys.time(), ttl = .UPLOAD_TTL) {
  pu <- pending_uploads(session)
  dropped <- Filter(
    function(id) difftime(now, pu[[id]][["last_used"]], units = "secs") > ttl,
    ls(pu)
  )
  rm(list = dropped, envir = pu)
  dropped
}


# %% Lookup / list / delete --------------------------------------------------

#' Get the data.table for a data_handle
#'
#' Bumps `last_used` so GC knows the handle is in active use.
#'
#' @param session Session env.
#' @param handle Character scalar.
#'
#' @return data.table.
#'
#' @author EDG
#' @keywords internal
#' @noRd
get_data <- function(session, handle) {
  check_wire_string(handle, "data_handle")
  h <- session[["data"]][[handle]]
  if (is.null(h)) {
    rtemis.core::abort(
      "Unknown data_handle '",
      handle,
      "'.",
      class = "rtemislive_not_found"
    )
  }
  h[["last_used"]] <- Sys.time()
  touch_session(session)
  h[["data"]]
}


#' Summarize a single data_handle (used in responses)
#'
#' @param h data_handle env.
#'
#' @return Named list.
#'
#' @author EDG
#' @keywords internal
#' @noRd
data_handle_summary <- function(h) {
  list(
    data_handle = h[["handle"]],
    name = h[["name"]],
    rows = h[["rows"]],
    cols = h[["cols"]]
  )
}


#' List all data_handles in a session
#'
#' @param session Session env.
#'
#' @return List of named lists - one summary per handle.
#'
#' @author EDG
#' @keywords internal
#' @noRd
list_data_handles <- function(session) {
  data_env <- session[["data"]]
  lapply(ls(data_env), function(handle) {
    data_handle_summary(data_env[[handle]])
  })
}


#' Delete a data_handle
#'
#' @param session Session env.
#' @param handle Character scalar.
#'
#' @return Logical - `TRUE` if a handle existed.
#'
#' @author EDG
#' @keywords internal
#' @noRd
delete_data <- function(session, handle) {
  data_env <- session[["data"]]
  if (
    !is.character(handle) ||
      length(handle) != 1L ||
      !exists(handle, envir = data_env, inherits = FALSE)
  ) {
    return(FALSE)
  }
  rm(list = handle, envir = data_env)
  touch_session(session)
  TRUE
}


# %% Describe ---------------------------------------------------------------

#' Describe a data_handle's columns
#'
#' Returns a per-column summary suitable for the `data.describe` response.
#' For each column: name, R type, n_unique, n_missing, plus a min/max
#' range for numeric columns or top-3 values for categorical columns.
#'
#' @param session Session env.
#' @param handle Character scalar.
#'
#' @return Named list - `rows`, `cols`, `columns` (list of per-column
#'   summaries).
#'
#' @author EDG
#' @keywords internal
#' @noRd
describe_data <- function(session, handle) {
  dt <- get_data(session, handle)
  cols <- lapply(names(dt), function(nm) {
    col <- dt[[nm]]
    list(
      name = nm,
      type = describe_col_type(col),
      n_missing = sum(is.na(col)),
      n_unique = data.table::uniqueN(col, na.rm = TRUE),
      summary = describe_col_summary(col)
    )
  })
  list(
    rows = nrow(dt),
    cols = ncol(dt),
    columns = cols
  )
}


#' Compact type label for a column
#'
#' @author EDG
#' @keywords internal
#' @noRd
describe_col_type <- function(x) {
  if (is.factor(x)) {
    return("factor")
  }
  if (is.logical(x)) {
    return("logical")
  }
  if (is.integer(x)) {
    return("integer")
  }
  if (is.double(x)) {
    return("double")
  }
  if (is.character(x)) {
    return("character")
  }
  if (inherits(x, "Date")) {
    return("date")
  }
  if (inherits(x, "POSIXct")) {
    return("datetime")
  }
  paste(class(x), collapse = "/")
}


#' Per-column summary (range for numerics, top values otherwise)
#'
#' @author EDG
#' @keywords internal
#' @noRd
describe_col_summary <- function(x) {
  if (is.numeric(x)) {
    if (all(is.na(x))) {
      return(list(min = NA_real_, max = NA_real_, mean = NA_real_))
    }
    list(
      min = as.numeric(min(x, na.rm = TRUE)),
      max = as.numeric(max(x, na.rm = TRUE)),
      mean = as.numeric(mean(x, na.rm = TRUE))
    )
  } else if (is.factor(x) || is.character(x)) {
    tab <- sort(table(x, useNA = "no"), decreasing = TRUE)
    top <- utils::head(tab, 3L)
    list(
      top = as.list(stats::setNames(as.integer(top), names(top)))
    )
  } else if (is.logical(x)) {
    list(
      n_true = sum(x, na.rm = TRUE),
      n_false = sum(!x, na.rm = TRUE)
    )
  } else {
    NULL
  }
}


# %% GC ----------------------------------------------------------------------

#' GC stale data_handles in a session
#'
#' Drops handles whose `last_used` is older than `ttl` seconds.
#'
#' @param session Session env.
#' @param now POSIXct.
#' @param ttl Numeric, seconds. (spec paragraph 6.3).
#'
#' @return Character vector of handles dropped.
#'
#' @author EDG
#' @keywords internal
#' @noRd
gc_data <- function(session, now = Sys.time(), ttl = 3600) {
  data_env <- session[["data"]]
  expired <- character(0L)
  for (handle in ls(data_env)) {
    h <- data_env[[handle]]
    if (difftime(now, h[["last_used"]], units = "secs") > ttl) {
      expired <- c(expired, handle)
    }
  }
  for (handle in expired) {
    rm(list = handle, envir = data_env)
  }
  expired
}
