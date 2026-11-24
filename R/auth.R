# auth.R
# ::rtemis.server::
# 2026- EDG rtemis.org

# Authentication and origin checking for the rtemislive WebSocket server.
# spec: rtemis.server/lifecycle#authentication

# Failed `auth` attempts that close a connection.
.RTEMISLIVE_AUTH_ATTEMPTS <- 3L

# Largest number of bytes of a request id, which every response repeats.
.RTEMISLIVE_ID_MAX <- 128L

# Seconds a connection may stay unauthenticated.
.RTEMISLIVE_AUTH_TIMEOUT <- 10

# Smallest number of characters of a supplied token.
.RTEMISLIVE_TOKEN_MIN <- 16L

# Default origins.
.RTEMISLIVE_DEFAULT_ORIGINS <- c(
  "https://live.rtemis.org",
  "http://localhost:3000",
  "http://127.0.0.1:3000"
)


# %% generate_token ----
#' Generate a connection token
#'
#' Produces an 8-byte cryptographically random value, formatted as four
#' lowercase hex groups of four characters separated by dashes - e.g.
#' `7f3a-9c2b-d4e1-5a8f`. 64 bits of entropy is ample for a local-only,
#' single-user, server-lifetime-bound token, and the form is short enough
#' to read aloud and paste comfortably.
#'
#' @return Character scalar.
#'
#' @author EDG
#' @keywords internal
#' @noRd
generate_token <- function() {
  rtemis.core::check_dependencies("openssl")
  bytes <- openssl::rand_bytes(8L)
  hex <- paste0(sprintf("%02x", as.integer(bytes)), collapse = "")
  # Group into four 4-char blocks: `xxxx-xxxx-xxxx-xxxx`
  paste(substring(hex, c(1L, 5L, 9L, 13L), c(4L, 8L, 12L, 16L)), collapse = "-")
}


# %% check_token ----
#' Constant-time token comparison
#'
#' Compares two character scalars in constant time to mitigate timing
#' side-channel attacks during `auth` validation.
#'
#' An `NA` or empty string on either side is a mismatch. `charToRaw()` reads
#' `NA_character_` as the bytes of "NA".
#'
#' @param presented Character scalar: Token submitted by the client.
#' @param expected Character scalar: The server's current token.
#'
#' @return Logical scalar.
#'
#' @author EDG
#' @keywords internal
#' @noRd
check_token <- function(presented, expected) {
  for (x in list(presented, expected)) {
    if (!is.character(x) || length(x) != 1L || is.na(x) || !nzchar(x)) {
      return(FALSE)
    }
  }
  a <- charToRaw(presented)
  b <- charToRaw(expected)
  # Always XOR over the longer of the two; pad the shorter with arbitrary
  # bytes so the comparison time is independent of input length up to that
  # bound. Mismatched lengths still reject.
  na <- length(a)
  nb <- length(b)
  n <- max(na, nb)
  if (na < n) {
    a <- c(a, raw(n - na))
  }
  if (nb < n) {
    b <- c(b, raw(n - nb))
  }
  diff <- as.integer(xor(a, b))
  identical(na, nb) && sum(diff) == 0L
}


# %% check_supplied_token ----
#' Check a token supplied by the caller of `serve()`
#'
#' @param token Character: Token.
#'
#' @return `token`, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
check_supplied_token <- function(token) {
  # `\z` is the end of the string; `$` also matches before a final newline.
  if (
    !is.character(token) ||
      length(token) != 1L ||
      is.na(token) ||
      !grepl("^[!-~]+\\z", token, perl = TRUE, useBytes = TRUE) ||
      nchar(token, type = "bytes") < .RTEMISLIVE_TOKEN_MIN
  ) {
    rtemis.core::abort(
      "`token` must be a single string of at least ",
      .RTEMISLIVE_TOKEN_MIN,
      " printable ASCII characters without spaces."
    )
  }
  invisible(token)
}


# %% request_header ----
#' Read a header of the WebSocket upgrade request
#'
#' Header names are case-insensitive (RFC 9110), and nanonext keeps the case
#' the client sent.
#'
#' @param headers Named character vector: Request headers.
#' @param name Character: Header name.
#'
#' @return Character scalar, or `NULL` if the header is absent.
#'
#' @author EDG
#' @keywords internal
#' @noRd
request_header <- function(headers, name) {
  i <- match(tolower(name), tolower(names(headers)))
  if (is.na(i)) {
    return(NULL)
  }
  headers[[i]]
}


# %% check_origin ----
#' Check WebSocket Origin against an allowlist
#'
#' Called when a WS upgrade is received. The `Origin` header (string) is
#' compared exactly against `allowed_origins`. Anything else is rejected.
#'
#' Loopback variants (`http://localhost`, `http://127.0.0.1`) are matched
#' on the host:port pair without requiring scheme equality with `https://`
#' equivalents - `http://localhost:3000` matches the literal string only,
#' since this is a local-only app and we don't want to silently accept
#' `https://localhost:3000` from a different listener.
#'
#' @param origin Character scalar: Value of the `Origin` HTTP header.
#' @param allowed_origins Character vector: Allowed origins.
#'
#' @return Logical scalar.
#'
#' @author EDG
#' @keywords internal
#' @noRd
check_origin <- function(
  origin,
  allowed_origins = .RTEMISLIVE_DEFAULT_ORIGINS
) {
  if (is.null(origin) || length(origin) == 0L || !is.character(origin)) {
    return(FALSE)
  }
  if (length(origin) != 1L || is.na(origin) || !nzchar(origin)) {
    return(FALSE)
  }
  if (!is.character(allowed_origins) || length(allowed_origins) == 0L) {
    return(FALSE)
  }
  origin %in% allowed_origins
}


# %% normalize_origins ----
#' Normalize a user-supplied origins vector
#'
#' Strips trailing slashes and whitespace. Validates that each entry is a
#' single non-empty string. Throws on malformed input.
#'
#' @param origins Optional character vector.
#'
#' @return Character vector (possibly the spec defaults if `origins` was
#' NULL).
#'
#' @author EDG
#' @keywords internal
#' @noRd
normalize_origins <- function(origins) {
  if (is.null(origins)) {
    return(.RTEMISLIVE_DEFAULT_ORIGINS)
  }
  if (!is.character(origins)) {
    rtemis.core::abort("`origins` must be a character vector or NULL.")
  }
  if (any(is.na(origins)) || any(!nzchar(origins))) {
    rtemis.core::abort("`origins` must not contain NA or empty strings.")
  }
  sub("/+$", "", trimws(origins))
}
