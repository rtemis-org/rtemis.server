# test_serve_integration.R
# ::rtemis::
# 2026- EDG rtemis.org
#
# Lifecycle smoke test for `serve()`.
#
# Full wire-protocol exercising the WebSocket layer requires a browser-
# compatible WS client. nanonext's `stream()` speaks NNG-over-WebSocket,
# which is NOT browser-compatible and cannot interoperate with our
# `handler_ws` endpoint. The right tool for that test is the
# `websocket` R package (not in rtemis's deps); the corresponding test
# is sketched in `__dev/` and is part of the manual QA pass.
#
# What this file verifies:
#
# - `serve()` starts in a fresh R subprocess without
#   crashing.
# - The server binds its TCP port (we open a plain socket to it).
# - The subprocess stays alive long enough to handle the smoke.
#
# Full request/response coverage is provided by the dispatcher tests in
# this directory, which exercise every handler with synthetic frames.

skip_on_cran()
skip_if_not_installed("callr")


# Helpers --------------------------------------------------------------------

pick_port <- function() {
  1024L + sample.int(20000L, 1L)
}

# Poll for `pred()` to become TRUE; return the elapsed seconds if so,
# or `NA_real_` on timeout.
wait_until <- function(pred, timeout = 10, interval = 0.05) {
  start <- Sys.time()
  repeat {
    if (isTRUE(pred())) {
      return(as.numeric(difftime(Sys.time(), start, units = "secs")))
    }
    if (as.numeric(difftime(Sys.time(), start, units = "secs")) > timeout) {
      return(NA_real_)
    }
    Sys.sleep(interval)
  }
}

can_connect <- function(port, timeout_s = 0.5) {
  con <- tryCatch(
    suppressWarnings(
      socketConnection(
        host = "127.0.0.1",
        port = port,
        blocking = FALSE,
        open = "r+",
        timeout = timeout_s
      )
    ),
    error = function(e) NULL,
    warning = function(w) NULL
  )
  if (is.null(con)) {
    return(FALSE)
  }
  close(con)
  TRUE
}


# Smoke: server starts, binds, stays alive ----

test_that("serve starts in a subprocess and binds its port", {
  port <- pick_port()

  bg <- callr::r_bg(
    function(port) {
      library(rtemis)
      rtemis.server::serve(
        port = port,
        token = "smoke-test-toke-9999",
        n_daemons = 1L,
        verbosity = 0L
      )
    },
    args = list(port = port),
    supervise = TRUE
  )
  on.exit(
    {
      tryCatch(bg$kill(), error = function(e) NULL)
    },
    add = TRUE
  )

  elapsed <- wait_until(function() can_connect(port), timeout = 15)
  expect_false(
    is.na(elapsed),
    info = paste0(
      "Server did not bind on port ",
      port,
      "\nstderr:\n",
      paste(bg$read_all_error(), collapse = "\n")
    )
  )
  expect_true(bg$is_alive(), info = "Server crashed after binding.")

  # Confirm the subprocess is still healthy ~half a second later
  Sys.sleep(0.5)
  expect_true(bg$is_alive(), info = "Server died shortly after start.")
})


# Smoke: stop signal cleans up ----

test_that("shutdown sets the flag (in-process check)", {
  s <- new_server_state(token = "x")
  expect_false(s[["stop_requested"]])
  shutdown(s)
  expect_true(s[["stop_requested"]])
})

test_that("shutdown rejects non-env input", {
  expect_error(shutdown("nope"))
  expect_error(shutdown(NULL))
})
