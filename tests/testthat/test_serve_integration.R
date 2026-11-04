# test_serve_integration.R
# ::rtemis.server::
# 2026- EDG rtemis.org
#
# Lifecycle smoke test for `serve()`.
#
# The test starts `serve()` in a separate `Rscript` process, checks that the
# server binds its TCP port with a plain socket connection, and stops the
# process by PID with `tools::pskill()`. nanonext's `stream()` speaks
# NNG-over-WebSocket, a framing the `handler_ws` endpoint does not accept, so
# this file does not exchange WebSocket frames. The dispatcher tests in this
# directory exercise every handler with synthetic frames.

skip_on_cran()


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
  pid_file <- tempfile("serve-pid-")
  err_file <- tempfile("serve-stderr-")
  script <- tempfile("serve-", fileext = ".R")
  writeLines(
    c(
      paste0("writeLines(as.character(Sys.getpid()), ", deparse(pid_file), ")"),
      "library(rtemis)",
      paste0(
        "rtemis.server::serve(port = ",
        port,
        "L, token = \"smoke-test-toke-9999\", n_daemons = 1L, verbosity = 0L)"
      )
    ),
    script
  )
  on.exit(unlink(c(pid_file, err_file, script)), add = TRUE)

  system2(
    file.path(R.home("bin"), "Rscript"),
    shQuote(script),
    stdout = FALSE,
    stderr = err_file,
    wait = FALSE
  )
  started <- wait_until(
    function() file.exists(pid_file) && length(readLines(pid_file)) == 1L,
    timeout = 15
  )
  expect_false(is.na(started), info = "Server process did not start.")
  if (!is.na(started)) {
    pid <- as.integer(readLines(pid_file))
    on.exit(tools::pskill(pid, tools::SIGTERM), add = TRUE, after = FALSE)
  }

  elapsed <- wait_until(function() can_connect(port), timeout = 15)
  expect_false(
    is.na(elapsed),
    info = paste0(
      "Server did not bind on port ",
      port,
      "\nstderr:\n",
      paste(readLines(err_file, warn = FALSE), collapse = "\n")
    )
  )

  # The port still accepts connections half a second after binding.
  Sys.sleep(0.5)
  expect_true(can_connect(port), info = "Server died shortly after start.")
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
