# test_dialog.R
# ::rtemis.server::
# 2026- EDG rtemis.org
#
# The folder dialog. A shell command stands in for the picker.

skip_on_cran()
skip_on_os("windows")


# Helpers ----
# A connection that records the frames it is sent.
recording_conn <- function() {
  sent <- new.env(parent = emptyenv())
  sent[["frames"]] <- list()
  conn <- new_connection(send_raw = function(bytes) {
    sent[["frames"]] <- c(sent[["frames"]], list(decode_frame(bytes)))
    0L
  })
  conn[["authed"]] <- TRUE
  list(conn = conn, sent = sent)
}

# Open a dialog whose picker is `command`.
open_fake <- function(server, conn, command, id = "r1") {
  local_mocked_bindings(picker_command = function(...) command)
  open_dialog(server, conn, id, "Select")
}

# Poll until the dialog ends; FALSE after 5 s.
wait_dialog <- function(server) {
  deadline <- Sys.time() + 5
  while (Sys.time() < deadline) {
    if (poll_dialog(server)) {
      return(TRUE)
    }
    Sys.sleep(0.02)
  }
  FALSE
}

reply <- function(rec) rec[["sent"]][["frames"]][[1L]][["header"]]

choose_dir_request <- list(
  header = list(v = 1L, id = "r2", method = "dialog.choose_dir"),
  payload = NULL
)

# Is a process running?
running <- function(pid) isTRUE(tools::pskill(pid, 0L))


# Tests ----
test_that("the chosen folder is the reply, with spaces and UTF-8", {
  server <- new_server_state()
  rec <- recording_conn()
  path <- "/tmp/a b \u00e9 "
  open_fake(server, rec[["conn"]], paste0("printf '%s\\n' '", path, "'"))
  expect_false(is.null(server[["dialog"]]))
  dir <- server[["dialog"]][["dir"]]
  expect_true(wait_dialog(server))
  expect_length(rec[["sent"]][["frames"]], 1L)
  expect_equal(reply(rec)[["id"]], "r1")
  expect_equal(reply(rec)[["result"]][["path"]], path)
  expect_false(reply(rec)[["result"]][["cancelled"]])
  expect_null(server[["dialog"]])
  expect_false(dir.exists(dir))
})

test_that("the event loop is free while a dialog is open", {
  server <- new_server_state()
  rec <- recording_conn()
  elapsed <- system.time({
    open_fake(server, rec[["conn"]], "sleep 30")
    expect_false(poll_dialog(server))
  })[["elapsed"]]
  expect_lt(elapsed, 2)
  close_dialog(server)
})

test_that("a canceled dialog and a failed one have different replies", {
  outcome <- function(command) {
    server <- new_server_state()
    rec <- recording_conn()
    open_fake(server, rec[["conn"]], command)
    expect_true(wait_dialog(server))
    reply(rec)
  }
  sh <- function(text, status) {
    paste("sh -c", shQuote(paste0("echo '", text, "' >&2; exit ", status)))
  }
  canceled <- outcome(sh("execution error: User canceled. (-128)", 1L))
  expect_true(canceled[["result"]][["cancelled"]])
  expect_true(outcome("false")[["result"]][["cancelled"]])
  expect_true(outcome(sh("Gtk-WARNING: theme", 1L))[["result"]][["cancelled"]])
  for (text in c(
    "execution error: No user interaction allowed. (-1713)",
    "cannot open display: "
  )) {
    failed <- outcome(sh(text, 1L))
    expect_false(failed[["ok"]])
    expect_equal(failed[["error"]][["code"]], "unavailable")
    expect_match(failed[["error"]][["message"]], text, fixed = TRUE)
  }
})

test_that("a second dialog is refused while one is open", {
  server <- new_server_state()
  rec <- recording_conn()
  open_fake(server, rec[["conn"]], "sleep 30")
  on.exit(close_dialog(server), add = TRUE)
  response <- dispatch_request(
    rec[["conn"]],
    choose_dir_request,
    server
  )
  expect_equal(response[["error"]][["code"]], "busy")
})

test_that("a missing picker is an error", {
  server <- new_server_state()
  rec <- recording_conn()
  local_mocked_bindings(picker_command = function(...) {
    rtemis.core::abort("No picker.", class = "rtemislive_unavailable")
  })
  response <- dispatch_request(
    rec[["conn"]],
    choose_dir_request,
    server
  )
  expect_equal(response[["error"]][["code"]], "unavailable")
  expect_null(server[["dialog"]])
})

test_that("picker_command() names the program it misses, and quotes its prompt", {
  withr_path <- Sys.getenv("PATH")
  on.exit(Sys.setenv(PATH = withr_path), add = TRUE)
  bin <- tempfile("bin-")
  dir.create(bin)
  on.exit(unlink(bin, recursive = TRUE), add = TRUE)
  Sys.setenv(PATH = bin)
  for (os in c("Darwin", "Linux")) {
    expect_error(
      picker_command("Select", os = os),
      class = "rtemislive_unavailable"
    )
  }
  for (program in c("osascript", "zenity")) {
    file <- file.path(bin, program)
    writeLines(c("#!/bin/sh", "for a in \"$@\"; do echo \"$a\"; done"), file)
    Sys.chmod(file, "0755")
  }
  prompt <- "It's \"here\" $(touch /tmp/x) `id`"
  out <- function(os) {
    system(picker_command(prompt, os = os), intern = TRUE)
  }
  expect_equal(out("Linux")[[3L]], paste0("--title=", prompt))
  expect_equal(
    out("Darwin")[[2L]],
    paste0(
      "POSIX path of (choose folder with prompt \"",
      gsub("\"", "", prompt),
      "\")"
    )
  )
})

test_that("closing the connection ends the picker and sends nothing", {
  server <- new_server_state()
  rec <- recording_conn()
  other <- recording_conn()
  register_connection(server, rec[["conn"]])
  register_connection(server, other[["conn"]])
  open_fake(server, rec[["conn"]], "sleep 30")
  dir <- server[["dialog"]][["dir"]]
  disconnect_connection(server, other[["conn"]])
  expect_false(is.null(server[["dialog"]]))
  while (!isTRUE(file.size(file.path(dir, "pid")) > 0)) {
    Sys.sleep(0.02)
  }
  pid <- as.integer(readLines(file.path(dir, "pid")))
  expect_true(running(pid))
  disconnect_connection(server, rec[["conn"]])
  expect_null(server[["dialog"]])
  expect_false(dir.exists(dir))
  deadline <- Sys.time() + 2
  while (running(pid) && Sys.time() < deadline) {
    Sys.sleep(0.02)
  }
  expect_false(running(pid))
  expect_length(rec[["sent"]][["frames"]], 0L)
})

test_that("a dialog closed right after it opens leaves no picker", {
  server <- new_server_state()
  rec <- recording_conn()
  marker <- tempfile("marker-")
  open_fake(
    server,
    rec[["conn"]],
    paste("sh -c", shQuote(paste("sleep 1; touch", marker)))
  )
  close_dialog(server)
  Sys.sleep(1.5)
  expect_false(file.exists(marker))
})

test_that("stop_server() ends an open dialog", {
  server <- new_server_state()
  rec <- recording_conn()
  open_fake(server, rec[["conn"]], "sleep 30")
  dir <- server[["dialog"]][["dir"]]
  stop_server(server)
  expect_null(server[["dialog"]])
  expect_false(dir.exists(dir))
})

test_that("a dialog that stays open is closed with an error", {
  server <- new_server_state()
  rec <- recording_conn()
  open_fake(server, rec[["conn"]], "sleep 30")
  expect_true(poll_dialog(server, now = Sys.time() + .DIALOG_SECONDS + 1))
  expect_equal(reply(rec)[["error"]][["code"]], "timeout")
  expect_null(server[["dialog"]])
})

test_that("windows_dialog() quotes the prompt and the paths", {
  script <- windows_dialog("It's here", "C:/Temp/it's")
  expect_match(script, "'It''s here'", fixed = TRUE)
  expect_match(script, "'C:/Temp/it''s/status'", fixed = TRUE)
})
