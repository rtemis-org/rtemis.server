# dialog.R
# ::rtemis.server::
# 2026- EDG rtemis.org

# Native folder dialog of the server machine.
#
# The picker runs as a child process beside the event loop. It writes its
# process id, output, and error text to a directory of its own under
# `tempdir()`, and its exit status last, under a name that exists only once the
# status is complete. `poll_dialog()` runs in `host_tick()` and sends the reply.
# `server$dialog` is NULL or the open dialog: a server has one at a time.

# %% Constants ----
# Seconds after which an open dialog is closed. The client waits 300.
.DIALOG_SECONDS <- 290


# %% picker_command ----
#' Shell command of the folder picker on macOS and Linux
#'
#' The command prints the folder and exits with status 0.
#'
#' @param prompt Character: Text shown in the dialog.
#' @param os Character: Name of the operating system, from `Sys.info()`.
#'
#' @return Character scalar.
#'
#' @author EDG
#' @keywords internal
#' @noRd
picker_command <- function(prompt, os = Sys.info()[["sysname"]]) {
  program <- if (identical(os, "Darwin")) "osascript" else "zenity"
  path <- Sys.which(program)
  if (!nzchar(path)) {
    rtemis.core::abort(
      "The folder dialog requires the program '",
      program,
      "', which is not on the PATH of the server. Enter the folder instead.",
      class = "rtemislive_unavailable"
    )
  }
  args <- if (identical(os, "Darwin")) {
    c(
      "-e",
      sprintf(
        'POSIX path of (choose folder with prompt "%s")',
        gsub('[\\\\"[:cntrl:]]', "", prompt)
      )
    )
  } else {
    c("--file-selection", "--directory", paste0("--title=", prompt))
  }
  paste(shQuote(c(path, args), type = "sh"), collapse = " ")
}


# %% unix_dialog ----
#' Shell script that runs a picker and records its result
#'
#' @param command Character: Shell command of the picker.
#' @param dir Character: Directory of the dialog's files.
#'
#' @return Character scalar.
#'
#' @author EDG
#' @keywords internal
#' @noRd
unix_dialog <- function(command, dir) {
  file <- function(name) shQuote(file.path(dir, name), type = "sh")
  paste0(
    command,
    " > ",
    file("path"),
    " 2> ",
    file("error"),
    " & pid=$!; echo $pid > ",
    file("pid"),
    "; wait $pid; echo $? > ",
    file("partial"),
    " && mv ",
    file("partial"),
    " ",
    file("status")
  )
}


# %% windows_dialog ----
#' PowerShell script that runs a folder dialog and records its result
#'
#' @param prompt Character: Text shown in the dialog.
#' @param dir Character: Directory of the dialog's files.
#'
#' @return Character scalar.
#'
#' @author EDG
#' @keywords internal
#' @noRd
windows_dialog <- function(prompt, dir) {
  quoted <- function(x) paste0("'", gsub("'", "''", x, fixed = TRUE), "'")
  file <- function(name) quoted(file.path(dir, name))
  paste(
    sprintf("[IO.File]::WriteAllText(%s, \"$PID\")", file("pid")),
    "$status = 2",
    "try {",
    "Add-Type -AssemblyName System.Windows.Forms -ErrorAction Stop",
    "$dialog = New-Object System.Windows.Forms.FolderBrowserDialog",
    paste("$dialog.Description =", quoted(prompt)),
    "$owner = New-Object System.Windows.Forms.Form -Property @{TopMost=$true}",
    "$status = 1",
    "if ($dialog.ShowDialog($owner) -eq 'OK') {",
    sprintf("[IO.File]::WriteAllText(%s, $dialog.SelectedPath)", file("path")),
    "$status = 0",
    "}",
    "} catch {",
    "$status = 2",
    sprintf("[IO.File]::WriteAllText(%s, \"$_\")", file("error")),
    "}",
    sprintf("[IO.File]::WriteAllText(%s, \"$status\")", file("partial")),
    sprintf("Move-Item %s %s", file("partial"), file("status")),
    sep = "\n"
  )
}


# %% open_dialog ----
#' Open the folder dialog
#'
#' Returns as soon as the child process is started, and stores the dialog as
#' `server$dialog`.
#'
#' @param server Server env.
#' @param conn Connection env: Receives the reply.
#' @param req_id Character: Id of the request.
#' @param prompt Character: Text shown in the dialog.
#'
#' @return `NULL`, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
open_dialog <- function(server, conn, req_id, prompt) {
  if (!is.null(server[["dialog"]])) {
    rtemis.core::abort(
      "A folder dialog is already open on the server machine.",
      class = "rtemislive_busy"
    )
  }
  windows <- identical(.Platform[["OS.type"]], "windows")
  # Raises an error without a picker, before anything is created.
  command <- if (!windows) picker_command(prompt)
  dir <- normalizePath(tempfile("rtemislive-dialog-"), "/", mustWork = FALSE)
  dir.create(dir, mode = "0700")
  if (windows) {
    script <- iconv(windows_dialog(prompt, dir), "", "UTF-16LE", toRaw = TRUE)
    system2(
      "powershell",
      c(
        "-NoProfile",
        "-STA",
        "-EncodedCommand",
        openssl::base64_encode(script[[1L]])
      ),
      stdout = FALSE,
      stderr = FALSE,
      wait = FALSE
    )
  } else {
    system2(
      "/bin/sh",
      c("-c", shQuote(unix_dialog(command, dir))),
      stdout = FALSE,
      stderr = FALSE,
      wait = FALSE
    )
  }
  server[["dialog"]] <- list(
    conn = conn,
    req_id = req_id,
    dir = dir,
    started_at = Sys.time()
  )
  invisible(NULL)
}


# %% poll_dialog ----
#' Reply to the request of a dialog that ended
#'
#' A dialog that stays open for `.DIALOG_SECONDS` is closed, and its request
#' answered with an error.
#'
#' @param server Server env.
#' @param now POSIXct: Current time.
#'
#' @return Logical scalar: `TRUE` if a dialog ended.
#'
#' @author EDG
#' @keywords internal
#' @noRd
poll_dialog <- function(server, now = Sys.time()) {
  dialog <- server[["dialog"]]
  if (is.null(dialog)) {
    return(FALSE)
  }
  ended <- file.exists(file.path(dialog[["dir"]], "status"))
  if (
    !ended &&
      difftime(now, dialog[["started_at"]], units = "secs") < .DIALOG_SECONDS
  ) {
    return(FALSE)
  }
  response <- if (ended) {
    tryCatch(
      dialog_response(dialog),
      error = function(e) {
        make_error(dialog[["req_id"]], "internal_error", conditionMessage(e))
      }
    )
  } else {
    make_error(
      dialog[["req_id"]],
      "timeout",
      paste0("No folder was chosen within ", .DIALOG_SECONDS, " s.")
    )
  }
  # Before the write: a failed write disconnects, which closes the dialog of
  # the connection.
  close_dialog(server)
  if (!write_frame(dialog[["conn"]], encode_response(response))) {
    disconnect_connection(server, dialog[["conn"]])
  }
  TRUE
}


# %% dialog_response ----
#' Response of a dialog that ended
#'
#' osascript reports a canceled dialog with status 1 and error -128, and any
#' other failure with status 1 and another error. zenity has status 1 for a
#' canceled dialog and for a missing display.
#'
#' @param dialog List: The dialog, as `open_dialog()` stores it.
#'
#' @return Named list: Response of `make_response()` or `make_error()`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
dialog_response <- function(dialog) {
  read <- function(name) {
    file <- file.path(dialog[["dir"]], name)
    if (!file.exists(file)) {
      return("")
    }
    text <- rawToChar(readBin(file, "raw", n = file.size(file)))
    Encoding(text) <- "UTF-8"
    # The picker ends its output with a line break, which is not part of a path.
    sub("\r?\n$", "", text)
  }
  status <- suppressWarnings(as.integer(read("status")))
  path <- read("path")
  reason <- read("error")
  if (identical(status, 0L) && nzchar(path)) {
    return(make_response(
      dialog[["req_id"]],
      list(path = path, cancelled = FALSE)
    ))
  }
  canceled <- identical(status, 1L) &&
    (!nzchar(reason) ||
      grepl("(-128)", reason, fixed = TRUE) ||
      (!grepl("execution error", reason, fixed = TRUE) &&
        !grepl("display", reason, ignore.case = TRUE)))
  if (canceled) {
    return(make_response(dialog[["req_id"]], list(cancelled = TRUE)))
  }
  make_error(
    dialog[["req_id"]],
    "unavailable",
    paste0(
      "The folder dialog failed (status ",
      status,
      "). Enter the folder instead.",
      if (nzchar(reason)) paste0("\n", reason)
    )
  )
}


# %% close_dialog ----
#' Close the dialog of a server and remove its files
#'
#' Ends the picker if it runs. The status file marks a picker that ended, whose
#' process id may belong to another process.
#'
#' @param server Server env.
#' @param conn Optional Connection env: If not `NULL`, closes the dialog only
#'   if this connection opened it.
#'
#' @return `NULL`, invisibly.
#'
#' @author EDG
#' @keywords internal
#' @noRd
close_dialog <- function(server, conn = NULL) {
  dialog <- server[["dialog"]]
  if (
    is.null(dialog) || (!is.null(conn) && !identical(dialog[["conn"]], conn))
  ) {
    return(invisible(NULL))
  }
  server[["dialog"]] <- NULL
  file <- function(name) file.path(dialog[["dir"]], name)
  # A picker started less than a moment ago has no process id yet.
  deadline <- Sys.time() + 1
  while (
    !file.exists(file("status")) &&
      !isTRUE(file.size(file("pid")) > 0) &&
      Sys.time() < deadline
  ) {
    Sys.sleep(0.02)
  }
  if (!file.exists(file("status")) && file.exists(file("pid"))) {
    pid <- suppressWarnings(as.integer(readLines(
      file("pid"),
      n = 1L,
      warn = FALSE
    )))
    if (length(pid) == 1L && !is.na(pid)) {
      tools::pskill(pid)
    }
  }
  unlink(dialog[["dir"]], recursive = TRUE)
  invisible(NULL)
}


# %% handle_choose_dir ----
#' `dialog.choose_dir` handler
#'
#' Opens a native folder dialog on the server machine. The reply, the chosen
#' path or `cancelled = TRUE`, follows when the dialog ends; `poll_dialog()`
#' sends it.
#'
#' @param conn,frame,server Standard handler triple.
#'
#' @return `NULL`: no immediate response.
#'
#' @author EDG
#' @keywords internal
#' @noRd
handle_choose_dir <- function(conn, frame, server) {
  req_id <- frame[["header"]][["id"]] %||% NA_character_
  params <- frame[["header"]][["params"]] %||% list()
  prompt <- params[["prompt"]] %||% "Select a folder"
  check_wire_string(prompt, "prompt")
  open_dialog(server, conn, req_id, prompt)
  NULL
}
