# test_sessions.R
# ::rtemis::
# 2026- EDG rtemis.org

# Each test starts from a clean registry.
setup_clean_registry <- function() {
  clear_sessions()
}


# new_session_id ----
test_that("new_session_id() returns sess-<hex16>", {
  id <- new_session_id()
  expect_match(id, "^sess-[0-9a-f]{16}$")
})

test_that("new_session_id() values are distinct", {
  ids <- replicate(50L, new_session_id())
  expect_equal(length(unique(ids)), 50L)
})


# validate_session_name ----
test_that("validate_session_name() accepts allowed names", {
  expect_silent(validate_session_name("iris-grid"))
  expect_silent(validate_session_name("a"))
  expect_silent(validate_session_name(paste(rep("x", 64L), collapse = "")))
  expect_silent(validate_session_name("Foo_bar.baz-1"))
})

test_that("validate_session_name() rejects empty / too-long / bad-char names", {
  expect_error(validate_session_name(""), class = "rtemislive_invalid_name")
  expect_error(
    validate_session_name(paste(rep("x", 65L), collapse = "")),
    class = "rtemislive_invalid_name"
  )
  expect_error(
    validate_session_name("has spaces"),
    class = "rtemislive_invalid_name"
  )
  expect_error(
    validate_session_name("has/slash"),
    class = "rtemislive_invalid_name"
  )
})

test_that("validate_session_name() rejects non-character / NA / wrong length", {
  expect_error(
    validate_session_name(NA_character_),
    class = "rtemislive_invalid_name"
  )
  expect_error(validate_session_name(123L), class = "rtemislive_invalid_name")
  expect_error(
    validate_session_name(c("a", "b")),
    class = "rtemislive_invalid_name"
  )
})


# new_session ----
test_that("new_session() registers a session and returns it", {
  setup_clean_registry()
  on.exit(clear_sessions(), add = TRUE)

  s <- new_session("iris-grid")
  expect_match(s[["id"]], "^sess-")
  expect_equal(s[["name"]], "iris-grid")
  expect_s3_class(s[["created_at"]], "POSIXct")
  expect_s3_class(s[["last_seen"]], "POSIXct")
  expect_equal(s[["connections"]], character(0L))
  expect_equal(s[["event_buffer"]], list())
  expect_equal(s[["events_dropped"]], 0L)
  expect_true(is.environment(s[["jobs"]]))
  expect_true(is.environment(s[["data"]]))

  expect_identical(get_session_by_id(s[["id"]]), s)
  expect_identical(get_session_by_name("iris-grid"), s)
})

test_that("new_session() generates untitled-<n> when no name given", {
  setup_clean_registry()
  on.exit(clear_sessions(), add = TRUE)

  s1 <- new_session()
  s2 <- new_session()
  s3 <- new_session()
  expect_equal(s1[["name"]], "untitled-1")
  expect_equal(s2[["name"]], "untitled-2")
  expect_equal(s3[["name"]], "untitled-3")
})

test_that("new_session() reuses freed `untitled-<n>` slots", {
  setup_clean_registry()
  on.exit(clear_sessions(), add = TRUE)

  s1 <- new_session()
  s2 <- new_session()
  delete_session(s1[["id"]])
  s3 <- new_session()
  expect_equal(s3[["name"]], "untitled-1")
})

test_that("new_session() rejects duplicate names with rtemislive_session_exists", {
  setup_clean_registry()
  on.exit(clear_sessions(), add = TRUE)

  new_session("iris-grid")
  expect_error(
    new_session("iris-grid"),
    class = "rtemislive_session_exists"
  )
})

test_that("new_session() respects max_sessions cap", {
  setup_clean_registry()
  on.exit(clear_sessions(), add = TRUE)

  for (i in 1:3) {
    new_session(max_sessions = 3L)
  }
  expect_error(
    new_session(max_sessions = 3L),
    class = "rtemislive_too_many_sessions"
  )
})


# Lookup ----
test_that("get_session_by_id / by_name return NULL for missing", {
  setup_clean_registry()
  expect_null(get_session_by_id("sess-nope"))
  expect_null(get_session_by_name("nope"))
  expect_null(get_session_by_id(NA_character_))
  expect_null(get_session_by_id(123L))
})

test_that("get_session() looks up by id or name", {
  setup_clean_registry()
  on.exit(clear_sessions(), add = TRUE)

  s <- new_session("foo")
  expect_identical(get_session(s[["id"]]), s)
  expect_identical(get_session("foo"), s)
  expect_null(get_session("nope"))
})


# Connection attach/detach ----
test_that("attach_connection() adds and is idempotent", {
  setup_clean_registry()
  on.exit(clear_sessions(), add = TRUE)

  s <- new_session("foo")
  attach_connection(s, "c-1")
  expect_equal(s[["connections"]], "c-1")
  attach_connection(s, "c-1")
  expect_equal(s[["connections"]], "c-1")
  attach_connection(s, "c-2")
  expect_setequal(s[["connections"]], c("c-1", "c-2"))
})

test_that("detach_connection() removes by id", {
  setup_clean_registry()
  on.exit(clear_sessions(), add = TRUE)

  s <- new_session("foo")
  attach_connection(s, "c-1")
  attach_connection(s, "c-2")
  detach_connection(s, "c-1")
  expect_equal(s[["connections"]], "c-2")
})

test_that("attach/detach update last_seen", {
  setup_clean_registry()
  on.exit(clear_sessions(), add = TRUE)

  s <- new_session("foo")
  s[["last_seen"]] <- Sys.time() - 60
  ts_before <- s[["last_seen"]]
  attach_connection(s, "c-1")
  expect_gt(as.numeric(s[["last_seen"]]), as.numeric(ts_before))
})


# Rename ----
test_that("rename_session() updates name", {
  setup_clean_registry()
  on.exit(clear_sessions(), add = TRUE)

  s <- new_session("foo")
  rename_session(s, "bar")
  expect_equal(s[["name"]], "bar")
  expect_identical(get_session_by_name("bar"), s)
  expect_null(get_session_by_name("foo"))
})

test_that("rename_session() to same name is a no-op", {
  setup_clean_registry()
  on.exit(clear_sessions(), add = TRUE)

  s <- new_session("foo")
  ts_before <- s[["last_seen"]]
  Sys.sleep(0.01) # ensure measurable elapsed time
  rename_session(s, "foo")
  expect_equal(s[["last_seen"]], ts_before)
})

test_that("rename_session() rejects collisions", {
  setup_clean_registry()
  on.exit(clear_sessions(), add = TRUE)

  s1 <- new_session("foo")
  new_session("bar")
  expect_error(rename_session(s1, "bar"), class = "rtemislive_session_exists")
})


# delete_session / gc ----
test_that("delete_session() removes by id and returns TRUE / FALSE", {
  setup_clean_registry()
  on.exit(clear_sessions(), add = TRUE)

  s <- new_session("foo")
  expect_true(delete_session(s[["id"]]))
  expect_null(get_session_by_id(s[["id"]]))
  expect_false(delete_session(s[["id"]]))
  expect_false(delete_session("sess-bogus"))
})

test_that("gc_sessions() collects sessions with 0 connections past TTL", {
  setup_clean_registry()
  on.exit(clear_sessions(), add = TRUE)

  old <- new_session("old")
  young <- new_session("young")
  active <- new_session("active")
  attach_connection(active, "c-1")

  # Force `old` past TTL; leave `young` and `active` recent.
  old[["last_seen"]] <- Sys.time() - 1000
  active[["last_seen"]] <- Sys.time() - 1000 # would be expired, but has conn

  expired <- gc_sessions(ttl = 500)
  expect_equal(expired, old[["id"]])
  expect_null(get_session_by_id(old[["id"]]))
  expect_identical(get_session_by_id(young[["id"]]), young)
  expect_identical(get_session_by_id(active[["id"]]), active)
})


# Event buffering ----
test_that("push_event() returns FALSE when connections attached", {
  setup_clean_registry()
  on.exit(clear_sessions(), add = TRUE)

  s <- new_session("foo")
  attach_connection(s, "c-1")
  expect_false(push_event(s, list(event = "ping")))
  expect_length(s[["event_buffer"]], 0L)
})

test_that("push_event() buffers when no connections attached", {
  setup_clean_registry()
  on.exit(clear_sessions(), add = TRUE)

  s <- new_session("foo")
  expect_true(push_event(s, list(event = "a")))
  expect_true(push_event(s, list(event = "b")))
  expect_length(s[["event_buffer"]], 2L)
})

test_that("push_event() drops oldest when buffer overflows", {
  setup_clean_registry()
  on.exit(clear_sessions(), add = TRUE)

  s <- new_session("foo", max_buffer = 3L)
  for (i in 1:5) {
    push_event(s, list(event = paste0("e", i)))
  }
  expect_length(s[["event_buffer"]], 3L)
  expect_equal(s[["events_dropped"]], 2L)
  # Oldest two dropped; newest three remain
  evs <- vapply(s[["event_buffer"]], `[[`, character(1L), "event")
  expect_equal(evs, c("e3", "e4", "e5"))
})

test_that("drain_event_buffer() returns events + dropped, then resets", {
  setup_clean_registry()
  on.exit(clear_sessions(), add = TRUE)

  s <- new_session("foo", max_buffer = 2L)
  for (i in 1:3) {
    push_event(s, list(event = paste0("e", i)))
  }
  out <- drain_event_buffer(s)
  expect_length(out[["events"]], 2L)
  expect_equal(out[["dropped"]], 1L)
  expect_length(s[["event_buffer"]], 0L)
  expect_equal(s[["events_dropped"]], 0L)
})


# Wire views ----
test_that("list_sessions() summarizes every session", {
  setup_clean_registry()
  on.exit(clear_sessions(), add = TRUE)

  s1 <- new_session("a")
  s2 <- new_session("b")
  attach_connection(s2, "c-1")

  out <- list_sessions()
  expect_length(out, 2L)
  names <- vapply(out, `[[`, character(1L), "name")
  expect_setequal(names, c("a", "b"))

  # Locate `b` entry and check connection count
  b <- Filter(function(x) x[["name"]] == "b", out)[[1L]]
  expect_equal(b[["n_connections"]], 1L)
  expect_equal(b[["n_jobs"]], 0L)
})

test_that("session_snapshot() returns identity + (empty) job/data views", {
  setup_clean_registry()
  on.exit(clear_sessions(), add = TRUE)

  s <- new_session("foo")
  attach_connection(s, "c-1")
  snap <- session_snapshot(s)
  expect_equal(snap[["name"]], "foo")
  expect_equal(snap[["session_id"]], s[["id"]])
  expect_equal(snap[["n_connections"]], 1L)
  expect_equal(snap[["jobs"]], list())
  expect_equal(snap[["data"]], list())
  expect_match(snap[["created"]], "^[0-9]{4}-[0-9]{2}-[0-9]{2}T")
})


# Clear ----
test_that("clear_sessions() empties the registry", {
  on.exit(clear_sessions(), add = TRUE)
  new_session("a")
  new_session("b")
  clear_sessions()
  expect_length(ls(session_registry()), 0L)
})
