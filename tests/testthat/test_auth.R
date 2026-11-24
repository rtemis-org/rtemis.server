# test_auth.R
# ::rtemis::
# 2026- EDG rtemis.org

# generate_token ----
test_that("generate_token() returns 4-group hex string of expected shape", {
  tok <- generate_token()
  expect_type(tok, "character")
  expect_length(tok, 1L)
  expect_match(tok, "^[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}$")
})

test_that("generate_token() returns distinct values across calls", {
  toks <- replicate(20L, generate_token())
  expect_equal(length(unique(toks)), 20L)
})


# check_token ----
test_that("check_token() returns TRUE for matching strings", {
  tok <- generate_token()
  expect_true(check_token(tok, tok))
})

test_that("check_token() returns FALSE for mismatched strings", {
  expect_false(check_token("abcd-efgh", "abcd-efgi"))
  expect_false(check_token("short", "longer-string"))
})

test_that("check_token() rejects non-character or wrong-length input", {
  expect_false(check_token(123L, "abc"))
  expect_false(check_token("abc", NULL))
  expect_false(check_token(c("a", "b"), "a"))
  expect_false(check_token(NA_character_, "a"))
  # `charToRaw()` reads `NA_character_` as the bytes of "NA".
  expect_false(check_token("NA", NA_character_))
  expect_false(check_token(NA_character_, "NA"))
  expect_false(check_token("", ""))
  expect_false(check_token(NULL, "a"))
})


# check_supplied_token ----
test_that("check_supplied_token() requires 16 printable characters", {
  expect_invisible(check_supplied_token("abcd-1234-ef56-7890"))
  expect_invisible(check_supplied_token(strrep("a", 16L)))
  for (token in list(
    strrep("a", 15L),
    "",
    NA_character_,
    "sixteen chars, but spaces",
    "caf\u00e9-caf\u00e9-caf\u00e9-caf\u00e9",
    paste0(strrep("a", 16L), "\n"),
    c("abcd-1234-ef56-7890", "abcd-1234-ef56-7890"),
    1234567890123456
  )) {
    expect_error(check_supplied_token(token), "at least 16")
  }
})


# request_header ----
test_that("request_header() matches names regardless of case", {
  headers <- c(Host = "127.0.0.1", origin = "https://live.rtemis.org")
  expect_identical(request_header(headers, "Origin"), "https://live.rtemis.org")
  expect_null(request_header(headers, "Cookie"))
  expect_null(request_header(character(0L), "Origin"))
})


# check_origin ----
test_that("check_origin() accepts allowed origins", {
  expect_true(check_origin("https://live.rtemis.org"))
  expect_true(check_origin("http://localhost:3000"))
  expect_true(check_origin("http://127.0.0.1:3000"))
})

test_that("check_origin() rejects unknown origins", {
  expect_false(check_origin("https://evil.example.com"))
  expect_false(check_origin("file://"))
  expect_false(check_origin("http://localhost:9999"))
})

test_that("check_origin() rejects empty/NA/NULL origin", {
  expect_false(check_origin(NULL))
  expect_false(check_origin(""))
  expect_false(check_origin(NA_character_))
  expect_false(check_origin(character(0L)))
})

test_that("check_origin() respects a custom allowlist", {
  expect_true(check_origin("https://my.app", c("https://my.app")))
  expect_false(check_origin("https://dev.rtemis.org", c("https://my.app")))
})


# normalize_origins ----
test_that("normalize_origins() strips trailing slashes and whitespace", {
  out <- normalize_origins(c(
    " https://live.rtemis.org/ ",
    "http://localhost:3000//"
  ))
  expect_equal(out, c("https://live.rtemis.org", "http://localhost:3000"))
})

test_that("normalize_origins() returns spec defaults for NULL", {
  out <- normalize_origins(NULL)
  expect_true("https://live.rtemis.org" %in% out)
  expect_true("http://localhost:3000" %in% out)
})

test_that("normalize_origins() rejects non-character or invalid input", {
  expect_error(normalize_origins(123), "character")
  expect_error(normalize_origins(c("ok", NA)), "NA")
  expect_error(normalize_origins(c("ok", "")), "empty")
})
