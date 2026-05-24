#' \pkg{rtemis.server}: rtemis WebSocket server
#'
#' @name rtemis.server-package
#'
#' @title rtemis.server: rtemis Server
#'
#' @description
#' Local WebSocket server that bridges browser-based rtemislive clients
#' to a persistent R session running rtemis. See `vignette("rtemislive")`
#' (TODO) and `specs/` for the wire protocol.
#'
#' @import data.table later methods mirai nanonext openssl S7 utils
#' @importFrom jsonlite toJSON fromJSON
#' @importFrom rtemis to_json
#' @importFrom rtemis setup_SuperConfigLive setup_Preprocessor setup_Resampler setup_ExecutionConfig
"_PACKAGE"

NULL
