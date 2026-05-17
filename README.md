[![r-ci](https://github.com/rtemis-org/rtemis.server/actions/workflows/r-ci.yml/badge.svg)](https://github.com/rtemis-org/rtemis.server/actions/workflows/r-ci.yml)

# rtemis.server

rtemis.server launches a WebSocket server bridging the rtemis machine learning library and the rtemislive web client. It handles authenticated client connections, session management, and async dispatch of rtemis operations - using nanonext for the WebSocket layer, mirai for parallel compute, and later for event-loop scheduling.