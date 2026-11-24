# zzz.R
# ::rtemis.server::
# 2026- EDG rtemis.org

# Compute profile of the server's mirai daemons. Every mirai call of the
# package names it: an unnamed call addresses the default profile, where the
# caller's own daemons live.
.RTEMISLIVE_COMPUTE <- "rtemis.server"

# Server state. On the host it holds the session registry
# (`rtemislive_sessions`) and the address of the progress channel
# (`rtemislive_progress_url`); on each daemon it holds the progress socket
# (`rtemislive_progress_socket`) and the id of the running job
# (`rtemislive_job_id`). The binding is locked with the namespace; the
# environment's contents stay mutable.
live <- new.env(parent = emptyenv())
