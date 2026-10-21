# zzz.R
# ::rtemis.server::

# Server state. On the host it holds the session registry
# (`rtemislive_sessions`); on each daemon it holds the progress socket
# (`rtemislive_progress_socket`) and the id of the running job
# (`rtemislive_job_id`). The binding is locked with the namespace; the
# environment's contents stay mutable.
live <- new.env(parent = emptyenv())
