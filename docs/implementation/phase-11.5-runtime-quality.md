# Runtime-quality lifecycle and subscriptions

`Client::close()` rejects new work, publishes `Closing`, waits at most one
second for registered workers to reach a safe boundary, aborts only remaining
stragglers, flushes SQLite, and then publishes `Closed`. A `watch` value
latches that one completion for all concurrent callers.

State/query subscriptions use `watch`, so slow readers receive the newest
committed snapshot rather than an accumulating history. Event and operation
subscriptions use bounded `broadcast` queues. An event watcher reports lag so
callers can recover from durable history; operation waiting treats lag as a
prompt to re-read its durable status.
