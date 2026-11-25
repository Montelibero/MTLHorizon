"""Tasks that take time, started from the admin UI (or by the schedule) and
run by the worker's background thread, one at a time.

A task is a row of ``service_job`` (:mod:`app.storage.service_jobs`): put
there by the request that asked for it and answered at once, taken from
there under a lease by :class:`app.tasks.runner.TaskRunner`. What it says of
itself while it runs is in ``progress``; what it came to is in ``result``.

The kinds are the handlers of :mod:`app.tasks.kinds`: a discovery pass over
one asset, a check of the pool, a verification or a new reading of one
subject's history over a window, and a pass — or a dry run — with the
Notifier. Each is the service function the commands use, with a budget on
what it may ask of Horizon and a way to be stopped between two requests.
"""
