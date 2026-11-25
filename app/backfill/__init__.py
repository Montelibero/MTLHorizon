"""Backfill: reads the history of tracked subjects for a ledger window from Horizon.

The window is read backwards, stream by stream (an account's transactions,
operations, effects and trades; a market's trades), from its top ledger to the
bottom one. Every stored page moves the stream checkpoint and closes its part
of the window in ``sync_coverage``; a stopped run continues from the
checkpoint, and a verified window is not read again. What the source no longer
keeps is marked ``unverifiable`` instead of being guessed.

Modules: ``cli`` (commands), ``config`` (the YAML watch-list), ``jobs`` (the
streams a window is made of), ``loader`` (the reading loop, context of
transactions, checkpoints), ``store`` (pages to rows), ``probe`` and ``verify``
(what the source says against what the database has).

It shares the Horizon adapter and the storage layer with the collector; only
the direction of travel and the bookkeeping of ranges differ.
"""
