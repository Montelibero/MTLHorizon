"""Backfill: collects the history of tracked subjects that predates the live stream.

Boundary (not implemented in stage 01): the backfill walks ledger ranges
backwards from the checkpoint, records what each range yielded in
``sync_coverage`` (including ranges that turned out to be empty), and stops
where the source's history begins. It reports the Horizon retention boundary
(``before_history``) instead of guessing past it.

It shares the Horizon adapter and the storage layer with the collector; only
the direction of travel and the bookkeeping of ranges differ.
"""
