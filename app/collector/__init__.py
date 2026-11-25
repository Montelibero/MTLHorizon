"""Live collector: follows the streams of tracked subjects as the network closes ledgers.

A cycle asks the source for its latest ledger, then reads every stream
forward from where the cycle before ended. It uses the watch lists and the
coverage of :mod:`app.storage`, the Horizon adapter of :mod:`app.sources`
and the storing of :mod:`app.backfill`; what is its own is the direction
(forward), the frontier of every stream, and when streams are read.
"""
