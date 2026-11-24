"""Live collector: follows the streams of tracked subjects as the network closes ledgers.

Boundary (not implemented in stage 01): the collector reads the watch lists
and checkpoints from :mod:`app.storage`, pages each stream forward through
the Horizon adapter in :mod:`app.sources`, and hands every page to the
storage layer in one transaction together with its checkpoint.

It owns *when* and *in what order* streams are read. It does not know the
wire format and does not write SQL of its own.
"""
