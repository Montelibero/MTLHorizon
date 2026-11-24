"""Adapters for external sources of Stellar history.

Boundary: a source adapter knows one provider's wire format and nothing
about the database. It turns the provider's records into plain values the
storage layer accepts (see :mod:`app.storage`). The Horizon adapter is the
only one planned for now; an archive import would be a sibling module that
maps into the same values without reusing anyone else's surrogate ids.
"""
