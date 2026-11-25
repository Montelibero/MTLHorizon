"""The history API: a Horizon-compatible, read-only subset of the history this
installation has collected, answered from PostgreSQL.

:func:`build_router` is what the application mounts, at its root. The routes
and what they answer are in :mod:`app.history_api.routes`; what is
implemented, and how it differs from upstream, is documented for people in
``docs/history-api.md``.
"""

from app.history_api.routes import build_router

__all__ = ["build_router"]
