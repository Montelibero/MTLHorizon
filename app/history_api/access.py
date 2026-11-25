"""Who may read the history API: everyone, or only the holder of a token.

The policy is ``settings.api_access``. ``open`` lets anyone who reaches the
API read it. ``token`` wants ``Authorization: Bearer <token>`` with one of
``settings.api_tokens``. With ``token`` and no token configured nobody is let
in: the API is closed and says so, because an API that forgot its secret must
not become public by that alone.

Nothing of a token reaches an answer or a log: not the one presented, not
how close it came, not which of the configured ones it was.
"""

from __future__ import annotations

import secrets

from fastapi import Request

from app.config import Settings
from app.history_api.problems import Problem, forbidden, unauthorized


def denied(request: Request, settings: Settings) -> Problem | None:
    """The problem to answer with, or None when the request may read."""
    if settings.api_access == "open":
        return None
    if not settings.api_tokens:
        return forbidden(
            "the history API needs a token and none is configured (MTL_HORIZON_API_TOKENS), so it is closed"
        )
    scheme, _, presented = request.headers.get("authorization", "").strip().partition(" ")
    if scheme.lower() != "bearer" or not presented.strip() or not _is_one_of(presented.strip(), settings.api_tokens):
        return unauthorized("the history API is read with a token: Authorization: Bearer <token>")
    return None


def _is_one_of(presented: str, tokens: tuple[str, ...]) -> bool:
    """Whether ``presented`` is one of ``tokens``, compared in constant time.

    Every configured token is compared, whatever an earlier comparison said:
    how long this takes tells nothing about which token, if any, matched.
    """
    shown = presented.encode()
    matched = False
    for token in tokens:
        matched |= secrets.compare_digest(shown, token.encode())
    return matched
