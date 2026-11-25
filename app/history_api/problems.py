"""Problem documents (RFC 9457), in the shape upstream Horizon answers with.

A route that cannot answer raises :class:`Problem`; the router turns it into
the response. The ``type`` of a 400, a 404, a 500 and a 503 is the one upstream
uses for the same situation. Where this installation has no upstream type to
borrow (it is not Stellar, and does not invent addresses under its name), the
type is ``about:blank`` and the title says what is wrong.
"""

from __future__ import annotations

from typing import Any

from fastapi.responses import JSONResponse

ERRORS = "https://stellar.org/horizon-errors/"

# Nothing of this API is stored by a cache: an answer is a look at the archive
# as it is now, and a shared cache must not keep what a token unlocked.
NO_STORE = "no-cache, no-store, max-age=0"


class Problem(Exception):
    """A problem document to answer with.

    ``field`` is the request parameter to blame. The value the client sent is
    never repeated in the document: it says nothing that the sender does not
    know, and an answer is no place to carry input back.
    """

    def __init__(
        self,
        status: int,
        type_: str,
        title: str,
        detail: str,
        *,
        field: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(detail)
        self.status = status
        self.type = type_
        self.title = title
        self.detail = detail
        self.field = field
        self.headers = dict(headers or {})

    def body(self) -> dict[str, Any]:
        body: dict[str, Any] = {"type": self.type, "title": self.title, "status": self.status, "detail": self.detail}
        if self.field is not None:
            body["extras"] = {"invalid_field": self.field}
        return body

    def response(self) -> JSONResponse:
        return JSONResponse(
            self.body(),
            status_code=self.status,
            media_type="application/problem+json",
            headers={"Cache-Control": NO_STORE, **self.headers},
        )


def bad_request(detail: str, *, field: str | None = None) -> Problem:
    """400: the request cannot be answered as it stands."""
    return Problem(400, f"{ERRORS}bad_request", "Bad Request", detail, field=field)


def not_found(detail: str) -> Problem:
    """404: the archive does not have it.

    That is all it says. This installation keeps the history of selected
    accounts and markets; what is not here may well exist on the network.
    """
    return Problem(404, f"{ERRORS}not_found", "Resource Missing", detail)


def service_unavailable(detail: str) -> Problem:
    """503: there is nothing to answer from, or the answer would take too long."""
    return Problem(503, f"{ERRORS}service_unavailable", "Service Unavailable", detail)


def internal_error() -> Problem:
    """500: the cause is in the server log, and nowhere in the answer."""
    return Problem(500, f"{ERRORS}internal_server_error", "Internal Server Error", "the request could not be answered")


def unauthorized(detail: str) -> Problem:
    """401: no token, or not one of the configured ones. The same words for both."""
    return Problem(
        401, "about:blank", "Unauthorized", detail, headers={"WWW-Authenticate": 'Bearer realm="history-api"'}
    )


def forbidden(detail: str) -> Problem:
    """403: a token is needed and there is none to give."""
    return Problem(403, "about:blank", "Forbidden", detail)
