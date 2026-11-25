"""Is a request really from the Notifier? It signs every webhook.

The Notifier signs the body it posts — exactly the bytes of
``JSON.stringify(data)`` — with ed25519, through a Stellar key pair, and
puts the signature in ``X-Request-ED25519-Signature`` as base64. The key it
signs with is published as a Stellar public key (a ``G…`` address) by
``GET /api/status`` of the Notifier itself; that address is what this
installation is configured with.

The signature is checked against the body as it arrived, before anything in
it is read. Nothing is signed that came from anywhere else: a forged or
tampered request fails here, and a captured one can only be posted again as
it is, which the event journal recognises.
"""

from __future__ import annotations

import base64
import binascii
from collections.abc import Sequence

from stellar_sdk import Keypair
from stellar_sdk.exceptions import BadSignatureError

HEADER = "X-Request-ED25519-Signature"
SIGNATURE_LENGTH = 64


def signed_by(public_keys: Sequence[str], body: bytes, signature: str | None) -> str | None:
    """Which of ``public_keys`` signed ``body``; None when none did, or when
    there is no signature to speak of."""
    if not signature:
        return None
    try:
        raw = base64.b64decode(signature, validate=True)
    except (ValueError, binascii.Error):
        return None
    if len(raw) != SIGNATURE_LENGTH:
        return None
    for public_key in public_keys:
        try:
            Keypair.from_public_key(public_key).verify(body, raw)
        except (BadSignatureError, ValueError, TypeError, AttributeError):
            continue
        return public_key
    return None


def checked_key(value: str) -> str:
    """A Notifier signing key as it is written: a Stellar public key. Raises
    ``ValueError`` when it is not one."""
    key = value.strip()
    try:
        Keypair.from_public_key(key)
    except Exception as error:  # noqa: BLE001 - whatever is wrong with it, it is not a key
        raise ValueError("not a Stellar public key") from error
    return key
