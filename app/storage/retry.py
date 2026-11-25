"""Deadlocks and serialization failures — PostgreSQL's SQLSTATE 40P01 and
40001 — told from other database errors.

The database aborts one transaction of a cycle to break it. That is not the
work's failure: what the transaction stored is stored, and the work goes on
from there. So a try that ended that way is asked again at once; it counts
as a try all the same, so that it cannot go on for ever.

The advisory lock that makes one writer at a time keeps writers of history
from colliding; it is no proof that no two transactions anywhere can wait
for each other. So this is looked for wherever several processes work.
"""

from __future__ import annotations

DEADLOCK = "40P01"
SERIALIZATION = "40001"
CONTENTION = (DEADLOCK, SERIALIZATION)


def contention(error: BaseException | None) -> str | None:
    """The SQLSTATE when ``error`` is a deadlock or a serialization failure —
    looked for through what wraps it and what it was raised from — else
    None."""
    stack: list[BaseException | None] = [error]
    seen: set[int] = set()
    while stack:
        item = stack.pop()
        if item is None or id(item) in seen:
            continue
        seen.add(id(item))
        code = getattr(item, "sqlstate", None)
        if code in CONTENTION:
            return str(code)
        stack += [getattr(item, "orig", None), item.__cause__, item.__context__]
    return None


def said(code: str | None) -> str:
    """What happened, in the words of the database."""
    if code == DEADLOCK:
        return "deadlock detected"
    if code == SERIALIZATION:
        return "could not serialize the transaction"
    return "the database aborted the transaction"
