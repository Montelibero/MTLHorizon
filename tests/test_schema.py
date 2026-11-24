"""The migrated database and the models describe the same schema.

`alembic check` compares columns and indexes but not CHECK constraints, and
PostgreSQL silently truncates long names, so names are compared here too.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator

from sqlalchemy import CheckConstraint, ForeignKeyConstraint, PrimaryKeyConstraint, UniqueConstraint, inspect, text
from sqlalchemy.engine import Engine

from app.storage.models import Base


def database_objects(engine: Engine) -> dict[str, set[tuple[str, str]]]:
    """Table -> {(kind, name)} as PostgreSQL has it."""
    inspector = inspect(engine)
    objects: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for table in inspector.get_table_names():
        if table == "alembic_version":
            continue
        for kind, constraints in (
            ("check", inspector.get_check_constraints(table)),
            ("unique", inspector.get_unique_constraints(table)),
            ("foreign_key", inspector.get_foreign_keys(table)),
        ):
            for constraint in constraints:
                objects[table].add((kind, constraint["name"]))
        primary_key = inspector.get_pk_constraint(table)["name"]
        objects[table].add(("primary_key", primary_key))
        for index in inspector.get_indexes(table):
            objects[table].add(("index", index["name"]))

    with engine.connect() as connection:
        exclusions = connection.execute(
            text("SELECT conrelid::regclass::text, conname FROM pg_constraint WHERE contype = 'x'")
        ).all()
    for table, name in exclusions:
        objects[table].add(("exclusion", name))
    return objects


def model_objects() -> Iterator[tuple[str, str, str]]:
    kinds = {
        PrimaryKeyConstraint: "primary_key",
        UniqueConstraint: "unique",
        ForeignKeyConstraint: "foreign_key",
        CheckConstraint: "check",
    }
    for table in Base.metadata.tables.values():
        for constraint in table.constraints:
            yield table.name, kinds[type(constraint)], constraint.name  # type: ignore[index]
        for index in table.indexes:
            yield table.name, "index", index.name
    # Declared as DDL next to the SyncCoverage model.
    yield "sync_coverage", "exclusion", "ex_sync_coverage_ledger_range"


def test_database_contains_exactly_the_model_tables(engine: Engine) -> None:
    assert set(inspect(engine).get_table_names()) == set(Base.metadata.tables) | {"alembic_version"}


def test_every_model_object_exists_under_its_own_name(engine: Engine) -> None:
    in_database = database_objects(engine)
    missing = sorted(thing for thing in model_objects() if thing[1:] not in in_database[thing[0]])
    assert missing == []


def backing_indexes(objects: set[tuple[str, str]]) -> set[tuple[str, str]]:
    """The indexes PostgreSQL creates to enforce the named constraints.

    Every UNIQUE, PRIMARY KEY or EXCLUSION constraint is backed by an index
    that carries the constraint's name.
    """
    enforced = {name for kind, name in objects if kind != "index"}
    return {("index", name) for name in enforced}


def test_database_has_no_object_the_models_do_not_know(engine: Engine) -> None:
    in_models: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for table, kind, name in model_objects():
        in_models[table].add((kind, name))
    for table, objects in database_objects(engine).items():
        extra = objects - in_models[table] - backing_indexes(objects)
        assert not extra, f"{table}: {sorted(extra)}"
