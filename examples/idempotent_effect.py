"""Cooperating PostgreSQL business sink: receipt and counter share one transaction."""

import argparse
from dataclasses import dataclass, field
from pathlib import Path
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    Column,
    Connection,
    Integer,
    MetaData,
    Table,
    Uuid,
    select,
)
from sqlalchemy.dialects.postgresql import insert

from workflow_engine.config import Settings, load_settings
from workflow_engine.database import database_engine
from workflow_engine.domain.completion import CompletionOutcome, CompletionResult
from workflow_engine.worker.handlers import HandlerContext

metadata = MetaData()
receipts = Table(
    "demo_effect_receipts",
    metadata,
    Column("task_id", Uuid, primary_key=True),
    Column("units", Integer, nullable=False),
)
counter = Table(
    "demo_business_counter",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("value", BigInteger, nullable=False),
)


def initialize(connection: Connection) -> None:
    """Explicit example setup, outside the engine's schema/migrations."""
    metadata.create_all(connection)
    connection.execute(insert(counter).values(id=1, value=0).on_conflict_do_nothing())


def record_effect(connection: Connection, task_id: UUID, *, units: int = 1) -> bool:
    """Return provisional first-write status; the caller must commit both writes."""
    if (
        not connection.in_transaction()
        or connection.dialect.name != "postgresql"
        or connection.get_isolation_level() != "READ COMMITTED"
        or getattr(connection.connection.driver_connection, "autocommit", True)
    ):
        raise ValueError("An active PostgreSQL READ COMMITTED transaction is required.")
    if not isinstance(task_id, UUID) or type(units) is not int or not 1 <= units <= 100:
        raise ValueError("A Task UUID and bounded positive units are required.")
    first = connection.scalar(
        insert(receipts)
        .values(task_id=task_id, units=units)
        .on_conflict_do_nothing(index_elements=[receipts.c.task_id])
        .returning(receipts.c.task_id)
    )
    if first is None:
        # A new READ COMMITTED statement sees the winning committed receipt.
        if (
            connection.scalar(
                select(receipts.c.units).where(receipts.c.task_id == task_id)
            )
            != units
        ):
            raise ValueError("Idempotency key is already bound to different input.")
        return False
    connection.execute(
        counter.update()
        .where(counter.c.id == 1)
        .values(value=counter.c.value + units)
        .returning(counter.c.id)
    ).scalar_one()
    return True


@dataclass(frozen=True)
class EffectHandler:
    """Trusted registration config supplies the business destination, never the DAG."""

    settings: Settings = field(repr=False)
    schema: str | None = None

    def __call__(self, context: HandlerContext) -> CompletionResult:
        with database_engine(self.settings) as engine:
            mapped = engine.execution_options(schema_translate_map={None: self.schema})
            with mapped.begin() as connection:
                record_effect(connection, UUID(context.idempotency_key))
        return CompletionResult(outcome=CompletionOutcome.SUCCEEDED)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-env-file", type=Path, required=True)
    args = parser.parse_args()
    with database_engine(load_settings(env_file=args.database_env_file)) as engine:
        with engine.begin() as connection:
            initialize(connection)
        key = uuid4()
        with engine.begin() as connection:
            assert record_effect(connection, key)
        with engine.begin() as connection:
            assert not record_effect(connection, key)
        print("Two invocations; one committed business increment for this Task key.")


if __name__ == "__main__":
    main()
