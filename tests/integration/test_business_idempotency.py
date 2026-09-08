"""The destination atomically couples a unique receipt with a non-idempotent effect."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from examples.idempotent_effect import counter, initialize, receipts, record_effect
from sqlalchemy import Engine, event, func, select

pytestmark = pytest.mark.integration


@pytest.fixture
def business_engine(engine: Engine, migration_schema: str) -> Engine:
    mapped = engine.execution_options(schema_translate_map={None: migration_schema})
    with mapped.begin() as connection:
        initialize(connection)
    return mapped


def test_concurrent_duplicates_increment_once(business_engine: Engine) -> None:
    key = uuid4()
    barrier = Barrier(4, timeout=5)

    def apply() -> bool:
        with business_engine.begin() as connection:
            barrier.wait()
            return record_effect(connection, key)

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(apply) for _ in range(4)]
        assert sum(f.result(timeout=10) for f in futures) == 1
    with business_engine.begin() as connection:
        assert connection.scalar(select(counter.c.value)) == 1
        assert connection.scalar(select(func.count()).select_from(receipts)) == 1
        assert record_effect(connection, uuid4())
    with business_engine.connect() as connection:
        assert connection.scalar(select(counter.c.value)) == 2


@pytest.mark.parametrize("fault", ["rollback", "commit"])
def test_failed_transaction_rolls_back_receipt_and_effect(
    business_engine: Engine, fault: str
) -> None:
    key = uuid4()

    def fail_commit(connection: object) -> None:
        raise RuntimeError("injected commit failure")

    if fault == "commit":
        with pytest.raises(RuntimeError, match="injected"):
            with business_engine.begin() as connection:
                event.listen(connection, "commit", fail_commit)
                assert record_effect(connection, key)
    else:
        with business_engine.begin() as connection:
            assert record_effect(connection, key)
            connection.rollback()
    with business_engine.begin() as connection:
        assert connection.scalar(select(counter.c.value)) == 0
        assert connection.scalar(select(func.count()).select_from(receipts)) == 0
        assert record_effect(connection, key)


def test_key_payload_conflict_cannot_change_effect(business_engine: Engine) -> None:
    key = uuid4()
    with business_engine.begin() as connection:
        assert record_effect(connection, key, units=2)
    with pytest.raises(ValueError, match="different input"):
        with business_engine.begin() as connection:
            record_effect(connection, key, units=3)
    with business_engine.connect() as connection:
        assert connection.scalar(select(counter.c.value)) == 2


def test_autocommit_rejected_before_writes(business_engine: Engine) -> None:
    with business_engine.connect().execution_options(
        isolation_level="AUTOCOMMIT"
    ) as connection:
        with connection.begin(), pytest.raises(ValueError, match="transaction"):
            record_effect(connection, uuid4())
    with business_engine.connect() as connection:
        assert connection.scalar(select(counter.c.value)) == 0
