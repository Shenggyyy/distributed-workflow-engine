"""Real PostgreSQL transaction and bounded-resource contracts."""

from collections.abc import Iterator
from uuid import uuid4

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.exc import DBAPIError, TimeoutError

from workflow_engine.config import Settings
from workflow_engine.database import check_database, database_engine

pytestmark = pytest.mark.integration


@pytest.fixture
def probe_table(engine: Engine) -> Iterator[str]:
    # The identifier is generated here, never obtained from external input.
    name = "dwe_test_" + uuid4().hex
    with engine.begin() as connection:
        connection.execute(text(f"CREATE TABLE {name} (id integer PRIMARY KEY)"))
    try:
        yield name
    finally:
        with engine.begin() as connection:
            connection.execute(text(f"DROP TABLE {name}"))


def test_authenticated_connection_and_isolation(engine: Engine) -> None:
    check_database(engine)
    with engine.connect() as connection:
        assert connection.get_isolation_level() == "READ COMMITTED"


def test_commit_visible_to_an_independent_connection(
    engine: Engine, database_settings: Settings, probe_table: str
) -> None:
    with engine.begin() as connection:
        connection.execute(text(f"INSERT INTO {probe_table} VALUES (:id)"), {"id": 7})
    with database_engine(database_settings) as observer:
        with observer.connect() as connection:
            assert (
                connection.execute(text(f"SELECT id FROM {probe_table}")).scalar_one()
                == 7
            )


def test_exception_rolls_back_and_connection_remains_usable(
    engine: Engine, probe_table: str
) -> None:
    with pytest.raises(RuntimeError, match="simulated failure"):
        with engine.begin() as connection:
            connection.execute(text(f"INSERT INTO {probe_table} VALUES (1)"))
            raise RuntimeError("simulated failure")
    with engine.connect() as connection:
        assert (
            connection.execute(text(f"SELECT count(*) FROM {probe_table}")).scalar_one()
            == 0
        )
    check_database(engine)


def test_pool_exhaustion_has_a_bounded_wait(database_settings: Settings) -> None:
    settings = database_settings.model_copy(
        update={"database_pool_size": 1, "database_pool_timeout_seconds": 1}
    )
    with database_engine(settings) as engine:
        with engine.connect():
            with pytest.raises(TimeoutError):
                with engine.connect():
                    pytest.fail("Pool must not create an overflow connection.")
        check_database(engine)


def test_statement_timeout_rolls_back_and_pool_can_be_reused(
    database_settings: Settings,
) -> None:
    settings = database_settings.model_copy(
        update={"database_pool_size": 1, "database_statement_timeout_ms": 100}
    )
    with database_engine(settings) as engine:
        with pytest.raises(DBAPIError) as error:
            with engine.begin() as connection:
                connection.execute(text("SELECT pg_sleep(1)"))
        assert getattr(error.value.orig, "sqlstate", None) == "57014"
        check_database(engine)
