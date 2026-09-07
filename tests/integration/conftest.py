"""Database fixtures shared by PostgreSQL and migration integration tests."""

from collections.abc import Iterator

import pytest
from sqlalchemy import Engine

from workflow_engine.config import Settings, load_settings
from workflow_engine.database import database_engine


@pytest.fixture
def database_settings(pytestconfig: pytest.Config) -> Settings:
    config_file = pytestconfig.getoption("database_env_file")
    if config_file is None:
        pytest.skip("Pass --database-env-file pointing to a dedicated test database.")
    return load_settings(env_file=config_file)


@pytest.fixture
def engine(database_settings: Settings) -> Iterator[Engine]:
    with database_engine(database_settings) as value:
        yield value
