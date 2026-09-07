"""Configure migrations against test-owned schemas."""

from alembic.config import Config
from sqlalchemy import Connection, text


def migration_config(connection: Connection, schema: str) -> Config:
    connection.execute(text(f'SET LOCAL search_path TO "{schema}"'))
    config = Config()
    config.set_main_option("script_location", "workflow_engine:migrations")
    config.attributes["connection"] = connection
    return config
