from collections.abc import Generator

import duckdb

from proteinvault.config import Settings
from proteinvault.db.schema import create_schema

_connection: duckdb.DuckDBPyConnection | None = None


def init_db(settings: Settings) -> None:
    global _connection  # noqa: PLW0603
    _connection = duckdb.connect(str(settings.db_path))
    create_schema(_connection)


def close_db() -> None:
    global _connection  # noqa: PLW0603
    if _connection is not None:
        _connection.close()
        _connection = None


def get_connection() -> duckdb.DuckDBPyConnection:
    if _connection is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _connection


def get_cursor() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    cursor = get_connection().cursor()
    try:
        yield cursor
    finally:
        cursor.close()
