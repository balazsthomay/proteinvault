from collections.abc import Generator
from typing import Annotated

import duckdb
from fastapi import Depends

from proteinvault.db.connection import get_connection

Cursor = duckdb.DuckDBPyConnection


def get_cursor() -> Generator[Cursor, None, None]:
    cursor = get_connection().cursor()
    try:
        yield cursor
    finally:
        cursor.close()


CursorDep = Annotated[Cursor, Depends(get_cursor)]
