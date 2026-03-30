from collections.abc import AsyncIterator, Generator
from contextlib import asynccontextmanager

import duckdb
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import proteinvault.db.connection as conn_module
from proteinvault.api.v1.router import v1_router
from proteinvault.db.schema import create_schema
from proteinvault.main import _register_exception_handlers


@pytest.fixture
def db() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    conn = duckdb.connect(":memory:")
    create_schema(conn)
    yield conn
    conn.close()


@pytest.fixture
def client() -> Generator[TestClient, None, None]:
    db = duckdb.connect(":memory:")
    create_schema(db)
    conn_module._connection = db

    @asynccontextmanager
    async def test_lifespan(_app: FastAPI) -> AsyncIterator[None]:
        yield

    app = FastAPI(lifespan=test_lifespan)
    _register_exception_handlers(app)
    app.include_router(v1_router, prefix="/api/v1")

    with TestClient(app, raise_server_exceptions=True) as c:
        yield c

    db.close()
    conn_module._connection = None
