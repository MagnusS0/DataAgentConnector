from pathlib import Path
from typing import TypeVar, Literal
from functools import cache
from async_lru import alru_cache

import lancedb
from lancedb import DBConnection, AsyncConnection
from lancedb.pydantic import LanceModel
from lancedb.db import Table, AsyncTable

from app.core.config import get_settings

settings = get_settings()

T = TypeVar("T", bound=LanceModel)


def sql_literal(value: str) -> str:
    """Quote a string for use in a LanceDB filter expression."""
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


@cache
def get_lance_db() -> DBConnection:
    """Get or create a LanceDB connection."""
    db_path = Path(settings.lance_db_path)
    db_path.mkdir(parents=True, exist_ok=True)
    return lancedb.connect(db_path)


@alru_cache(maxsize=1)
async def get_lance_db_async() -> AsyncConnection:
    """Get or create a LanceDB asynchronous connection."""
    db_path = Path(settings.lance_db_path)
    db_path.mkdir(parents=True, exist_ok=True)
    return await lancedb.connect_async(db_path)


def batch_insert(
    table: Table,
    records: list[T] | T,
    *,
    batch_size: int = 1000,
    mode: Literal["overwrite", "append"] = "append",
):
    """Insert records into a LanceDB table."""
    if isinstance(records, list):
        for i in range(0, len(records), batch_size):
            table.add(records[i : i + batch_size], mode=mode)
    else:
        table.add(records, mode=mode)


async def batch_insert_async(
    table: AsyncTable,
    records: list[T] | T,
    *,
    batch_size: int = 1000,
    mode: Literal["overwrite", "append"] = "append",
):
    """Asynchronously insert records into a LanceDB table."""
    if isinstance(records, list):
        for i in range(0, len(records), batch_size):
            await table.add(records[i : i + batch_size], mode=mode)
    else:
        await table.add(records, mode=mode)


def open_or_create_table(
    db: DBConnection, table_name: str, schema: type[LanceModel] | None = None
) -> Table:
    """Open an existing LanceDB table or create a new one if it doesn't exist."""
    # Open first: create_table(exist_ok=True) rejects a table whose schema differs
    # from the model, e.g. one written before a field was removed.
    try:
        return db.open_table(table_name)
    except ValueError:
        if schema is None:
            raise

    return db.create_table(table_name, schema=schema, exist_ok=True)


async def open_or_create_table_async(
    db: AsyncConnection, table_name: str, schema: type[LanceModel] | None = None
) -> AsyncTable:
    """Asynchronously open an existing LanceDB table or create a new one if it doesn't exist."""
    try:
        return await db.open_table(table_name)
    except ValueError:
        if schema is None:
            raise

    return await db.create_table(table_name, schema=schema, exist_ok=True)
