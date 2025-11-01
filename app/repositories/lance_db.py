from pathlib import Path
from typing import TypeVar, Literal
from async_lru import alru_cache

import lancedb
from lancedb import DBConnection, AsyncConnection
from lancedb.pydantic import LanceModel
from lancedb.db import Table, AsyncTable

from app.core.config import get_settings

settings = get_settings()

T = TypeVar("T", bound=LanceModel)


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
    if table_name in db.table_names():
        return db.open_table(table_name)

    if schema is None:
        raise ValueError(f"Table '{table_name}' does not exist and no schema provided")

    try:
        return db.create_table(table_name, schema=schema)
    except ValueError as e:
        if "already exists" in str(e):
            return db.open_table(table_name)
        raise


async def open_or_create_table_async(
    db: AsyncConnection, table_name: str, schema: type[LanceModel] | None = None
) -> AsyncTable:
    """Asynchronously open an existing LanceDB table or create a new one if it doesn't exist."""
    table_names = await db.table_names()
    if table_name in table_names:
        return await db.open_table(table_name)

    if schema is None:
        raise ValueError(f"Table '{table_name}' does not exist and no schema provided")

    return await db.create_table(table_name, schema=schema)
