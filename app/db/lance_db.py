from hashlib import sha256
from pathlib import Path
from typing import Optional

import lancedb
from lancedb import DBConnection, AsyncConnection
from lancedb.pydantic import LanceModel, Vector
from lancedb.db import Table, AsyncTable
from pydantic import field_validator

from app.core.config import get_settings

settings = get_settings()


class TableAnnotation(LanceModel):
    """Schema for table annotations stored in LanceDB."""

    database_name: str
    table_name: str
    description: str
    embeddings: Optional[Vector(settings.n_dims)] = None  # type: ignore[valid-type]
    metadata_json: str = ""
    schema_hash: str | None = None

    @field_validator("schema_hash", mode="before")
    @classmethod
    def compute_schema_hash(cls, v, info):
        """Compute schema hash if not provided."""
        if v:
            return v
        data = info.data
        hash_input = (
            f"{data['database_name']}.{data['table_name']}.{data['metadata_json']}"
        )
        return sha256(hash_input.encode("utf-8")).hexdigest()


def get_lance_db() -> DBConnection:
    """Get or create a LanceDB connection."""
    db_path = Path(settings.lance_db_path)
    db_path.mkdir(parents=True, exist_ok=True)
    return lancedb.connect(db_path)


async def get_lance_db_async() -> AsyncConnection:
    """Get or create a LanceDB asynchronous connection."""
    db_path = Path(settings.lance_db_path)
    db_path.mkdir(parents=True, exist_ok=True)
    return await lancedb.connect_async(db_path)


def batch_insert(
    table: Table,
    records: list[TableAnnotation] | TableAnnotation,
    batch_size: int = 1000,
):
    """Insert records into a LanceDB table."""
    if isinstance(records, list):
        for i in range(0, len(records), batch_size):
            table.add(records[i : i + batch_size])
    else:
        table.add(records)


async def batch_insert_async(
    table: AsyncTable,
    records: list[TableAnnotation] | TableAnnotation,
    batch_size: int = 1000,
):
    """Asynchronously insert records into a LanceDB table."""
    if isinstance(records, list):
        for i in range(0, len(records), batch_size):
            await table.add(records[i : i + batch_size])
    else:
        await table.add(records)
