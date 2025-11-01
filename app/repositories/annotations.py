from hashlib import sha256

from app.models.lance import TableAnnotation
from app.repositories.lance_db import (
    get_lance_db_async,
    batch_insert_async,
    open_or_create_table_async,
)
from app.core.db_registry import TableMetadata


class AnnotationRepository:
    """Data access for TableAnnotation in LanceDB."""

    def __init__(self):
        self._db = None
        self._table = None

    async def _get_table(self):
        """Lazy-load table connection."""
        if self._table is None:
            if self._db is None:
                self._db = await get_lance_db_async()
            self._table = await open_or_create_table_async(
                self._db, "table_annotations", TableAnnotation
            )
        return self._table

    async def exists_by_schema_hash(self, schema_hash: str) -> bool:
        """Check if annotation exists for given schema hash."""
        table = await self._get_table()
        results = (
            await table.query()
            .where(f"schema_hash == '{schema_hash}'")
            .limit(1)
            .to_list()
        )
        return len(results) > 0

    async def save_batch(self, annotations: list[TableAnnotation]) -> None:
        """Save multiple annotations to LanceDB."""
        if not annotations:
            return
        table = await self._get_table()
        await batch_insert_async(table, annotations)

    async def get_descriptions_by_database(self, database: str) -> list[dict[str, str]]:
        """Get all table descriptions for a database."""
        table = await self._get_table()
        results = (
            await table.query()
            .where(f"database_name = '{database}'")
            .select(["table_name", "description"])
            .to_list()
        )
        return results

    @staticmethod
    def compute_schema_hash(
        database: str, table_name: str, metadata: TableMetadata
    ) -> str:
        """Compute schema hash for checking if annotation is current."""
        hash_input = f"{database}.{table_name}.{metadata.model_dump_json()}"
        return sha256(hash_input.encode("utf-8")).hexdigest()
