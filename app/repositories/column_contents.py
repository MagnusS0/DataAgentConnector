from typing import Literal

from app.models.lance import ColumnContent
from app.repositories.lance_db import (
    get_lance_db_async,
    get_lance_db,
    batch_insert_async,
    open_or_create_table_async,
)


class ColumnContentRepository:
    """Data access for ColumnContent in LanceDB."""

    def __init__(self, database: str, *, schema: str | None = None):
        self.database = database
        self.schema = schema
        self.table_name = f"column_contents_{database}"

    def search_fts(
        self,
        query: str,
        *,
        schema: str | None = None,
        top_k: int = 5,
    ) -> list[dict]:
        """Full-text search on column contents (sync version for simple queries)."""
        db = get_lance_db()

        # Check if table exists before opening
        if self.table_name not in db.table_names():
            raise ValueError(
                f"Column contents for database '{self.database}' have not been indexed. "
                f"Please run indexing first."
            )

        table = db.open_table(self.table_name)

        target_schema = schema or self.schema

        try:
            search = table.search(query, query_type="fts", fts_columns=["content"])
            if target_schema:
                search = search.where(f"schema_name = '{target_schema}'")
            results = (
                search.select(
                    [
                        "schema_name",
                        "table_name",
                        "column_name",
                        "content",
                        "num_distinct",
                        "_score",
                    ]
                )
                .limit(top_k)
                .to_list()
            )
        except Exception as e:
            if "no inverted index" in str(e).lower():
                raise ValueError(
                    f"FTS index for database '{self.database}' is incomplete. "
                    f"Please re-run indexing."
                ) from e
            raise

        return results

    async def get_by_table(
        self,
        table_name: str,
        *,
        schema: str | None = None,
        columns: list[str] | None = None,
    ) -> list[dict]:
        """Get column contents for a specific table."""
        db = await get_lance_db_async()
        try:
            table = await db.open_table(self.table_name)
        except ValueError as exc:
            raise ValueError(
                f"Column contents table '{self.table_name}' does not exist."
            ) from exc

        target_schema = schema or self.schema
        query = table.query().where(f"table_name = '{table_name}'")
        if target_schema:
            query = query.where(f"schema_name = '{target_schema}'")

        if columns:
            query = query.select(columns)

        return await query.to_list()

    async def save_batch(
        self,
        contents: list[ColumnContent],
        *,
        mode: Literal["overwrite", "append"] = "append",
    ) -> None:
        """Save column contents in bulk."""
        if not contents:
            return
        db = await get_lance_db_async()
        table = await open_or_create_table_async(
            db, self.table_name, schema=ColumnContent
        )
        await batch_insert_async(table, contents, mode=mode)
