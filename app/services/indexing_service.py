import asyncio
from collections.abc import Sequence
from typing import Literal

from lancedb.index import FTS

from app.repositories.column_contents import ColumnContentRepository
from app.repositories.lance_db import get_lance_db_async, open_or_create_table_async
from app.models.lance import ColumnContent
from app.schemas.config import ExtractionOptions
from app.domain.extract_colum_content import extract_column_contents
from app.repositories.sql_db import list_schemas_for
from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)


class IndexingService:
    """Service for managing column content extraction and FTS indexing."""

    async def index_database(
        self,
        database: str,
        *,
        schemas: Sequence[str] | None = None,
        mode: Literal["overwrite", "append"] = "overwrite",
        language: str = "English",
        options: ExtractionOptions | None = None,
    ) -> None:
        """Extract column contents and create FTS index for a database.

        Args:
            database: Name of the database to index.
            schemas: Iterable of schemas to index. Defaults to all schemas discovered.
            mode: How to handle existing index data:
                - overwrite: Drop and recreate
                - append: Add to existing
            language: Language for text tokenization/stemming.
            options: Optional extraction tuning parameters.
        """
        target_schemas = (
            tuple(dict.fromkeys(schemas))
            if schemas
            else tuple(list_schemas_for(database))
        )
        if not target_schemas:
            logger.warning(
                "No schemas discovered for database '%s'. Skipping indexing.", database
            )
            return

        # Extract contents (CPU-bound, run in thread pool)
        contents = await asyncio.to_thread(
            extract_column_contents,
            database,
            schemas=target_schemas,
            options=options or get_settings().fts_extraction_options,
        )

        if not contents:
            logger.warning("No content to index for database '%s'", database)
            return

        # Save to LanceDB via repository
        column_repo = ColumnContentRepository(database)
        await column_repo.save_batch(contents, mode=mode)

        # Create FTS index
        db = await get_lance_db_async()
        table_name = f"column_contents_{database}"
        table = await open_or_create_table_async(db, table_name, schema=ColumnContent)

        index = FTS(
            with_position=True,
            stem=False,
            language=language,
        )

        await table.create_index(
            "content",
            replace=True,
            config=index,
        )
        await table.wait_for_index(["content_idx"])

        logger.info(
            "FTS index for database '%s' (schemas: %s) created/updated with %d entries.",
            database,
            target_schemas,
            len(contents),
        )

    async def index_all_databases(
        self,
        *,
        mode: Literal["overwrite", "append"] = "overwrite",
        language: str = "English",
        options: ExtractionOptions | None = None,
    ) -> None:
        """Create FTS indices for all configured databases.

        Args:
            mode: How to handle existing index data.
            language: Language for text tokenization/stemming.
            options: Optional extraction tuning parameters.
        """
        settings = get_settings()
        if not settings.databases:
            logger.warning("No databases configured")
            return

        databases = settings.databases.names()

        tasks = [
            self.index_database(
                database,
                schemas=list_schemas_for(database),
                mode=mode,
                language=language,
                options=options,
            )
            for database in databases
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for database, result in zip(databases, results):
            if isinstance(result, Exception):
                logger.exception(
                    "Failed to create content index for database '%s'",
                    database,
                    exc_info=result,
                )


def get_indexing_service() -> IndexingService:
    """Create indexing service instance."""
    return IndexingService()
