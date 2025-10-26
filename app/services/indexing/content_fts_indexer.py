from typing import Literal
import asyncio

from lancedb.index import FTS

from app.db.lance_db import (
    get_lance_db_async,
    open_or_create_table_async,
    batch_insert_async,
)
from app.core.config import get_settings
from app.core.logging import get_logger
from app.models.lance_schemas import ColumnContent
from app.models.indexing import ExtractionOptions
from app.services.indexing.extract_colum_content import extract_column_contents

logger = get_logger(__name__)


async def create_content_index(
    database: str,
    mode: Literal["overwrite", "append"] = "overwrite",
    language: str = "English",
    *,
    options: ExtractionOptions | None = None,
) -> None:
    """Create or update FTS index for column contents in a database.

    Args:
        database: Name of the database to index
        options: Optional extraction tuning parameters
        mode: How to handle existing index data:
            - overwrite: Drop and recreate
            - append: Add to existing
        language: Language for text tokenization/stemming
    """
    contents = await asyncio.to_thread(
        extract_column_contents,
        database,
        options=options or get_settings().fts_extraction_options,
    )
    if not contents:
        logger.warning("No content to index for database '%s'", database)
        return

    db = await get_lance_db_async()
    table_name = f"column_contents_{database}"
    table = await open_or_create_table_async(
        db,
        table_name,
        schema=ColumnContent,
    )

    await batch_insert_async(table, contents, mode=mode)

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
        "FTS index for database '%s' created/updated with %d entries.",
        database,
        len(contents),
    )


async def create_all_content_indices(
    *,
    mode: Literal["overwrite", "append"] = "overwrite",
    language: str = "English",
    options: ExtractionOptions | None = None,
) -> None:
    """Create FTS indices for all configured databases."""
    settings = get_settings()
    if not settings.databases:
        logger.warning("No databases configured")
        return

    databases = settings.databases.names()

    tasks = [
        create_content_index(
            database,
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
