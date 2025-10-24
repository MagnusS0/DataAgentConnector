import asyncio
from typing import TypedDict
from functools import lru_cache

from app.db.lance_db import get_lance_db, open_or_create_table
from app.services.annotate.table_annotation import annotate_database, save_to_lance_db
from app.core.logging import get_logger
from app.core.config import get_settings

logger = get_logger("services.annotation_store")
settings = get_settings()


class TableDescription(TypedDict):
    table_name: str
    description: str | None


async def store_table_descriptions() -> None:
    """Store table descriptions for all configured databases in LanceDB."""
    if not settings.annotate_on_startup:
        logger.info(
            "Skipping table annotation store because annotate_on_startup is disabled."
        )
        return

    database_registry = settings.databases
    database_names = database_registry.names() if database_registry is not None else []

    if not database_names:
        logger.info("No databases configured, skipping annotation store.")
        return

    save_tasks: list[asyncio.Task[None]] = []
    for database in database_names:
        annotations = await annotate_database(database, lance=True)
        if not annotations:
            logger.info(
                "Database '%s' already has up-to-date annotations, skipping save.",
                database,
            )
            continue

        save_tasks.append(asyncio.create_task(save_to_lance_db(annotations)))

    if save_tasks:
        await asyncio.gather(*save_tasks)
        clear_table_description_cache()
        logger.info("Finished persisting new table annotations.")
    else:
        logger.info("No new table annotations were generated.")


@lru_cache(maxsize=32)
def get_table_descriptions(database: str, tables: tuple[str]) -> list[TableDescription]:
    """
    Retrieve table descriptions for all tables in the specified database.
    If no descriptions exist, return None for each table.
    """
    db = get_lance_db()
    table_annotations = open_or_create_table(db, "table_annotations")

    result = (
        table_annotations.search()
        .where(f"database_name = '{database}'")
        .select(["table_name", "description"])
        .to_list()
    )

    if len(result) == 0:
        logger.warning(f"No table descriptions found for database: {database}")
        return [{"table_name": table, "description": None} for table in tables]

    tbl_description = [
        TableDescription(
            table_name=record["table_name"], description=record["description"]
        )
        for record in result
    ]

    return tbl_description


def clear_table_description_cache():
    """Clear the cached table descriptions."""
    get_table_descriptions.cache_clear()
