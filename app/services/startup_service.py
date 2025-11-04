import asyncio

from app.core.config import get_settings
from app.core.logging import get_logger
from app.core.db_registry import DatabaseRegistry
from app.repositories.sql_db import connection_scope, list_schemas

logger = get_logger(__name__)


async def initialize_database_schemas() -> None:
    """Populate the database registry with discovered schema names."""
    settings = get_settings()
    registry = settings.databases
    if registry is None:
        logger.info("No database registry configured; skipping schema discovery.")
        return

    database_names = registry.names()
    tasks = [
        asyncio.to_thread(_resolve_schemas_for_database, registry, name)
        for name in database_names
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for name, result in zip(database_names, results, strict=True):
        if isinstance(result, BaseException):
            raise result
        registry.set_schema_names(name, result)
        logger.debug("Database '%s' schemas initialized: %s", name, result)


def _resolve_schemas_for_database(
    registry: DatabaseRegistry, database: str
) -> tuple[str, ...]:
    config = registry.get(database)
    with connection_scope(database) as conn:
        discovered = tuple(dict.fromkeys(list_schemas(conn)))

    if not discovered:
        logger.warning(
            "No schemas discovered for database '%s'; downstream components may fail.",
            database,
        )
        return ()

    configured = config.schemas
    if configured is None:
        return discovered

    matched = tuple(name for name in configured if name in discovered)
    missing = tuple(name for name in configured if name not in discovered)

    if missing:
        logger.warning(
            "Database '%s': configured schemas not found and will be ignored: %s",
            database,
            missing,
        )

    if not matched:
        raise ValueError(
            f"Configured schemas for database '{database}' did not match any discovered schemas."
        )

    return matched
