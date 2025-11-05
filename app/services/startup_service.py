import asyncio

from app.core.config import get_settings
from app.core.logging import get_logger
from app.core.db_registry import DatabaseRegistry
from app.repositories.sql_db import connection_scope, list_schemas, get_inspector

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
        schemas, default_schema = result
        registry.set_schema_names(name, schemas)
        if default_schema is not None:
            registry.set_default_schema(name, default_schema)
        logger.debug(
            "Database '%s' schemas initialized: %s (default: %s)",
            name,
            schemas,
            default_schema,
        )


def _resolve_schemas_for_database(
    registry: DatabaseRegistry, database: str
) -> tuple[tuple[str, ...], str | None]:
    config = registry.get(database)
    with connection_scope(database) as conn:
        inspector = get_inspector(conn)
        raw_schemas = tuple(dict.fromkeys(list_schemas(conn)))
        discovered = tuple(
            schema
            for schema in raw_schemas
            if schema is not None and str(schema).strip() != ""
        )
        default_schema = inspector.default_schema_name or None
        if isinstance(default_schema, str) and default_schema.strip() == "":
            default_schema = None

    if not discovered and default_schema:
        discovered = (default_schema,)

    if not discovered:
        logger.warning(
            "No schemas discovered for database '%s'; downstream components may fail.",
            database,
        )
        raise ValueError(f"No schemas discovered for database '{database}'.")

    configured = config.schemas
    if configured is None:
        schemas = discovered
    else:
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

        schemas = matched

    if not schemas:
        raise ValueError(
            f"No schemas available for database '{database}' after applying configuration."
        )

    if default_schema in schemas:
        resolved_default = default_schema
    elif len(schemas) == 1:
        resolved_default = schemas[0]
    else:
        if default_schema is not None:
            logger.warning(
                "Database '%s': default schema '%s' is not available; falling back to '%s'.",
                database,
                default_schema,
                schemas[0],
            )
        resolved_default = schemas[0]

    return schemas, resolved_default
