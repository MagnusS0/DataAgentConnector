from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from sqlalchemy import column, select, table
from sqlalchemy.engine import Connection
from sqlalchemy.sql.sqltypes import (
    JSON,
    LargeBinary,
    NullType,
    String,
    Text,
    Unicode,
    UnicodeText,
    BLOB,
    BINARY,
    VARBINARY,
    Date,
)

from app.repositories.sql_db import connection_scope, get_inspector
from app.models.lance import ColumnContent
from app.core.logging import get_logger
from app.core.config import get_settings
from app.schemas.config import ExtractionOptions

logger = get_logger(__name__)

_TEXTUAL_TYPES = (
    String,
    Text,
    Unicode,
    UnicodeText,
)

_SKIP_TYPES = (
    LargeBinary,
    JSON,
    NullType,
    BLOB,
    BINARY,
    VARBINARY,
    Date,
)


def extract_column_contents(
    database: str,
    *,
    options: ExtractionOptions | None = None,
) -> list[ColumnContent]:
    """Extract column contents from all tables in the specified database.

    Args:
        database: The name of the database to extract from.
        options: ExtractionOptions for tuning the extraction process.
    Returns:
        A list of ColumnContent objects representing the extracted contents.
    """
    options = options or get_settings().fts_extraction_options

    with connection_scope(database) as conn:
        inspector = get_inspector(conn)
        tables = inspector.get_table_names()

        table_columns = {
            table_name: inspector.get_columns(table_name) for table_name in tables
        }

        tasks: list[tuple[str, str]] = []
        for table_name, columns in table_columns.items():
            for col in columns:
                if _is_textual_column(col["type"]):
                    tasks.append((table_name, col["name"]))

        if not tasks:
            logger.warning("No textual columns discovered in database '%s'.", database)
            return []

        max_workers = min(max(1, options.max_workers), len(tasks))
        results: list[ColumnContent] = []
        total_skipped = 0

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(
                    _collect_column_values,
                    database,
                    table_name,
                    column_name,
                    options,
                ): (table_name, column_name)
                for table_name, column_name in tasks
            }

            for future in as_completed(futures):
                table_name, column_name = futures[future]
                try:
                    column_content = future.result()
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "Failed to extract values for %s.%s", table_name, column_name
                    )
                    continue

                if column_content is None:
                    total_skipped += 1
                    continue

                results.append(column_content)

        logger.debug(
            "Extracted textual values for %d columns (skipped %d) in database '%s'.",
            len(results),
            total_skipped,
            database,
        )
        return results


def _collect_column_values(
    database: str,
    table_name: str,
    column_name: str,
    options: ExtractionOptions,
) -> ColumnContent | None:
    """Collect and clean distinct string values from a specific column."""
    with connection_scope(database) as conn:
        distinct_values = _fetch_distinct_strings(
            conn,
            table_name,
            column_name,
            limit=options.max_values_per_column + 1,
        )

        if len(distinct_values) > options.max_values_per_column:
            logger.debug(
                f"Skipping {table_name}.{column_name}: "
                f"more than {options.max_values_per_column} distinct values"
            )
            return None

    filtered = [
        value
        for value in (_clean_value(raw, options) for raw in distinct_values)
        if value is not None
    ]
    if not filtered:
        return None

    return ColumnContent(
        database_name=database,
        table_name=table_name,
        column_name=column_name,
        content=filtered,
        num_distinct=len(filtered),
    )


def _fetch_distinct_strings(
    conn: Connection,
    table_name: str,
    column_name: str,
    *,
    limit: int,
) -> list[Any]:
    """Fetch distinct non-null values from a specified column up to a limit."""
    tbl = table(table_name, column(column_name))
    col = tbl.c[column_name]
    stmt = select(col).where(col.is_not(None)).distinct().limit(limit)

    result = conn.execute(stmt)
    return list(result.scalars())


def _clean_value(value: Any, options: ExtractionOptions) -> str | None:
    """Clean and validate a raw column value."""
    if value is None:
        return None

    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    elif not isinstance(value, str):
        value = str(value)

    trimmed = value.strip()
    if not trimmed:
        return None
    if not (options.min_length <= len(trimmed) <= options.max_length):
        return None

    if _looks_numeric(trimmed):
        return None

    return trimmed


def _is_textual_column(sql_type) -> bool:
    if isinstance(sql_type, _SKIP_TYPES):
        return False
    return isinstance(sql_type, _TEXTUAL_TYPES)


def _looks_numeric(value: str) -> bool:
    try:
        float(value)
        return True
    except ValueError:
        return False
