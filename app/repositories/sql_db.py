from collections.abc import Generator, Sequence
from contextlib import contextmanager
from functools import cache

from sqlalchemy import column, create_engine, event, inspect, select, table, text
from sqlalchemy.engine import Connection, Engine, Inspector, RowMapping
from sqlalchemy.exc import NoSuchTableError, ProgrammingError, StatementError

from app.core.config import get_settings
from app.core.db_registry import DatabaseRegistry, DatabaseConfig, TableMetadata


def get_registry() -> DatabaseRegistry:
    registry = get_settings().databases
    if registry is None:
        raise RuntimeError("Database registry is not configured.")
    return registry


@cache
def get_engine(database: str) -> Engine:
    registry = get_registry()
    config: DatabaseConfig = registry.get(database)
    engine = create_engine(config.url, pool_pre_ping=True)

    settings = get_settings()
    allowed_commands = settings.allowed_sql_commands

    @event.listens_for(engine, "before_cursor_execute")
    def enforce_read_only(conn, cursor, statement, parameters, context, executemany):
        """Enforce read-only access by blocking non-SELECT statements."""
        normalized = statement.strip().upper()

        if not any(normalized.startswith(keyword) for keyword in allowed_commands):
            raise StatementError(
                "The operation is not allowed. only the following SQL commands are permitted: "
                + ", ".join(allowed_commands),
                statement,
                parameters,
                orig=None,
            )

    return engine


@contextmanager
def connection_scope(
    database: str, engine: Engine | None = None
) -> Generator[Connection, None, None]:
    target_engine = engine or get_engine(database)
    with target_engine.connect() as conn:
        yield conn


def get_inspector(conn: Connection) -> Inspector:
    return inspect(conn)


def list_schemas(conn: Connection) -> list[str]:
    return get_inspector(conn).get_schema_names()


def list_tables(conn: Connection, schema: str | None = None) -> list[str]:
    return get_inspector(conn).get_table_names(schema=schema)


def list_views(conn: Connection, schema: str | None = None) -> list[str]:
    return get_inspector(conn).get_view_names(schema=schema)


def get_table_metadata(
    conn: Connection, table_name: str, schema: str | None = None
) -> TableMetadata:
    inspector = get_inspector(conn)
    if not inspector.has_table(table_name, schema=schema):
        raise ValueError(f'Table "{table_name}" does not exist.')
    raw_metadata = {
        "columns": inspector.get_columns(table_name, schema=schema),
        "primary_keys": inspector.get_pk_constraint(table_name, schema=schema),
        "foreign_keys": inspector.get_foreign_keys(table_name, schema=schema),
        "indexes": inspector.get_indexes(table_name, schema=schema),
    }

    metadata = TableMetadata.from_sqlalchemy(raw_metadata)
    return metadata


def get_table_preview(
    conn: Connection,
    table_name: str,
    schema: str | None = None,
    limit: int = 5,
    max_field_length: int = 150,
) -> list[dict]:
    """Get a preview of table data"""
    inspector = get_inspector(conn)
    try:
        columns = inspector.get_columns(table_name, schema=schema)
    except NoSuchTableError as exc:
        raise ValueError(f'Table "{table_name}" does not exist.') from exc

    if not columns:
        return []

    skip_types = {
        "BLOB",
        "BINARY",
        "VARBINARY",
        "BYTEA",
        "RAW",
        "LONGVARBINARY",
        "IMAGE",
    }
    preview_columns = [
        col for col in columns if str(col["type"]).upper() not in skip_types
    ]

    if not preview_columns:
        return []

    tbl = table(
        table_name, *[column(col["name"]) for col in preview_columns], schema=schema
    )

    stmt = select(tbl).limit(limit)
    try:
        result = conn.execute(stmt)
    except ProgrammingError as exc:
        raise ValueError(f'Failed to preview table "{table_name}": {exc.orig}') from exc
    rows = result.mappings().all()

    return [
        {key: _truncate_value(value, max_field_length) for key, value in row.items()}
        for row in rows
    ]


def get_view_definition(
    conn: Connection, view_name: str, schema: str | None = None
) -> str:
    inspector = get_inspector(conn)
    try:
        view_definition = inspector.get_view_definition(view_name, schema=schema)
    except NoSuchTableError as exc:
        raise ValueError(f'View "{view_name}" does not exist.') from exc
    if view_definition is None:
        raise ValueError(f'View definition for "{view_name}" could not be retrieved.')
    return view_definition


def get_distinct_column_values(
    conn: Connection,
    table_name: str,
    column_name: str,
    schema: str | None = None,
    limit: int = 500,
) -> list[str]:
    """Get distinct non-null values from a specified column up to a limit."""
    tbl = table(table_name, column(column_name), schema=schema)
    col = tbl.c[column_name]
    stmt = select(col).where(col.is_not(None)).distinct().limit(limit)
    try:
        result = conn.execute(stmt)
    except ProgrammingError as exc:
        raise ValueError(
            f'Failed to get column details for "{table_name}.{column_name}": {exc.orig}'
        ) from exc

    return list(result.scalars())


def get_fk_graph(
    conn: Connection, schema: str | None = None
) -> list[tuple[str | None, list[tuple[str, str | None]]]]:
    inspector = get_inspector(conn)
    return inspector.get_sorted_table_and_fkc_names(schema=schema)


def execute_select(
    conn: Connection, query: str, limit: int | None = None
) -> Sequence[RowMapping]:
    result = conn.execute(text(query))
    mappings = result.mappings()
    if limit is not None:
        return mappings.fetchmany(limit)
    return mappings.all()


def list_databases() -> list[dict[str, str | list[str]]]:
    """List available databases with their descriptions and schemas."""
    dbs = get_registry().summary()
    dbs = [
        {
            **db,
            "schemas": list_schemas_for(db["name"]),
        }
        for db in dbs
    ]
    return dbs


def _truncate_value(value, max_length: int = 150):
    """Truncate string values that exceed max_length."""
    if isinstance(value, str) and len(value) > max_length:
        return value[:max_length] + "..."
    return value


def list_schemas_for(database: str) -> list[str]:
    return list(get_registry().schemas_for(database))
