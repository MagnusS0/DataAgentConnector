from collections.abc import Generator, Sequence
from contextlib import contextmanager
from functools import cache

from sqlalchemy import column, create_engine, event, inspect, select, table, text
from sqlalchemy.engine import Connection, Engine, Inspector, RowMapping
from sqlalchemy.exc import NoSuchTableError, ProgrammingError, StatementError

from app.core.config import get_settings
from app.models.database_registry import DatabaseRegistry, DatabaseConfig, TableMetadata


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


def list_tables(conn: Connection) -> list[str]:
    return get_inspector(conn).get_table_names()

def list_views(conn: Connection) -> list[str]:
    return get_inspector(conn).get_view_names()

def get_table_metadata(conn: Connection, table_name: str) -> TableMetadata:
    inspector = get_inspector(conn)
    if not inspector.has_table(table_name):
        raise ValueError(f'Table "{table_name}" does not exist.')
    raw_metadata = {
        "columns": inspector.get_columns(table_name),
        "primary_keys": inspector.get_pk_constraint(table_name),
        "foreign_keys": inspector.get_foreign_keys(table_name),
        "indexes": inspector.get_indexes(table_name),
    }

    metadata = TableMetadata.from_sqlalchemy(raw_metadata)
    return metadata


def get_table_preview(
    conn: Connection, table_name: str, limit: int = 5
) -> Sequence[RowMapping]:
    """Get a preview of table data"""
    inspector = get_inspector(conn)
    try:
        columns = inspector.get_columns(table_name)
    except NoSuchTableError as exc:
        raise ValueError(f'Table "{table_name}" does not exist.') from exc

    if not columns:
        return []

    tbl = table(table_name, *[column(col["name"]) for col in columns])

    stmt = select(tbl).limit(limit)
    try:
        result = conn.execute(stmt)
    except ProgrammingError as exc:
        raise ValueError(f'Failed to preview table "{table_name}": {exc.orig}') from exc
    return result.mappings().all()

def get_veiw_definition(conn: Connection, view_name: str) -> str:
    inspector = get_inspector(conn)
    try:
        view_definition = inspector.get_view_definition(view_name)
    except NoSuchTableError as exc:
        raise ValueError(f'View "{view_name}" does not exist.') from exc
    if view_definition is None:
        raise ValueError(f'View definition for "{view_name}" could not be retrieved.')
    return view_definition

def get_fk_graph(conn: Connection):
    inspector = get_inspector(conn)
    return inspector.get_sorted_table_and_fkc_names()


def execute_select(
    conn: Connection, query: str, limit: int | None = None
) -> Sequence[RowMapping]:
    result = conn.execute(text(query))
    mappings = result.mappings()
    if limit is not None:
        return mappings.fetchmany(limit)
    return mappings.all()


def list_databases() -> list[dict[str, str]]:
    return get_registry().summary()
