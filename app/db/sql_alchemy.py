from __future__ import annotations

from collections.abc import Generator, Sequence
from contextlib import contextmanager
from functools import cache

from sqlalchemy import create_engine, event, inspect, select, table, text
from sqlalchemy.engine import Connection, Engine, Inspector, RowMapping
from sqlalchemy.exc import StatementError

from app.core.config import get_settings
from app.models.database_registry import DatabaseRegistry, DatabaseConfig


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


def get_table_metadata(conn: Connection, table_name: str) -> dict:
    inspector = get_inspector(conn)
    return {
        "columns": str(inspector.get_columns(table_name)),
        "primary_keys": inspector.get_pk_constraint(table_name),
        "foreign_keys": inspector.get_foreign_keys(table_name),
        "indexes": inspector.get_indexes(table_name),
    }


def get_table_preview(
    conn: Connection, table_name: str, limit: int = 5
) -> Sequence[RowMapping]:
    """Get a preview of table data using database-agnostic SQLAlchemy constructs."""
    # Reflect the table from the database
    tbl = table(table_name)

    # Use SQLAlchemy's select with limit for database-agnostic query
    stmt = select(tbl).limit(limit)
    result = conn.execute(stmt)
    return result.mappings().all()


def get_fk_graph(conn: Connection):
    inspector = get_inspector(conn)
    return inspector.get_sorted_table_and_fkc_names()


def execute_select(conn: Connection, query: str) -> Sequence[RowMapping]:
    result = conn.execute(text(query))
    return result.mappings().all()


def list_databases() -> list[dict[str, str]]:
    return get_registry().summary()
