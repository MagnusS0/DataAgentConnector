from functools import lru_cache
from typing import Sequence

from app.repositories.sql_db import connection_scope, get_inspector


class SqlForeignKeyInfo:
    """Adapter for SQLAlchemy Inspector to provide FK metadata for domain layer."""

    def __init__(self, database: str):
        self.database = database
        self._tables: list[str] | None = None
        self._fk_by_table: dict[str, Sequence[dict]] | None = None

    def __hash__(self) -> int:
        return hash(self.database)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SqlForeignKeyInfo):
            return NotImplemented
        return self.database == other.database

    def get_table_names(self) -> list[str]:
        """Return all table names in the database."""
        self._ensure_metadata_loaded()
        # Return a shallow copy to keep internal cache immutable
        return list(self._tables or [])

    def get_foreign_keys(self, table_name: str) -> list[dict]:
        """Return foreign key constraints for a table."""
        self._ensure_metadata_loaded()
        if not self._fk_by_table:
            return []
        return [fk.copy() for fk in self._fk_by_table.get(table_name, [])]

    def refresh(self) -> None:
        """Reload FK metadata from the underlying database."""
        tables, fk_by_table = _load_metadata(self.database)
        self._tables = tables
        self._fk_by_table = fk_by_table

    def _ensure_metadata_loaded(self) -> None:
        if self._tables is None or self._fk_by_table is None:
            self.refresh()


@lru_cache(maxsize=16)
def get_fk_info(database: str) -> SqlForeignKeyInfo:
    """Get cached FK info provider for a database."""
    return SqlForeignKeyInfo(database)


def clear_cached_fk_info(database: str | None = None) -> None:
    """Invalidate cached FK metadata adapters."""
    if database is None:
        get_fk_info.cache_clear()
        return

    try:
        info = get_fk_info(database)
    except KeyError:
        return
    info.refresh()


def _load_metadata(database: str) -> tuple[list[str], dict[str, Sequence[dict]]]:
    """Load table names and foreign key metadata."""
    with connection_scope(database) as conn:
        insp = get_inspector(conn)
        tables = list(insp.get_table_names())
        fk_by_table: dict[str, Sequence[dict]] = {}
        for table_name in tables:
            fks = insp.get_foreign_keys(table_name) or []
            fk_by_table[table_name] = [dict(fk) for fk in fks]
    return tables, fk_by_table
