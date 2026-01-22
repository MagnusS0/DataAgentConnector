"""Column masking for PII protection.

This module provides functionality to mask sensitive column values based on
configuration-driven rules. Masked columns are excluded from FTS indexing
and their values are redacted in all exposed outputs.
"""

from collections.abc import Sequence
from fnmatch import fnmatch
from functools import lru_cache
from hashlib import sha256
from typing import Any

from app.core.config import get_settings
from app.schemas.config import AnonymizationConfig, MaskingStrategy


class ColumnMasker:
    """Masks sensitive column values based on anonymization configuration."""

    def __init__(self, config: AnonymizationConfig | None):
        self._config = config or AnonymizationConfig()
        self._masked_columns_lower: frozenset[str] = frozenset(
            col.lower() for col in self._config.masked_columns
        )
        self._patterns_lower: tuple[str, ...] = tuple(
            p.lower() for p in self._config.masked_patterns
        )

    @property
    def enabled(self) -> bool:
        """Check if anonymization is enabled."""
        return self._config.enabled

    @property
    def mask_value(self) -> str:
        """Get the mask value used for redaction."""
        return self._config.mask_value

    @property
    def strategy(self) -> MaskingStrategy:
        """Get the masking strategy."""
        return self._config.strategy

    def is_column_masked(
        self,
        column_name: str,
        *,
        table_name: str | None = None,
        schema_name: str | None = None,
    ) -> bool:
        """Check if a column should be masked.

        Args:
            column_name: Name of the column to check.
            table_name: Optional table name (reserved for future per-table rules).
            schema_name: Optional schema name (reserved for future per-schema rules).

        Returns:
            True if the column should be masked, False otherwise.
        """
        if not self._config.enabled:
            return False

        col_lower = column_name.lower()

        # Exact match
        if col_lower in self._masked_columns_lower:
            return True

        # Glob patterns
        for pattern in self._patterns_lower:
            if fnmatch(col_lower, pattern):
                return True

        return False

    def get_masked_columns(self, columns: Sequence[str]) -> frozenset[str]:
        """Get the set of columns that should be masked.

        Args:
            columns: Sequence of column names to check.

        Returns:
            Frozenset of column names that should be masked.
        """
        return frozenset(col for col in columns if self.is_column_masked(col))

    def mask_value_single(self, value: Any, column_name: str) -> Any:
        """Mask a single value if its column is masked.

        Args:
            value: The value to potentially mask.
            column_name: Name of the column.

        Returns:
            The masked value or the original value if not masked.
        """
        if value is None:
            return None

        if not self.is_column_masked(column_name):
            return value

        if self._config.strategy == MaskingStrategy.hash:
            # Use SHA256 hash prefix for deterministic masking (enables JOINs)
            str_value = str(value)
            hash_prefix = sha256(str_value.encode()).hexdigest()[:16]
            return f"hash:{hash_prefix}"

        # Default: redact strategy
        return self._config.mask_value

    def mask_row(self, row: dict[str, Any]) -> dict[str, Any]:
        """Mask sensitive values in a row dictionary.

        Args:
            row: Dictionary representing a database row.

        Returns:
            New dictionary with masked values where applicable.
        """
        if not self._config.enabled:
            return row

        return {key: self.mask_value_single(value, key) for key, value in row.items()}

    def mask_rows(self, rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
        """Mask sensitive values in multiple rows.

        Args:
            rows: Sequence of row dictionaries.

        Returns:
            List of dictionaries with masked values where applicable.
        """
        if not self._config.enabled:
            return list(rows)

        return [self.mask_row(row) for row in rows]


@lru_cache(maxsize=32)
def get_column_masker(database: str) -> ColumnMasker:
    """Get a cached ColumnMasker instance for a database.

    Args:
        database: Name of the database.

    Returns:
        ColumnMasker instance configured for the database.
    """
    settings = get_settings()
    if settings.databases is None:
        return ColumnMasker(None)

    try:
        db_config = settings.databases.get(database)
        return ColumnMasker(db_config.anonymization)
    except ValueError:
        return ColumnMasker(None)


def should_exclude_from_index(
    database: str,
    column_name: str,
    *,
    table_name: str | None = None,
    schema_name: str | None = None,
) -> bool:
    """Check if a column should be excluded from FTS indexing.

    Args:
        database: Name of the database.
        column_name: Name of the column.
        table_name: Optional table name.
        schema_name: Optional schema name.

    Returns:
        True if the column should be excluded from indexing.
    """
    masker = get_column_masker(database)
    return masker.is_column_masked(
        column_name, table_name=table_name, schema_name=schema_name
    )


def clear_masker_cache() -> None:
    """Clear the cached ColumnMasker instances."""
    get_column_masker.cache_clear()
