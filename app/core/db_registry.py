from pathlib import Path

from pydantic import BaseModel
from app.schemas.config import DatabaseConfig


class DatabaseRegistry(BaseModel):
    """Collection of named database configurations."""

    databases: dict[str, DatabaseConfig]

    def names(self) -> list[str]:
        return list(self.databases.keys())

    def summary(self) -> list[dict[str, str]]:
        return [
            {"name": name, "description": cfg.description}
            for name, cfg in self.databases.items()
        ]

    def get(self, name: str) -> DatabaseConfig:
        try:
            return self.databases[name]
        except KeyError as exc:
            available = ", ".join(self.names())
            raise ValueError(
                f"Unknown database '{name}'. Available: {available}"
            ) from exc

    @classmethod
    def from_toml(cls, path: Path) -> "DatabaseRegistry":
        import tomllib

        data = tomllib.loads(path.read_text())
        return cls(**data)


class TableMetadata(BaseModel):
    columns: list[dict]
    primary_keys: dict
    foreign_keys: list[dict]
    indexes: list[dict]

    @classmethod
    def from_sqlalchemy(cls, raw: dict) -> "TableMetadata":
        def serialize_column(col: dict) -> dict:
            col = col.copy()
            if "type" in col:
                col["type"] = str(col["type"])
            return col

        columns = [serialize_column(c) for c in raw.get("columns", [])]
        primary_keys = raw.get("primary_keys", {})
        foreign_keys = raw.get("foreign_keys", [])
        indexes = raw.get("indexes", [])

        return cls(
            columns=columns,
            primary_keys=primary_keys,
            foreign_keys=foreign_keys,
            indexes=indexes,
        )

    def to_create_table(self, table_name: str, include_indexes: bool = True) -> str:
        """Generate CREATE TABLE statement representation for LLM consumption.

        Args:
            table_name: Name of the table
            include_indexes: If True, append CREATE INDEX statements after the table
        """
        column_defs = [
            f"  {col['name']} {col.get('type', 'UNKNOWN')}"
            + ("" if col.get("nullable", True) else " NOT NULL")
            for col in self.columns
        ]

        # PK constraint
        if pk_cols := self.primary_keys.get("constrained_columns", []):
            column_defs.append(f"  PRIMARY KEY ({', '.join(pk_cols)})")

        # FK constraints
        column_defs.extend(
            f"  FOREIGN KEY ({', '.join(fk['constrained_columns'])}) "
            f"REFERENCES {fk['referred_table']}({', '.join(fk['referred_columns'])})"
            for fk in self.foreign_keys
            if fk.get("constrained_columns")
            and fk.get("referred_table")
            and fk.get("referred_columns")
        )

        parts = [f"CREATE TABLE {table_name} (", ",\n".join(column_defs), ");"]

        if include_indexes and self.indexes:
            parts.extend(
                f"\nCREATE {'UNIQUE ' if idx.get('unique') else ''}INDEX {idx['name']} "
                f"ON {table_name} ({', '.join(idx['column_names'])});"
                for idx in self.indexes
                if idx.get("column_names")
            )

        return "\n".join(parts)
