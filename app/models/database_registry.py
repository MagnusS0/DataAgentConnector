from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field


class DatabaseConfig(BaseModel):
    """Describes a single database connection."""

    url: str = Field(description="SQLAlchemy-compatible connection string.")
    description: str = Field(
        default="", description="Short description of the dataset/schema."
    )


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