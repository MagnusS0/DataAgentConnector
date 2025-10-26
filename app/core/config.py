from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.models.database_registry import DatabaseRegistry
from app.models.indexing import ExtractionOptions


class Settings(BaseSettings):
    """
    Settings for the application.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    log_level: str = Field(default="INFO")
    log_folder: Path = Field(default=Path("logs"))

    llm_model_name: str | None = Field(default=None, alias="LLM_MODEL_NAME")
    llm_base_url: str | None = Field(default=None, alias="LLM_BASE_URL")
    llm_api_key: str | None = Field(default=None, alias="LLM_API_KEY")

    databases_file: Path = Field(
        default=Path("databases.toml"),
        description="TOML file defining available databases.",
    )
    databases: DatabaseRegistry | None = None
    limit: int | None = Field(
        default=25,
        description="Maximum number of rows to return for SELECT queries.",
    )

    lance_db_path: Path = Field(
        default=Path("./data/lance_db"),
        description="Path to the LanceDB database for storing table annotations.",
    )
    n_dims: int = Field(
        default=768,
        description="Dimensionality of the embedding vectors.",
    )
    embedding_model_name: str = Field(
        default="google/embeddinggemma-300m",
        description="Name of the embedding model to use.",
    )
    device: str = Field(
        default="cpu",
        description="Device to run the embedding model on (e.g., 'cpu', 'cuda').",
    )

    annotate_on_startup: bool = Field(
        default=True,
        description="Whether to annotate tables on startup if no annotation exists.",
    )
    fts_extraction_options: ExtractionOptions = Field(
        default_factory=ExtractionOptions,
        description="Column-content extraction tuning for FTS indexing.",
    )

    allowed_sql_commands: tuple[str, ...] = Field(
        default=(
            "SELECT",
            "SHOW",
            "DESCRIBE",
            "DESC",
            "EXPLAIN",
            "WITH",
            "PRAGMA",
        ),
        description="SQL keywords that are allowed, defaults to read-only mode.",
    )

    @field_validator("databases_file")
    @classmethod
    def ensure_file_exists(cls, value: Path) -> Path:
        if not value.exists():
            raise FileNotFoundError(f"Databases file '{value}' is missing.")
        return value

    @field_validator("databases", mode="before")
    @classmethod
    def validate_registry(cls, value):
        if value is None or isinstance(value, DatabaseRegistry):
            return value
        raise TypeError("databases must be a DatabaseRegistry instance or None.")

    @model_validator(mode="after")
    def ensure_registry(self) -> "Settings":
        if self.databases is None:
            self.databases = DatabaseRegistry.from_toml(self.databases_file)
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
