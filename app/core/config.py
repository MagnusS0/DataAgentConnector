from functools import lru_cache
from pathlib import Path

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic_settings.sources import (
    PydanticBaseSettingsSource,
    PyprojectTomlConfigSettingsSource,
)

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
        pyproject_toml_depth=1,
        pyproject_toml_table_header=("tool", "dac", "settings"),
    )

    log_level: str = Field(default="INFO")
    log_folder: Path = Field(default=Path("logs"))

    llm_model_name: str | None = Field(
        default=None, validation_alias=AliasChoices("LLM_MODEL_NAME")
    )
    llm_base_url: str | None = Field(
        default=None, validation_alias=AliasChoices("LLM_BASE_URL")
    )
    llm_api_key: str | None = Field(
        default=None, validation_alias=AliasChoices("LLM_API_KEY")
    )

    databases_file: Path = Field(
        default=Path("databases.toml"),
        description="TOML file defining available databases.",
    )
    databases: DatabaseRegistry | None = None
    mcp_query_limit: int | None = Field(
        default=25,
        description="Force a limit on the number of rows returned by SQL queries on MCP tool calls.",
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
    max_concurrent_annotations: int = Field(
        default=32,
        description="Maximum number of concurrent table annotations to prevent resource exhaustion.",
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

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            env_settings,
            dotenv_settings,
            PyprojectTomlConfigSettingsSource(settings_cls),
            file_secret_settings,
            init_settings,
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

    @model_validator(mode="after")
    def validate_llm_config(self) -> "Settings":
        llm_fields = [self.llm_model_name, self.llm_api_key]
        if any(llm_fields) and not all(llm_fields):
            raise ValueError(
                "LLM_MODEL_NAME and LLM_API_KEY must both be set or both be None"
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
