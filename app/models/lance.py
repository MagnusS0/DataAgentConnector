from hashlib import sha256
from typing import Optional

from lancedb.pydantic import LanceModel, Vector
from pydantic import field_validator

from app.core.config import get_settings

settings = get_settings()


class TableAnnotation(LanceModel):
    """Schema for table annotations stored in LanceDB."""

    database_name: str
    table_name: str
    description: str
    embeddings: Optional[Vector(settings.n_dims)] = None  # type: ignore[valid-type]
    metadata_json: str = ""
    schema_hash: str | None = None

    @field_validator("schema_hash", mode="before")
    @classmethod
    def compute_schema_hash(cls, v, info):
        """Compute schema hash if not provided."""
        if v:
            return v
        data = info.data
        hash_input = (
            f"{data['database_name']}.{data['table_name']}.{data['metadata_json']}"
        )
        return sha256(hash_input.encode("utf-8")).hexdigest()


class ColumnContent(LanceModel):
    """Schema for column content values"""

    id_hash: str | None = None
    database_name: str
    table_name: str
    column_name: str
    content: list[str]
    num_distinct: int

    @field_validator("id_hash", mode="before")
    @classmethod
    def compute_id_hash(cls, v, info):
        """Compute id_hash if not provided."""
        if v:
            return v
        data = info.data
        hash_input = (
            f"{data['database_name']}."
            f"{data['table_name']}."
            f"{data['column_name']}."
            f"{data['content']}"
        )
        return sha256(hash_input.encode("utf-8")).hexdigest()
