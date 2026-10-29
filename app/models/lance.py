import re
import unicodedata
from hashlib import sha256

from lancedb.pydantic import LanceModel
from pydantic import field_validator, model_validator


class TableAnnotation(LanceModel):
    """Schema for table annotations stored in LanceDB."""

    database_name: str
    schema_name: str
    table_name: str
    description: str
    metadata_json: str = ""
    schema_hash: str | None = None

    @field_validator("schema_hash", mode="before")
    @classmethod
    def compute_schema_hash(cls, v, info):
        """Compute schema hash if not provided."""
        if v:
            return v
        data = info.data
        hash_input = f"{data['database_name']}.{data['schema_name']}.{data['table_name']}.{data['metadata_json']}"
        return sha256(hash_input.encode("utf-8")).hexdigest()


def compact(value: str) -> str:
    """Fold case and accents and strip spaces and punctuation: "AC/DC" -> "acdc"."""
    decomposed = unicodedata.normalize("NFKD", value)
    unaccented = "".join(c for c in decomposed if not unicodedata.combining(c))
    return re.sub(r"[\W_]+", "", unaccented.casefold())


class ColumnContent(LanceModel):
    """Schema for column content values"""

    id_hash: str | None = None
    database_name: str
    schema_name: str
    table_name: str
    column_name: str
    content: list[str]
    # One compacted copy per value, aligned with `content`, for the n-gram index.
    compact_content: list[str] | None = None
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
            f"{data['schema_name']}."
            f"{data['table_name']}."
            f"{data['column_name']}."
            f"{data['content']}"
        )
        return sha256(hash_input.encode("utf-8")).hexdigest()

    @model_validator(mode="after")
    def compute_compact_content(self) -> "ColumnContent":
        """Derive compacted values from content if not provided."""
        if self.compact_content is None:
            self.compact_content = [compact(value) for value in self.content]
        return self
