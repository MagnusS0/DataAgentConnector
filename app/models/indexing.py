from pydantic import BaseModel, ConfigDict, Field, model_validator


class ExtractionOptions(BaseModel):
    max_workers: int = Field(default=8, ge=1)
    max_values_per_column: int = Field(default=1000, ge=1)
    min_length: int = Field(default=2, ge=0)
    max_length: int = Field(default=40, ge=1)

    model_config = ConfigDict(frozen=True)

    @model_validator(mode="after")
    def ensure_length_bounds(self) -> "ExtractionOptions":
        if self.min_length > self.max_length:
            raise ValueError("min_length must be <= max_length")
        return self
