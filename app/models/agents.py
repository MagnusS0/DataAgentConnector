from pydantic import BaseModel, Field, field_validator


class BaseDescription(BaseModel):
    description: str = Field(
        ...,
        description="A brief but detailed summary of a table's contents and potential uses.",
    )

    @field_validator("description")
    @classmethod
    def check_word_count(cls, v, info):
        words = v.split()
        max_words = info.data.get("max_words", 100)
        if len(words) > max_words:
            raise ValueError(
                f"Description must be at most {max_words} words, please shorten it."
            )
        return v


class TableDescription(BaseDescription):
    max_words: int = 100


class DatabaseDescription(BaseDescription):
    description: str = Field(
        ...,
        description="A brief but detailed summary of a database's contents and potential uses.",
    )
    max_words: int = 200
