from pydantic import BaseModel, Field, field_validator


class BaseDescription(BaseModel):
    description: str = Field(
        ...,
        description="A brief but detailed summary of a table's contents and potential uses.",
    )

    max_words: int = 100

    @field_validator("description")
    @classmethod
    def check_word_count(cls, v):
        words = v.split()
        if len(words) > cls.max_words:
            raise ValueError(
                f"Description must be at most {cls.max_words} words, please shorten it."
            )
        return v


class TableDescription(BaseDescription):
    pass


class DatabaseDescription(BaseDescription):
    description: str = Field(
        ...,
        description="A brief but detailed summary of a database's contents and potential uses.",
    )
    max_words: int = 200
