import asyncio
from pathlib import Path

import lancedb
import pytest
from lancedb.pydantic import LanceModel

from app.models.lance import ColumnContent
from app.repositories.column_contents import ColumnContentRepository
from app.repositories.lance_db import (
    batch_insert,
    get_lance_db,
    open_or_create_table,
    sql_literal,
)


class Narrow(LanceModel):
    name: str


class Wide(LanceModel):
    name: str
    extra: str | None = None


def test_sql_literal_escapes_quotes() -> None:
    assert sql_literal("O'Brien") == "'O''Brien'"
    assert sql_literal("plain") == "'plain'"


def test_creates_when_missing(tmp_path: Path) -> None:
    db = lancedb.connect(tmp_path)
    table = open_or_create_table(db, "t", Narrow)
    assert table.count_rows() == 0
    assert "t" in db.list_tables().tables


def test_opens_when_present(tmp_path: Path) -> None:
    db = lancedb.connect(tmp_path)
    open_or_create_table(db, "t", Narrow).add([Narrow(name="a")])
    assert open_or_create_table(db, "t", Narrow).count_rows() == 1


def test_missing_without_schema_raises(tmp_path: Path) -> None:
    db = lancedb.connect(tmp_path)
    with pytest.raises(ValueError):
        open_or_create_table(db, "absent")


def test_opens_table_with_extra_column(tmp_path: Path) -> None:
    db = lancedb.connect(tmp_path)
    db.create_table("t", schema=Wide).add([Wide(name="a", extra="x")])

    table = open_or_create_table(db, "t", Narrow)
    table.add([Narrow(name="b")])

    assert table.count_rows() == 2


def test_overwrite_keeps_every_batch(tmp_path: Path) -> None:
    db = lancedb.connect(tmp_path)
    table = open_or_create_table(db, "t", Narrow)
    table.add([Narrow(name="old")])

    batch_insert(table, [Narrow(name=n) for n in "abc"], batch_size=2, mode="overwrite")

    assert sorted(r["name"] for r in table.search().to_list()) == ["a", "b", "c"]


def test_overwrite_replaces_an_older_schema() -> None:
    get_lance_db().create_table(
        "column_contents_upgrade", schema=Narrow, mode="overwrite"
    )
    content = ColumnContent(
        database_name="upgrade",
        schema_name="main",
        table_name="bands",
        column_name="name",
        content=["AC/DC"],
        num_distinct=1,
    )

    repo = ColumnContentRepository("upgrade")
    asyncio.run(repo.save_batch([content], mode="overwrite"))

    table = get_lance_db().open_table("column_contents_upgrade")
    assert table.search().to_list()[0]["ngram_content"] == ["AC/DC"]
