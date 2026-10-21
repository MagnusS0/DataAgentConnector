from pathlib import Path

import lancedb
import pytest
from lancedb.pydantic import LanceModel

from app.repositories.lance_db import open_or_create_table, sql_literal


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
