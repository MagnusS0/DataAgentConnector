from collections.abc import Iterator

import pytest

from app.models.lance import TableAnnotation
from app.repositories.lance_db import get_lance_db, open_or_create_table
from app.services.annotation_service import get_table_descriptions


@pytest.fixture(autouse=True)
def annotations() -> Iterator[None]:
    table = open_or_create_table(get_lance_db(), "table_annotations", TableAnnotation)
    rows = [
        ("shop", "main", "customers", "Customer records"),
        ("shop", "main", "orders", "Order lines"),
        ("shop", "main", "o'brien", "Quoted name"),
        ("other", "main", "customers", "WRONG DATABASE"),
    ]
    table.add(
        [
            TableAnnotation(
                database_name=db, schema_name=schema, table_name=name, description=d
            )
            for db, schema, name, d in rows
        ]
    )
    get_table_descriptions.cache_clear()
    yield
    table.delete("table_name IN ('customers', 'orders', 'o''brien')")
    get_table_descriptions.cache_clear()


def _as_dict(database: str, targets: tuple[tuple[str, str], ...]) -> dict:
    return {
        (d["schema_name"], d["table_name"]): d["description"]
        for d in get_table_descriptions(database, targets)
    }


def test_returns_only_requested_tables_of_right_database() -> None:
    result = _as_dict("shop", (("main", "customers"),))
    assert result == {("main", "customers"): "Customer records"}


def test_missing_entry_is_none() -> None:
    result = _as_dict("shop", (("main", "orders"), ("main", "products")))
    assert result[("main", "orders")] == "Order lines"
    assert result[("main", "products")] is None


def test_quote_in_table_name() -> None:
    assert _as_dict("shop", (("main", "o'brien"),)) == {
        ("main", "o'brien"): "Quoted name"
    }


def test_schema_must_match() -> None:
    assert _as_dict("shop", (("public", "customers"),)) == {
        ("public", "customers"): None
    }


def test_empty_targets() -> None:
    assert get_table_descriptions("shop", ()) == []
