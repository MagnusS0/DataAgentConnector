import pytest
from sqlalchemy.exc import StatementError

from app.repositories.sql_db import (
    connection_scope,
    execute_select,
    get_distinct_column_values,
    get_table_metadata,
    list_tables,
    list_views,
)


def test_select_works() -> None:
    with connection_scope("shop") as conn:
        rows = execute_select(conn, "SELECT name FROM customers ORDER BY id")
    assert [r["name"] for r in rows] == ["Alice", "Bob", "Carl", "Dan O'Brien"]


def test_select_limit() -> None:
    with connection_scope("shop") as conn:
        rows = execute_select(conn, "SELECT * FROM customers", limit=2)
    assert len(rows) == 2


@pytest.mark.parametrize(
    "query",
    ["DELETE FROM customers", "DROP TABLE customers", "UPDATE customers SET city='x'"],
)
def test_write_statements_are_blocked(query: str) -> None:
    with connection_scope("shop") as conn, pytest.raises(StatementError):
        execute_select(conn, query)


def test_lists_tables_and_views() -> None:
    with connection_scope("shop") as conn:
        assert {"customers", "products", "orders"} <= set(list_tables(conn))
        assert list_views(conn) == ["v_orders"]


def test_create_table_includes_keys() -> None:
    with connection_scope("shop") as conn:
        ddl = get_table_metadata(conn, "orders").to_create_table("orders")
    assert "CREATE TABLE orders (" in ddl
    assert "PRIMARY KEY (id)" in ddl
    assert "FOREIGN KEY (customer_id) REFERENCES customers(id)" in ddl
    assert "FOREIGN KEY (product_id) REFERENCES products(id)" in ddl


def test_missing_table_raises() -> None:
    with connection_scope("shop") as conn, pytest.raises(ValueError, match="nope"):
        get_table_metadata(conn, "nope")


def test_distinct_values_are_unique_and_non_null() -> None:
    with connection_scope("shop") as conn:
        values = get_distinct_column_values(conn, "customers", "city")
    assert sorted(values) == ["Bergen", "Oslo", "São Paulo"]
