"""Test bootstrap: configure env and a SQLite fixture DB before any `app` import."""

import asyncio
import os
import sqlite3
import tempfile
from pathlib import Path

import pytest

_ROOT = Path(tempfile.mkdtemp(prefix="dac-tests-"))
_SHOP_DB = _ROOT / "shop.db"

_SCHEMA = """
CREATE TABLE customers (
    id INTEGER PRIMARY KEY, name TEXT, city TEXT, ssn TEXT
);
CREATE TABLE products (
    id INTEGER PRIMARY KEY, title TEXT, price NUMERIC
);
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    product_id INTEGER NOT NULL REFERENCES products(id),
    qty INTEGER,
    created_at TEXT
);
CREATE VIEW v_orders AS
    SELECT o.id, c.name, p.title, o.qty
    FROM orders o
    JOIN customers c ON c.id = o.customer_id
    JOIN products p ON p.id = o.product_id;
"""


def _build_shop_db() -> None:
    conn = sqlite3.connect(_SHOP_DB)
    conn.executescript(_SCHEMA)
    conn.executemany(
        "INSERT INTO customers VALUES (?, ?, ?, ?)",
        [
            (1, "Alice", "Oslo", "111-11-1111"),
            (2, "Bob", "Bergen", "222-22-2222"),
            (3, "Carl", "Oslo", "333-33-3333"),
            (4, "Dan O'Brien", "São Paulo", "444-44-4444"),
        ],
    )
    conn.executemany(
        "INSERT INTO products VALUES (?, ?, ?)",
        [
            (1, "Red Bicycle", 199.5),
            (2, "Blue Helmet", 49),
            (3, "Metallica T-Shirt", 25),
        ],
    )
    conn.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
        [
            (1, 1, 1, 1, "2024-01-01"),
            (2, 2, 2, 2, "2024-01-02"),
            (3, 3, 3, 1, "2024-01-03"),
        ],
    )
    conn.commit()
    conn.close()


def _configure_env() -> None:
    _build_shop_db()
    databases_file = _ROOT / "databases.toml"
    databases_file.write_text(
        f"""
[databases.shop]
url = "sqlite:///{_SHOP_DB}"
description = "Test shop"

[databases.shop.anonymization]
masked_columns = ["ssn"]
"""
    )
    os.environ.update(
        DATABASES_FILE=str(databases_file),
        LANCE_DB_PATH=str(_ROOT / "lance"),
        LOG_FOLDER=str(_ROOT / "logs"),
        ANNOTATE_ON_STARTUP="false",
        LLM_API_KEY="test",
        LLM_MODEL_NAME="test",
        LOG_LEVEL="WARNING",
    )


_configure_env()


@pytest.fixture(scope="session", autouse=True)
def _startup_pipeline() -> None:
    """Run the real schema discovery and FTS indexing once per session."""
    from app.services.indexing_service import IndexingService
    from app.services.startup_service import initialize_database_schemas

    asyncio.run(initialize_database_schemas())
    asyncio.run(IndexingService().index_all_databases())
