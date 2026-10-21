import asyncio

from fastmcp import Client

from app.interfaces.mcp.main import mcp

EXPECTED_TOOLS = {
    "get_databases",
    "show_tables",
    "show_views",
    "describe_view",
    "describe_tables",
    "get_distinct_values",
    "find_relevant_columns_and_content",
    "preview_table",
    "query_database",
    "join_path",
}


def call(name: str, args: dict, *, raise_on_error: bool = True):
    async def run():
        async with Client(mcp) as client:
            return await client.call_tool(name, args, raise_on_error=raise_on_error)

    return asyncio.run(run())


def test_lists_all_tools() -> None:
    async def run():
        async with Client(mcp) as client:
            return await client.list_tools()

    assert {t.name for t in asyncio.run(run())} == EXPECTED_TOOLS


def test_get_databases() -> None:
    result = call("get_databases", {})
    assert result.data[0]["name"] == "shop"
    assert result.data[0]["schemas"] == ["main"]


def test_preview_masks_ssn() -> None:
    rows = call("preview_table", {"table_name": "customers", "database": "shop"}).data
    assert len(rows) == 4
    assert {r["ssn"] for r in rows} == {"***MASKED***"}
    assert rows[0]["name"] == "Alice"


def test_distinct_values_denied_for_masked_column() -> None:
    result = call(
        "get_distinct_values",
        {"table_name": "customers", "column_name": "ssn", "database": "shop"},
    )
    assert "ACCESS DENIED" in str(result.data)


def test_query_database_select() -> None:
    result = call(
        "query_database",
        {"query": "SELECT city FROM customers WHERE id = 1", "database": "shop"},
    )
    assert result.data == [{"city": "Oslo"}]


def test_query_database_delete_is_error() -> None:
    result = call(
        "query_database",
        {"query": "DELETE FROM customers", "database": "shop"},
        raise_on_error=False,
    )
    assert result.is_error


def test_describe_tables_notes_masked_columns() -> None:
    result = call("describe_tables", {"table_names": ["customers"], "database": "shop"})
    assert "MASKED COLUMNS: ssn" in result.data


def test_find_relevant_columns() -> None:
    result = call(
        "find_relevant_columns_and_content", {"query": "Oslo", "database": "shop"}
    )
    assert "Matching values:" in result.data[0]


def test_join_path_mentions_orders() -> None:
    result = call(
        "join_path", {"tables": ["customers", "products"], "database": "shop"}
    )
    assert 'JOIN "orders"' in result.data
