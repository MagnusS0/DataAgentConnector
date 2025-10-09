from typing import Annotated

from fastmcp import FastMCP
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware import Middleware

from app.db.sql_alchemy import (
    connection_scope,
    list_databases,
    list_tables,
    get_table_metadata,
    execute_select,
    get_table_preview,
)
from app.services.fk_analyzer import shortest_join_path, connect_tables


mcp = FastMCP(
    name="DataAgentStack",
    instructions="""
This MCP server provides tools for exploring and querying SQL databases.
You can connect to a database, list tables, view table metadata, and execute SQL queries.
Use the provided tools to interact with the database effectively.
""",
)


@mcp.tool
def get_databases() -> list[dict[str, str]]:
    """
    Use this to get a list of available databases and their descriptions.
    This is usually the first tool to call. You use the 'name' field from the output
    to specify which database to connect to in subsequent calls.
    """
    return list_databases()


@mcp.tool
def show_tables(database: str) -> list[str]:
    """List all tables in the connected database."""
    with connection_scope(database) as connection:
        return list_tables(connection)


@mcp.tool
def describe_table(
    table_name: Annotated[str, "Name of the table to describe"],
    database: Annotated[str, "Name of the database to use"],
) -> dict:
    """
    Get metadata for a specific table.
    Includes columns, primary keys, foreign keys, and indexes.
    """
    with connection_scope(database) as connection:
        return get_table_metadata(connection, table_name)


@mcp.tool
def preview_table(
    table_name: Annotated[str, "Name of the table to preview"],
    database: Annotated[str, "Name of the database to use"],
) -> list[dict]:
    """
    Preview the first few rows of a specific table.
    Default to 5 rows.
    """
    with connection_scope(database) as connection:
        rows = get_table_preview(connection, table_name)
        return [dict(row) for row in rows]


@mcp.tool
def query_database(
    query: str,
    database: Annotated[str, "Name of the database to query"],
) -> list[dict]:
    """
    Execute a SQL SELECT query and return the results.
    Only SELECT queries are allowed.
    """
    with connection_scope(database) as connection:
        rows = execute_select(connection, query)
        return [dict(row) for row in rows]


@mcp.tool
def join_path(
    tables: Annotated[list[str], "Tables that should be connected via joins"],
    database: Annotated[str, "Name of the database to use"],
) -> str:
    """
    Suggest the shortest join path connecting the provided tables.

    Returns a SQL JOIN clause that connects the tables.
    The format is `FROM "table1" JOIN "table2" ON ...`.
    You should decide when to use INNER JOIN vs LEFT JOIN based on the context of your query.
    """
    if len(tables) < 2:
        return "At least two tables are required to form a join path."

    if len(tables) == 2:
        left, right = tables
        try:
            steps = shortest_join_path(database, left, right)
        except ValueError as e:
            return str(e)
    else:
        try:
            steps = connect_tables(database, tables)
        except ValueError as e:
            return str(e)

    if not steps:
        return f"No join path found for tables: {', '.join(tables)}"

    start_table = tables[0]

    join_parts = []
    for step in steps:
        on_conditions = " AND ".join(
            f'"{step.left_table}"."{left_col}" = "{step.right_table}"."{right_col}"'
            for left_col, right_col in step.column_pairs
        )
        join_parts.append(f'JOIN "{step.right_table}" ON {on_conditions}')

    return f'FROM "{start_table}"\n' + "\n".join(join_parts)


def setup_cors() -> list[Middleware]:
    """Configure CORS middleware for the MCP server."""
    expose_headers = [
        "Mcp-Session-Id",
        "mcp-session-id",
        "mcp-protocol-version",
    ]

    middleware = [
        Middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["GET", "POST", "PUT", "DELETE"],
            allow_headers=["*"],
            allow_credentials=False,
            expose_headers=expose_headers,
        )
    ]

    return middleware


if __name__ == "__main__":
    mcp.run(transport="streamable-http", middleware=setup_cors())
