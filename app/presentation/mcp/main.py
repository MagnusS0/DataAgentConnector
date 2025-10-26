from typing import Annotated

from fastmcp import FastMCP
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

from app.db.sql_alchemy import (
    connection_scope,
    execute_select,
    get_table_metadata,
    get_table_preview,
    get_veiw_definition,
    list_databases,
    list_tables,
    list_views,
)
from app.models.database_registry import TableMetadata
from app.services.fk_analyzer import connect_tables, shortest_join_path
from app.services.annotate.annotation_store import (
    get_table_descriptions,
    TableDescription,
)
from app.services.indexing.search import search_column_contents
from app.core.config import get_settings

settings = get_settings()


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
    to specify which database to connect to in other tools on this mcp server.
    """
    return list_databases()


@mcp.tool
def show_tables(database: str) -> list[TableDescription]:
    """List tables in the connected database along with stored descriptions."""
    with connection_scope(database) as connection:
        tables = list_tables(connection)

    return get_table_descriptions(database, tuple(tables))


@mcp.tool
def show_views(database: str) -> list[str]:
    """List views in the connected database."""
    with connection_scope(database) as connection:
        views = list_views(connection)

    return views


@mcp.tool
def describe_view(
    view_name: Annotated[str, "Name of the view to describe"],
    database: Annotated[str, "Name of the database to use"],
) -> str:
    """
    Get the SQL definition of a specific view.
    """
    with connection_scope(database) as connection:
        return get_veiw_definition(connection, view_name)


@mcp.tool
def describe_table(
    table_name: Annotated[str, "Name of the table to describe"],
    database: Annotated[str, "Name of the database to use"],
) -> TableMetadata:
    """
    Get metadata for a specific table.
    Includes columns, primary keys, foreign keys, and indexes.
    """
    with connection_scope(database) as connection:
        return get_table_metadata(connection, table_name)


@mcp.tool
def find_relevant_columns_and_content(
    query: Annotated[str, "The search query to find relevant columns for"],
    database: Annotated[str, "Name of the database to use"],
    top_k: Annotated[int, "Number of top relevant contents to return"] = 5,
) -> list[str]:
    """
    This tool searches for relevant content in columns based on the provided query.
    It uses a BM25 index over the distinct values in textual columns to find matches.
    For example if you need to find the column for a specific product name, or look for
    customer names related to a certain city, you can use this tool to find the relevant columns.

    Returns the top_k most relevant contents including table, and column information.
    """
    return search_column_contents(database=database, query=query, top_k=top_k)


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
        return rows


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
        rows = execute_select(connection, query, limit=settings.limit)
        return [dict(row) for row in rows]


@mcp.tool
def join_path(
    tables: Annotated[list[str], "Tables that should be connected via joins"],
    database: Annotated[str, "Name of the database to use"],
) -> str:
    """
    This tool suggests the shortest join path connecting the provided tables.

    Returns a SQL JOIN clause that connects the tables.
    You need to decide when to use INNER JOIN vs LEFT JOIN based on the context of your query.
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
            allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            allow_headers=["*"],
            allow_credentials=False,
            expose_headers=expose_headers,
        )
    ]

    return middleware


def create_mcp_app():
    """Return the MCP server as an ASGI application."""
    return mcp.http_app(
        transport="streamable-http",
        middleware=setup_cors(),
        path="/",
    )


if __name__ == "__main__":
    mcp.run(transport="streamable-http", middleware=setup_cors())
