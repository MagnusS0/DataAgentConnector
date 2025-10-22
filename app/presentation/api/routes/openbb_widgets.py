from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.presentation.api.routes.widget_registry import WIDGETS, register_widget
from app.db.sql_alchemy import connection_scope, execute_select

router = APIRouter()


def _run_query(database: str, query: str) -> list[dict[str, Any]]:
    with connection_scope(database) as connection:
        rows = execute_select(connection, query)
        return [dict(row) for row in rows]


@router.get("/widgets.json")
def get_widgets_manifest() -> dict[str, Any]:
    """Expose registered widget metadata for OpenBB Workspace."""
    return WIDGETS


@register_widget(
    {
        "name": "Query Database (MCP Mirror)",
        "description": "Mirrors the MCP query_database tool with a tabular response.",
        "category": "Data Agents",
        "type": "table",
        "endpoint": "widgets/query-database",
        "data": {
            "table": {
                "enableCharts": True,
                "showAll": True,
            }
        },
        "params": [
            {
                "paramName": "database",
                "type": "text",
                "description": "Database name as configured in DataAgentStack.",
                "label": "Database",
                "show": True,
            },
            {
                "paramName": "query",
                "type": "text",
                "description": "Read-only SQL query to execute.",
                "label": "SQL Query",
                "show": True,
            },
        ],
        "gridData": {"w": 30, "h": 18},
        "mcp_tool": {
            "mcp_server": "database-connector",
            "tool_id": "query_database",
        },
    }
)
@router.get("/widgets/query-database")
def query_database_widget(
    database: str = Query(default=..., description="Registered database alias."),
    query: str = Query(default=..., description="Read-only SQL query to execute."),
) -> list[dict[str, Any]]:
    try:
        rows = _run_query(database, query)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return rows
