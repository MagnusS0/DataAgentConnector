from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Body, HTTPException
from pydantic import BaseModel, Field

from app.presentation.api.routes.widget_registry import WIDGETS, register_widget
from app.db.sql_alchemy import connection_scope, execute_select

router = APIRouter()


class DataFormat(BaseModel):
    data_type: str = Field(default="object")
    parse_as: str = Field(default="table")


class OmniWidgetResponse(BaseModel):
    content: Any
    data_format: DataFormat
    extra_citations: list[Any] | None = Field(default_factory=list)
    citable: bool = True


def _run_query(database: str, query: str) -> list[dict[str, Any]]:
    with connection_scope(database) as connection:
        rows = execute_select(connection, query)
        return [dict(row) for row in rows]


def _parse_payload(payload: str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="Invalid JSON payload") from exc
    return payload


@router.get("/widgets.json")
def get_widgets_manifest() -> dict[str, Any]:
    """Expose registered widget metadata for OpenBB Workspace."""
    return WIDGETS


@register_widget(
    {
        "name": "Query Database (MCP Mirror)",
        "description": "Mirrors the MCP query_database tool with a tabular response.",
        "category": "Data Agents",
        "type": "omni",
        "endpoint": "widgets/query-database",
        "params": [
            {
                "paramName": "prompt",
                "type": "text",
                "description": "Forwarded Copilot prompt context.",
                "label": "Prompt",
                "show": False,
            },
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
@router.post("/widgets/query-database", response_model=OmniWidgetResponse)
async def query_database_widget(data: str | dict[str, Any] = Body(...)) -> OmniWidgetResponse:
    parsed = _parse_payload(data)

    database = parsed.get("database")
    query = parsed.get("query")

    if not database:
        raise HTTPException(status_code=422, detail="Missing 'database' parameter.")
    if not query:
        raise HTTPException(status_code=422, detail="Missing 'query' parameter.")

    try:
        rows = _run_query(database, query)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if not rows:
        return OmniWidgetResponse(
            content="Query executed. No rows returned.",
            data_format=DataFormat(data_type="object", parse_as="text"),
        )

    return OmniWidgetResponse(
        content=rows,
        data_format=DataFormat(data_type="object", parse_as="table"),
    )
