from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.presentation.api.main import create_app as create_api_app
from app.presentation.mcp.main import create_mcp_app


def build_application() -> FastAPI:
    """Return the FastAPI application with MCP mounted and lifespans combined."""
    application = create_api_app()
    mcp_app = create_mcp_app()

    original_lifespan = application.router.lifespan_context

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with original_lifespan(app):
            async with mcp_app.lifespan(app):
                yield

    application.router.lifespan_context = lifespan
    application.mount("/mcp", mcp_app)
    return application


app = build_application()
