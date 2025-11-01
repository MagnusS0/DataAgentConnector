from contextlib import asynccontextmanager
import timeit

from fastapi import FastAPI

from app.core.logging import get_logger
from app.interfaces.api.main import create_app as create_api_app
from app.interfaces.mcp.main import create_mcp_app
from app.services.annotate.annotation_store import store_table_descriptions
from app.services.indexing.content_fts_indexer import create_all_content_indices

logger = get_logger("main")


def build_application() -> FastAPI:
    """Return the FastAPI application with MCP mounted and lifespans combined."""
    application = create_api_app()
    mcp_app = create_mcp_app()

    original_lifespan = application.router.lifespan_context

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            start_time = timeit.default_timer()
            await create_all_content_indices()
            elapsed = timeit.default_timer() - start_time
            logger.info(
                "Content FTS indices created successfully in %.2f seconds", elapsed
            )
        except Exception as exc:
            logger.exception("Failed to create content FTS indices on startup: %s", exc)
        try:
            start_time = timeit.default_timer()
            await store_table_descriptions()
            elapsed = timeit.default_timer() - start_time
            logger.info(
                "Table annotations stored successfully in %.2f seconds", elapsed
            )
        except Exception as exc:
            logger.exception("Failed to store table annotations on startup: %s", exc)

        async with original_lifespan(app):
            async with mcp_app.lifespan(app):
                yield

    application.router.lifespan_context = lifespan
    application.mount("/mcp", mcp_app)
    return application


app = build_application()
