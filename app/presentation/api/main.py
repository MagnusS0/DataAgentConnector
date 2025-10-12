from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.types import StatelessLifespan, StatefulLifespan

from app.core.config import get_settings
from app.presentation.api.routes.openbb_widgets import router as openbb_widgets_router


def create_app(
    lifespan: StatelessLifespan[FastAPI] | StatefulLifespan[FastAPI] | None = None,
) -> FastAPI:
    _ = get_settings()

    application = FastAPI(
        title="Data Agent Connector API",
        description="REST endpoints for Data Agent Connector functionality.",
        lifespan=lifespan,
    )

    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    application.include_router(openbb_widgets_router)

    return application


app = create_app()
