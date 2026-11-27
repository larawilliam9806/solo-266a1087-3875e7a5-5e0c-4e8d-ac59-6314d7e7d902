"""FastAPI 应用工厂，按 APP_ROLE 装配主库或副本。"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .client import PrimaryClient
from .config import Settings, load_settings
from . import db
from .replay import ReplayWorker
from .routers import primary as primary_router
from .routers import replica as replica_router

logger = logging.getLogger("paramstore")


@asynccontextmanager
async def lifespan(app: FastAPI):
    worker = getattr(app.state, "worker", None)
    if worker is not None and app.state.settings.replay_auto_start:
        worker.start()
    try:
        yield
    finally:
        if worker is not None:
            worker.stop()
        client = getattr(app.state, "primary_client", None)
        if client is not None:
            import asyncio

            await asyncio.shield(client.aclose())


def create_app(
    settings: Settings | None = None,
    primary_transport=None,
) -> FastAPI:
    settings = settings or load_settings()
    db.init_db(settings.db_path)

    app = FastAPI(
        title="设备参数库（主库 + 查询副本）",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.worker = None
    app.state.primary_client = None

    if settings.role == "primary":
        app.include_router(primary_router.router)
    else:
        client = PrimaryClient(settings.primary_url, transport=primary_transport)
        app.state.primary_client = client
        app.state.worker = ReplayWorker(settings, client)
        app.include_router(replica_router.router)
    return app


app = create_app()
