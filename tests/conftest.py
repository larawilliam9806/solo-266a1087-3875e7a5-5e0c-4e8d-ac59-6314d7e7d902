"""pytest 全局配置与夹具：进程内 ASGI 直连的主库 / 副本应用。"""

import os
import tempfile

import httpx
import pytest_asyncio

from app.config import Settings
from app.main import create_app


@pytest_asyncio.fixture()
async def env():
    tmp = tempfile.mkdtemp(prefix="paramstore-test-")
    primary_db = os.path.join(tmp, "primary.db")
    replica_db = os.path.join(tmp, "replica.db")

    primary_app = create_app(
        Settings(role="primary", db_path=primary_db, replay_auto_start=False)
    )
    primary_transport = httpx.ASGITransport(app=primary_app)

    replica_settings = Settings(
        role="replica",
        db_path=replica_db,
        primary_url="http://primary-test",
        max_catchup=2,
        poll_interval=0.05,
        replay_lag=0.0,
        replay_auto_start=False,
    )
    replica_app = create_app(
        replica_settings, primary_transport=primary_transport
    )

    primary_http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=primary_app),
        base_url="http://primary-test",
    )
    replica_http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=replica_app),
        base_url="http://replica-test",
    )

    try:
        yield {
            "primary": primary_http,
            "replica": replica_http,
            "replica_app": replica_app,
            "worker": replica_app.state.worker,
            "tmp": tmp,
        }
    finally:
        await primary_http.aclose()
        await replica_http.aclose()
        await replica_app.state.primary_client.aclose()
