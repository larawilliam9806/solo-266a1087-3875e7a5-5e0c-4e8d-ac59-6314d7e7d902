"""副本访问主库的异步 HTTP 客户端。

回放线程有自己的事件循环、FastAPI 请求跑在 uvicorn 主循环，httpx 的
AsyncClient 内部同步原语绑定创建时的事件循环，因此这里按循环惰性持有
独立客户端；测试时可注入 ASGI 直连 transport。
"""

from __future__ import annotations

import asyncio
import weakref
from typing import Any

import httpx


class PrimaryClient:
    def __init__(self, base_url: str, transport: httpx.AsyncBaseTransport | None = None):
        self._base_url = base_url
        self._transport = transport
        self._clients: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, httpx.AsyncClient]" = (
            weakref.WeakKeyDictionary()
        )

    def _client_for(self, loop: asyncio.AbstractEventLoop) -> httpx.AsyncClient:
        client = self._clients.get(loop)
        if client is None:
            client = httpx.AsyncClient(
                base_url=self._base_url,
                transport=self._transport,
                timeout=10.0,
            )
            self._clients[loop] = client
        return client

    def _client(self) -> httpx.AsyncClient:
        return self._client_for(asyncio.get_running_loop())

    async def aclose(self) -> None:
        for client in list(self._clients.values()):
            await client.aclose()
        self._clients.clear()

    async def fetch_commits(self, last_seq: int, limit: int) -> list[dict[str, Any]]:
        resp = await self._client().get(
            "/internal/commits",
            params={"after": last_seq, "limit": limit},
        )
        resp.raise_for_status()
        return resp.json()["commits"]

    async def primary_latest(self) -> int:
        resp = await self._client().get("/internal/watermark")
        resp.raise_for_status()
        return int(resp.json()["latest_commit"])

    async def read_snapshot(
        self, device_ids: list[str], min_commit: int
    ) -> dict[str, Any]:
        resp = await self._client().post(
            "/internal/snapshot",
            json={"device_ids": device_ids, "min_commit": min_commit},
        )
        resp.raise_for_status()
        return resp.json()

    # -- 供回放线程在其私有事件循环里同步调用 --------------------------------

    def fetch_commits_sync(
        self, loop: "asyncio.AbstractEventLoop", last_seq: int, limit: int
    ) -> list[dict[str, Any]]:
        future = asyncio.run_coroutine_threadsafe(
            self.fetch_commits(last_seq, limit), loop
        )
        return future.result()
