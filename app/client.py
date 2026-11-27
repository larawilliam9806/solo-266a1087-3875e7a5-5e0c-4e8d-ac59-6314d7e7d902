"""副本访问主库的异步 HTTP 客户端。

回放线程有自己的事件循环、FastAPI 请求跑在 uvicorn 主循环，httpx 的
AsyncClient 内部同步原语绑定创建时的事件循环，因此这里按循环惰性持有
独立客户端；测试时可注入 ASGI 直连 transport。

关停注意：httpx.AsyncClient 必须在其绑定的事件循环上 aclose，跨循环调用
只会静默返回，挂起请求与连接不会被关闭。因此回放循环上的客户端由回放线程
在自己的循环内同步关闭（见 aclose_for_loop_sync），主循环上的客户端走
aclose()。
"""

from __future__ import annotations

import asyncio
import weakref
from typing import Any

import httpx

# 跨循环回收客户端时，等待在原循环上完成 aclose 的最长时间
_CLOSE_TIMEOUT = 2.0


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
        """关闭客户端。

        绑定当前事件循环的客户端直接关闭；绑定其它仍在运行循环的客户端，
        把关闭协程投递回原循环执行，避免跨循环 aclose 静默泄漏连接。
        """
        current: asyncio.AbstractEventLoop | None
        try:
            current = asyncio.get_running_loop()
        except RuntimeError:
            current = None
        for loop, client in list(self._clients.items()):
            self._clients.pop(loop, None)
            if loop is current or not loop.is_running():
                await client.aclose()
                continue
            future = asyncio.run_coroutine_threadsafe(client.aclose(), loop)
            try:
                future.result(timeout=_CLOSE_TIMEOUT)
            except Exception:
                # 原循环来不及收尾：进程关停场景下连接随进程回收
                future.cancel()

    def aclose_for_loop_sync(
        self, loop: asyncio.AbstractEventLoop, timeout: float = _CLOSE_TIMEOUT
    ) -> None:
        """在指定事件循环上同步关闭绑定该循环的客户端（供回放线程关停使用）。"""
        client = self._clients.pop(loop, None)
        if client is None:
            return
        if not loop.is_running():
            loop.run_until_complete(client.aclose())
            return
        future = asyncio.run_coroutine_threadsafe(client.aclose(), loop)
        try:
            future.result(timeout=timeout)
        except Exception:
            future.cancel()

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
        self,
        loop: "asyncio.AbstractEventLoop",
        last_seq: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        """把拉取协程投递到回放循环执行并同步等待结果。

        需要可取消关停的调用方（ReplayWorker）应直接使用
        ``asyncio.run_coroutine_threadsafe`` 拿到 concurrent.futures.Future，
        在收到关停信号时对其调用 cancel，以中止回放循环上挂起的拉取任务
        并释放其 HTTP 连接。
        """
        future = asyncio.run_coroutine_threadsafe(
            self.fetch_commits(last_seq, limit), loop
        )
        return future.result()
