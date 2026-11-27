"""副本回放线程：按提交顺序从主库拉取并回放，每轮最多追 max_catchup 笔。

回放线程拥有私有事件循环跑异步 HTTP 客户端；SQLite 写入在线程内同步完成，
参数与回放进度同事务落库，不阻塞服务事件循环。run_once 也可在测试协程里
直接调用——它把 HTTP 协程提交到回放线程的私有循环执行。
"""

from __future__ import annotations

import asyncio
import logging
import threading

from .client import PrimaryClient
from .config import Settings
from . import db

logger = logging.getLogger("replay")


class ReplayWorker:
    def __init__(
        self,
        settings: Settings,
        primary_client: PrimaryClient,
    ) -> None:
        self.settings = settings
        self.client = primary_client
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        # 回放线程私有事件循环；未启动线程时按需惰性创建并在守护线程里运行
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_lock = threading.Lock()
        self._loop_thread: threading.Thread | None = None

    # -- 生命周期 ----------------------------------------------------------

    def start(self) -> None:
        if self._thread is not None:
            return
        self._ensure_loop()
        self._thread = threading.Thread(
            target=self.run_forever, name="replay-worker", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    def wake(self) -> None:
        """让等待中的轮询立刻再跑一轮（测试 / 加速用）。"""
        self._wake.set()

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        with self._loop_lock:
            if self._loop is not None and self._loop.is_running():
                return self._loop
            loop = asyncio.new_event_loop()

            def _run_loop() -> None:
                asyncio.set_event_loop(loop)
                loop.run_forever()

            self._loop_thread = threading.Thread(
                target=_run_loop, name="replay-loop", daemon=True
            )
            self._loop_thread.start()
            self._loop = loop
            return loop

    # -- 主循环 ------------------------------------------------------------

    def run_forever(self) -> None:
        while not self._stop.is_set():
            try:
                applied = self.run_once()
            except Exception:
                logger.exception("回放轮询失败，下轮重试")
                applied = 0
            wait = self.settings.replay_lag if applied else self.settings.poll_interval
            if wait > 0:
                self._wake.wait(timeout=wait)
                self._wake.clear()

    def run_once(self) -> int:
        """执行一轮拉取 + 回放，返回本轮实际回放的提交数。"""
        loop = self._ensure_loop()
        with db.session(self.settings.db_path) as conn:
            last_seq = db.replica_last_seq(conn)
        commits = self.client.fetch_commits_sync(
            loop, last_seq, self.settings.max_catchup
        )
        applied = 0
        for commit in commits:
            if self._stop.is_set():
                break
            with db.session(self.settings.db_path) as conn:
                db.apply_commit(conn, commit)
            applied += 1
            logger.info("副本已回放提交 %s", commit["seq"])
        return applied
