"""副本回放线程：按提交顺序从主库拉取并回放，每轮最多追 max_catchup 笔。

回放线程拥有私有事件循环跑异步 HTTP 客户端；SQLite 写入在线程内同步完成，
参数与回放进度同事务落库，不阻塞服务事件循环。run_once 也可在测试协程里
直接调用——它把 HTTP 协程提交到回放线程的私有循环执行。

关停语义（stop，默认 5 秒上限）：
- 主库迟迟不回时，关停信号会取消回放循环上挂起的拉取任务，HTTP 连接随之关闭；
- 关停开始后尚未开始写入的提交一律不再落库；
- 正在写入的一笔由 SQLite 事务保证整笔提交或整笔回滚，参数与回放水位一致；
- 回放循环上的 HTTP 客户端在其绑定循环内关闭，随后停掉私有循环并回收线程。
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from concurrent.futures import CancelledError as FutureCancelledError
from concurrent.futures import Future

from .client import PrimaryClient
from .config import Settings
from . import db

logger = logging.getLogger("replay")

# 关停时留给私有循环回收 HTTP 连接的时间，整体仍受 stop 的 5 秒预算约束
_LOOP_TEARDOWN_LIMIT = 2.0


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
        # 当前在途拉取的并发 Future 与关停信号；stop() 取消挂起的 HTTP 请求
        self._fetch_future: Future | None = None
        self._fetch_lock = threading.Lock()

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
        """请求关停并在 timeout 秒内回收回放线程、HTTP 连接与私有循环。

        主库无响应时通过取消在途拉取立即解除阻塞，而不是等 HTTP 超时。
        """
        deadline = time.monotonic() + timeout
        self._stop.set()
        self._wake.set()
        self._cancel_inflight_fetch()

        thread = self._thread
        if thread is not None:
            thread.join(timeout=max(deadline - time.monotonic(), 0.0))
            if thread.is_alive():
                logger.warning("回放线程未能在 %.1f 秒内退出", timeout)
            self._thread = None

        self._teardown_loop(deadline)

    def wake(self) -> None:
        """让等待中的轮询立刻再跑一轮（测试 / 加速用）。"""
        self._wake.set()

    def _cancel_inflight_fetch(self) -> None:
        with self._fetch_lock:
            future = self._fetch_future
        if future is not None and not future.done():
            # 取消投递在回放循环上的拉取任务，挂起的 HTTP 请求随之关闭
            future.cancel()

    def _teardown_loop(self, deadline: float) -> None:
        with self._loop_lock:
            loop = self._loop
            loop_thread = self._loop_thread
            self._loop = None
            self._loop_thread = None
        if loop is None:
            return
        # 先在绑定循环上关闭 HTTP 客户端，确保挂起连接真正释放
        remaining = max(deadline - time.monotonic(), 0.0)
        self.client.aclose_for_loop_sync(
            loop, timeout=min(remaining, _LOOP_TEARDOWN_LIMIT)
        )
        if loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
        if loop_thread is not None:
            remaining = max(deadline - time.monotonic(), 0.0)
            loop_thread.join(timeout=min(remaining, _LOOP_TEARDOWN_LIMIT))
        if not loop.is_closed():
            try:
                loop.close()
            except RuntimeError:
                pass

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
            except (asyncio.CancelledError, FutureCancelledError):
                # 关停取消了在途拉取：结束循环，交由 stop 回收私有循环
                logger.info("拉取已被关停信号取消，回放线程退出")
                break
            except Exception:
                logger.exception("回放轮询失败，下轮重试")
                applied = 0
            if self._stop.is_set():
                break
            wait = self.settings.replay_lag if applied else self.settings.poll_interval
            if wait > 0 and self._wake.wait(timeout=wait):
                self._wake.clear()

    def run_once(self) -> int:
        """执行一轮拉取 + 回放，返回本轮实际回放的提交数。"""
        # 关停已经开始：不再发起任何新的拉取 / 写入
        if self._stop.is_set():
            return 0
        loop = self._ensure_loop()
        with db.session(self.settings.db_path) as conn:
            last_seq = db.replica_last_seq(conn)

        future = asyncio.run_coroutine_threadsafe(
            self.client.fetch_commits(last_seq, self.settings.max_catchup), loop
        )
        with self._fetch_lock:
            self._fetch_future = future
        # 注册期间若 stop 已先行，补上取消，避免关停信号丢失
        if self._stop.is_set():
            future.cancel()
        try:
            commits = future.result()
        finally:
            with self._fetch_lock:
                self._fetch_future = None

        # 拉取期间关停开始：本轮回放整体放弃，未开始写入的提交不再落库
        if self._stop.is_set():
            return 0

        applied = 0
        for commit in commits:
            if self._stop.is_set():
                break
            # 单提交在一个 SQLite 事务里完成：整笔提交或整笔回滚，
            # 参数与回放水位要么一起前进，要么都不变
            with db.session(self.settings.db_path) as conn:
                db.apply_commit(conn, commit)
            applied += 1
            logger.info("副本已回放提交 %s", commit["seq"])
        return applied
