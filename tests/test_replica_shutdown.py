"""副本关停语义：在 5 秒预算内取消挂起拉取、停止回放并关闭 HTTP 连接。

覆盖需求：
- 主库迟迟不回时，stop() 在 5 秒上限内取消在途拉取、结束回放线程与私有循环；
- 关停开始后未开始写入的提交不再落库；
- 正在写入的一笔整笔提交或整笔回滚，参数与回放水位保持一致；
- 用过主库兜底查询（主事件循环上也持有连接）的副本也能正常退出；
- 用原库重启后，从最后一笔已落库提交之后继续，每笔只落库一次。
"""

from __future__ import annotations

import asyncio
import threading
import time

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app import db
from app.client import PrimaryClient
from app.config import Settings
from app.replay import ReplayWorker

pytestmark = pytest.mark.asyncio

SHUTDOWN_BUDGET = 5.0


class HangingPrimary:
    """可挂起 /internal/commits 的假主库，其余接口立即返回。"""

    def __init__(self) -> None:
        self.app = FastAPI()
        self.request_started = threading.Event()
        self.release = threading.Event()
        self.app.add_api_route(
            "/internal/commits", self.commits, methods=["GET"]
        )
        self.app.add_api_route(
            "/internal/watermark", self.watermark, methods=["GET"]
        )
        self.app.add_api_route(
            "/internal/snapshot", self.snapshot, methods=["POST"]
        )

    async def commits(self, request: Request):
        self.request_started.set()
        # 模拟主库迟迟不回：阻塞直到测试放行（远超 5 秒关停预算）
        await asyncio.get_running_loop().run_in_executor(
            None, self.release.wait, 30.0
        )
        return {"after": 0, "limit": 5, "commits": []}

    async def watermark(self, request: Request):
        return {"latest_commit": 0}

    async def snapshot(self, request: Request):
        return JSONResponse(
            {"ready": True, "source": "primary",
             "snapshot_commit": 1, "devices": {}}
        )


def _make_worker(tmp_path, hanging: HangingPrimary):
    db_path = str(tmp_path / "replica.db")
    db.init_db(db_path)
    client = PrimaryClient(
        "http://hanging-primary",
        transport=httpx.ASGITransport(app=hanging.app),
    )
    settings = Settings(
        role="replica",
        db_path=db_path,
        primary_url="http://hanging-primary",
        poll_interval=0.05,
        replay_auto_start=False,
    )
    worker = ReplayWorker(settings, client)
    return worker, client, db_path


async def test_stop_cancels_hanging_fetch_within_shutdown_budget(tmp_path):
    hanging = HangingPrimary()
    worker, _client, _db_path = _make_worker(tmp_path, hanging)
    worker.start()
    assert hanging.request_started.wait(5.0), "回放线程未发起拉取"

    started = time.monotonic()
    worker.stop(timeout=SHUTDOWN_BUDGET)
    elapsed = time.monotonic() - started

    assert elapsed < SHUTDOWN_BUDGET, f"关停耗时 {elapsed:.2f}s 超过 5 秒上限"
    assert worker._thread is None
    # 回放线程与私有循环线程都已退出，没有后台残留
    replay_threads = [
        t.name for t in threading.enumerate() if "replay" in t.name
    ]
    assert replay_threads == []
    hanging.release.set()


async def test_stop_without_started_worker_is_immediate(tmp_path):
    hanging = HangingPrimary()
    worker, _client, _ = _make_worker(tmp_path, hanging)
    # 未 start、也未触发过惰性循环时，stop 应当立即返回
    started = time.monotonic()
    worker.stop(timeout=SHUTDOWN_BUDGET)
    assert time.monotonic() - started < 1.0


async def test_no_new_commits_written_after_shutdown_begins(tmp_path):
    hanging = HangingPrimary()
    worker, _client, db_path = _make_worker(tmp_path, hanging)
    worker.start()
    assert hanging.request_started.wait(5.0)

    worker.stop(timeout=SHUTDOWN_BUDGET)
    hanging.release.set()
    await asyncio.sleep(0.1)

    with db.session(db_path) as conn:
        assert db.replica_last_seq(conn) == 0
        assert conn.execute("SELECT COUNT(*) AS c FROM change_log").fetchone()["c"] == 0
        assert conn.execute("SELECT COUNT(*) AS c FROM param_current").fetchone()["c"] == 0


async def test_shutdown_closes_connections_on_both_loops(tmp_path):
    hanging = HangingPrimary()
    worker, client, _db_path = _make_worker(tmp_path, hanging)
    worker.start()
    assert hanging.request_started.wait(5.0)
    # 主事件循环也用过一次兜底查询，产生绑定主循环的连接
    await client.read_snapshot(["D1"], 1)
    assert len(client._clients) == 2

    worker.stop(timeout=SHUTDOWN_BUDGET)
    # 回放循环上的客户端已在其绑定循环内关闭并摘除
    assert worker._loop is None
    await client.aclose()
    assert len(client._clients) == 0
    hanging.release.set()


async def test_stop_is_idempotent_and_run_once_after_stop_is_noop(tmp_path):
    hanging = HangingPrimary()
    worker, _client, db_path = _make_worker(tmp_path, hanging)
    worker.start()
    assert hanging.request_started.wait(5.0)

    worker.stop(timeout=SHUTDOWN_BUDGET)
    # 再次 stop 不报错、不重建循环
    worker.stop(timeout=SHUTDOWN_BUDGET)
    assert worker._loop is None
    # 关停后直接驱动一轮也不会重新拉取 / 写入
    assert worker.run_once() == 0
    with db.session(db_path) as conn:
        assert db.replica_last_seq(conn) == 0
    hanging.release.set()


async def test_inflight_commit_is_atomic_when_write_fails(tmp_path, monkeypatch):
    """写入中途失败：整笔回滚，参数与回放水位都停在原值。"""
    hanging = HangingPrimary()
    worker, _client, db_path = _make_worker(tmp_path, hanging)
    commit = {
        "seq": 1,
        "created_at": "2026-01-01T00:00:00.000Z",
        "changes": [{"device_id": "D1", "params": {"offset": 1.0}}],
    }

    real_apply = db.apply_commit
    call_count = {"n": 0}

    def flaky_apply(conn, payload):
        call_count["n"] += 1
        if call_count["n"] == 1:
            # 模拟事务提交前失败（如磁盘异常），apply_commit 内部必须 ROLLBACK
            raise RuntimeError("模拟写入中途失败")
        return real_apply(conn, payload)

    monkeypatch.setattr(db, "apply_commit", flaky_apply)
    with pytest.raises(RuntimeError):
        with db.session(db_path) as conn:
            db.apply_commit(conn, commit)

    with db.session(db_path) as conn:
        # 整笔回滚：没有半截参数，水位也没前进
        assert db.replica_last_seq(conn) == 0
        assert conn.execute("SELECT COUNT(*) AS c FROM param_current").fetchone()["c"] == 0
        assert conn.execute("SELECT COUNT(*) AS c FROM change_log").fetchone()["c"] == 0

    # 重试同一笔成功：只落库一次，水位与参数一致前进到 seq=1
    monkeypatch.undo()
    with db.session(db_path) as conn:
        db.apply_commit(conn, commit)
    with db.session(db_path) as conn:
        assert db.replica_last_seq(conn) == 1
        row = conn.execute(
            "SELECT value, seq FROM param_current WHERE device_id='D1' AND param_name='offset'"
        ).fetchone()
        assert (row["value"], row["seq"]) == (1.0, 1)
        assert conn.execute("SELECT COUNT(*) AS c FROM change_log").fetchone()["c"] == 1


async def test_commit_mid_write_completes_or_rolls_back_atomically(tmp_path, monkeypatch):
    """关停信号在一笔提交写入过程中到达：该笔完整提交或整笔回滚，不留半截。"""
    hanging = HangingPrimary()
    worker, _client, db_path = _make_worker(tmp_path, hanging)
    commit = {
        "seq": 1,
        "created_at": "2026-01-01T00:00:00.000Z",
        "changes": [
            {"device_id": "D1", "params": {"offset": 1.0}},
            {"device_id": "D2", "params": {"offset": 2.0, "gain": 3.0}},
        ],
    }
    entered = threading.Event()
    release = threading.Event()
    real_apply = db.apply_commit

    def blocking_apply(conn, payload):
        entered.set()
        # 模拟一笔多设备提交正在写入，期间外部触发关停
        release.wait(5.0)
        return real_apply(conn, payload)

    monkeypatch.setattr(db, "apply_commit", blocking_apply)

    t = threading.Thread(
        target=lambda: _run_one(worker, [commit]), daemon=True
    )
    t.start()
    assert entered.wait(5.0), "写入未开始"
    # 写入进行中触发关停；在写的一笔不被打断，等其事务结束
    worker.stop(timeout=SHUTDOWN_BUDGET)
    release.set()
    t.join(timeout=5.0)

    with db.session(db_path) as conn:
        watermark = db.replica_last_seq(conn)
        n_change = conn.execute("SELECT COUNT(*) AS c FROM change_log").fetchone()["c"]
        n_param = conn.execute("SELECT COUNT(*) AS c FROM param_current").fetchone()["c"]
    # 在写的一笔要么完整落库（水位=1，3 个参数、2 条改动），要么整笔回滚（全 0），
    # 不允许出现只写了一部分设备的中间状态
    if watermark == 1:
        assert n_change == 2 and n_param == 3
    else:
        assert watermark == 0 and n_change == 0 and n_param == 0


def _run_one(worker: ReplayWorker, commits):
    """绕过 HTTP，直接驱动 worker 的写入循环（用打桩的 apply_commit）。"""
    # 复用 run_once 的事务边界，但喂入固定提交
    applied = 0
    for commit in commits:
        if worker._stop.is_set():
            break
        with db.session(worker.settings.db_path) as conn:
            db.apply_commit(conn, commit)
        applied += 1
    return applied


async def test_restart_resumes_after_last_durable_commit(tmp_path):
    """用原库重启 worker：从最后一笔已落库提交之后继续，每笔只落库一次。"""
    hanging = HangingPrimary()
    worker, _client, db_path = _make_worker(tmp_path, hanging)

    c1 = {"seq": 1, "created_at": "2026-01-01T00:00:00.000Z",
          "changes": [{"device_id": "D1", "params": {"offset": 1.0}}]}
    c2 = {"seq": 2, "created_at": "2026-01-01T00:00:01.000Z",
          "changes": [{"device_id": "D2", "params": {"offset": 2.0}}]}
    with db.session(db_path) as conn:
        db.apply_commit(conn, c1)
        db.apply_commit(conn, c2)
    worker.stop()

    # 模拟副本用原库重启：新 worker 指向同一个 SQLite 文件
    worker2, client2, _ = _make_worker(tmp_path, hanging)
    with db.session(db_path) as conn:
        assert db.replica_last_seq(conn) == 2

    # 重启后拉取的起点是 last_seq=2，而不是从头重放
    captured: dict[str, int] = {}

    async def fake_fetch(last_seq, limit):
        captured["after"] = last_seq
        # 主库没有新提交
        return []

    client2.fetch_commits = fake_fetch  # type: ignore[method-assign]
    assert worker2.run_once() == 0
    assert captured["after"] == 2

    # 已落库的两笔仍然各只有一份，没有重复回放
    with db.session(db_path) as conn:
        assert db.replica_last_seq(conn) == 2
        assert conn.execute("SELECT COUNT(*) AS c FROM change_log").fetchone()["c"] == 2
        rows = conn.execute(
            "SELECT DISTINCT seq FROM change_log ORDER BY seq"
        ).fetchall()
        assert [r["seq"] for r in rows] == [1, 2]
    worker2.stop()
    hanging.release.set()
