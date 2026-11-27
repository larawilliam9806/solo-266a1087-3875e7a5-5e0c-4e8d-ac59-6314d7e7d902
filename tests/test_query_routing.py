"""副本查询路由：优先副本、等待、主库兜底、未就绪。"""

import asyncio

import pytest

from tests.helpers import commit, query

pytestmark = pytest.mark.asyncio


async def test_replica_serves_when_min_commit_met_even_if_primary_newer(env):
    c1 = await commit(env["primary"], [{"device_id": "D1", "params": {"offset": 1.0}}])
    env["worker"].run_once()
    # 主库继续写入，副本停在 c1
    c2 = await commit(env["primary"], [{"device_id": "D1", "params": {"offset": 2.0}}])

    resp = await query(env["replica"], ["D1"], min_commit=c1)
    body = resp.json()
    assert body["ready"] is True
    assert body["source"] == "replica"
    # 同次读取所见的提交号是副本水位，不含主库的新更新
    assert body["snapshot_commit"] == c1
    assert body["devices"]["D1"]["params"]["offset"]["value"] == 1.0
    assert c2 > c1


async def test_wait_then_replica_catches_up_during_wait(env):
    c1 = await commit(env["primary"], [{"device_id": "D1", "params": {"offset": 1.0}}])

    async def replay_later():
        await asyncio.sleep(0.3)
        env["worker"].run_once()

    task = asyncio.create_task(replay_later())
    started = asyncio.get_running_loop().time()
    resp = await query(env["replica"], ["D1"], min_commit=c1, max_wait_ms=2000)
    elapsed = asyncio.get_running_loop().time() - started
    await task

    body = resp.json()
    assert body["ready"] is True
    assert body["source"] == "replica"
    assert body["snapshot_commit"] == c1
    assert 0.2 < elapsed < 1.5  # 确实等了，但副本一达到就返回，没有耗满 2s


async def test_timeout_falls_back_to_primary(env):
    c1 = await commit(env["primary"], [{"device_id": "D1", "params": {"offset": 5.0}}])
    # 不驱动 worker，副本始终为 0；等待超时后主库兜底
    resp = await query(env["replica"], ["D1"], min_commit=c1, max_wait_ms=100)
    body = resp.json()
    assert body["ready"] is True
    assert body["source"] == "primary"
    assert body["snapshot_commit"] >= c1
    assert body["reason"] == "replica_stale_fallback_primary"
    assert body["devices"]["D1"]["params"]["offset"]["value"] == 5.0


async def test_not_ready_when_both_sides_below_min_commit(env):
    await commit(env["primary"], [{"device_id": "D1", "params": {"offset": 1.0}}])
    resp = await query(env["replica"], ["D1"], min_commit=99, max_wait_ms=50)
    assert resp.status_code == 200
    body = resp.json()
    assert body["ready"] is False
    assert body["source"] == "none"
    assert body["min_commit"] == 99
    assert body["devices"] is None


async def test_writes_and_replay_continue_while_query_waits(env):
    # 先放一笔并追上
    c1 = await commit(env["primary"], [{"device_id": "D1", "params": {"offset": 1.0}}])
    env["worker"].run_once()
    c2 = await commit(env["primary"], [{"device_id": "D1", "params": {"offset": 2.0}}])

    waiting = asyncio.create_task(
        query(env["replica"], ["D1"], min_commit=c2, max_wait_ms=3000)
    )
    # 让等待中的查询完成首次读取并进入 asyncio.sleep 轮询（poll 间隔 50ms）
    await asyncio.sleep(0.1)

    # 先回放：此刻主库只有 c2 未追，副本恰好停在 c2
    env["worker"].run_once()
    # 等候期间主库写入照常（c3 此时才产生，等待中的查询不应看到它）
    c3 = await commit(env["primary"], [{"device_id": "D2", "params": {"offset": 3.0}}])
    resp = await asyncio.wait_for(waiting, timeout=2.0)

    body = resp.json()
    assert body["ready"] is True
    assert body["source"] == "replica"
    assert body["snapshot_commit"] == c2  # 不含等待期间之后的 c3

    env["worker"].run_once()  # 再追 c3
    resp = await query(env["replica"], ["D1", "D2"], min_commit=c3)
    devices = resp.json()["devices"]
    assert set(devices) == {"D1", "D2"}
    assert devices["D2"]["params"]["offset"]["commit"] == c3


async def test_fresh_replica_with_zero_min_commit_returns_empty_devices(env):
    resp = await query(env["replica"], ["D1"], min_commit=0)
    body = resp.json()
    assert body["ready"] is True
    assert body["source"] == "replica"
    assert body["snapshot_commit"] == 0
    assert body["devices"] == {}


async def test_batch_query_unknown_device(env):
    c1 = await commit(env["primary"], [{"device_id": "D1", "params": {"offset": 1.0}}])
    env["worker"].run_once()
    resp = await query(env["replica"], ["D1", "NOPE"], min_commit=c1)
    body = resp.json()
    assert body["ready"] is True
    assert set(body["devices"]) == {"D1"}
