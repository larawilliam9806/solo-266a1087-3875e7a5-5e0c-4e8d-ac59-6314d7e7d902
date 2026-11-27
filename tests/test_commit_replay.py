"""提交与回放基础语义。"""

import pytest

from app import db

from tests.helpers import commit, query

pytestmark = pytest.mark.asyncio


async def test_multi_device_commit_returns_sequence(env):
    seq1 = await commit(env["primary"], [
        {"device_id": "D1", "params": {"offset": 1.5}},
        {"device_id": "D2", "params": {"offset": -2.0, "gain": 3.0}},
    ])
    seq2 = await commit(env["primary"], [
        {"device_id": "D3", "params": {"offset": 0.0}},
    ])
    assert seq2 == seq1 + 1

    progress = (await env["primary"].get("/progress")).json()
    assert progress["latest_commit"] == seq2


async def test_replica_replays_each_original_change_in_commit_order(env):
    c1 = await commit(env["primary"], [{"device_id": "D1", "params": {"offset": 1.0}}])
    c2 = await commit(env["primary"], [
        {"device_id": "D1", "params": {"offset": 2.0}},
        {"device_id": "D2", "params": {"offset": 9.0}},
    ])
    c3 = await commit(env["primary"], [{"device_id": "D2", "params": {"offset": 10.0}}])

    # 每轮最多 2 笔，两轮追完
    applied1 = env["worker"].run_once()
    applied2 = env["worker"].run_once()
    applied3 = env["worker"].run_once()
    assert (applied1, applied2, applied3) == (2, 1, 0)

    with db.session(env["replica_app"].state.settings.db_path) as conn:
        assert db.replica_last_seq(conn) == c3
        rows = conn.execute(
            "SELECT seq, device_id, params FROM change_log ORDER BY seq, seq_ord"
        ).fetchall()
    assert [tuple(r) for r in rows] == [
        (c1, "D1", '{"offset": 1.0}'),
        (c2, "D1", '{"offset": 2.0}'),
        (c2, "D2", '{"offset": 9.0}'),
        (c3, "D2", '{"offset": 10.0}'),
    ]


async def test_replay_is_idempotent_and_progress_durable(env):
    await commit(env["primary"], [{"device_id": "D1", "params": {"offset": 1.0}}])
    env["worker"].run_once()
    # 再跑一轮不应重复回放
    assert env["worker"].run_once() == 0

    progress = (await env["replica"].get("/progress")).json()
    assert progress["replayed_commit"] == 1
    assert progress["lag"] == 0


async def test_max_catchup_limits_replay_per_round(env):
    # conftest 中 max_catchup=2
    for i in range(5):
        await commit(env["primary"], [
            {"device_id": f"D{i}", "params": {"offset": float(i)}}
        ])
    assert env["worker"].run_once() == 2
    assert env["worker"].run_once() == 2
    assert env["worker"].run_once() == 1


async def test_empty_or_duplicate_changes_rejected(env):
    bad = await env["primary"].post("/commits", json={"changes": []})
    assert bad.status_code == 422

    dup = await env["primary"].post("/commits", json={"changes": [
        {"device_id": "D1", "params": {"offset": 1.0}},
        {"device_id": "D1", "params": {"offset": 2.0}},
    ]})
    assert dup.status_code == 422


async def test_params_and_progress_commit_together(env):
    # 回放后参数与进度在同一库中一致
    c1 = await commit(env["primary"], [{"device_id": "D1", "params": {"offset": 7.0}}])
    env["worker"].run_once()
    resp = await query(env["replica"], ["D1"])
    body = resp.json()
    assert body["ready"] is True
    assert body["source"] == "replica"
    assert body["snapshot_commit"] == c1
    assert body["devices"]["D1"]["params"]["offset"] == {
        "value": 7.0,
        "commit": c1,
    }
