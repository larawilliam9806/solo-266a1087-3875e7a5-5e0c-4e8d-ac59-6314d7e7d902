"""SQLite 持久化：提交日志、原始改动、当前参数、回放进度。"""

from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS commit_log (
    seq         INTEGER PRIMARY KEY,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    change_cnt  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS change_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    seq         INTEGER NOT NULL,
    device_id   TEXT NOT NULL,
    params      TEXT NOT NULL,
    seq_ord     INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS param_current (
    device_id   TEXT NOT NULL,
    param_name  TEXT NOT NULL,
    value       REAL NOT NULL,
    seq         INTEGER NOT NULL,
    PRIMARY KEY (device_id, param_name)
);

CREATE TABLE IF NOT EXISTS replay_progress (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    last_seq        INTEGER NOT NULL,
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    parent = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


@contextmanager
def session(db_path: str) -> Iterator[sqlite3.Connection]:
    conn = connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


def init_db(db_path: str) -> None:
    with session(db_path) as conn:
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT OR IGNORE INTO replay_progress(id, last_seq) VALUES (1, 0)"
        )


# ---------------------------------------------------------------------------
# 主库写入
# ---------------------------------------------------------------------------

def create_commit(
    conn: sqlite3.Connection, changes: list[dict[str, Any]]
) -> int:
    """一次提交多台设备改动，返回提交号。提交号、原始改动、当前参数同事务落库。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            "INSERT INTO commit_log(change_cnt) VALUES (?)", (len(changes),)
        )
        seq = int(cur.lastrowid)
        for ordinal, change in enumerate(changes):
            device_id = change["device_id"]
            params: dict[str, float] = change["params"]
            conn.execute(
                "INSERT INTO change_log(seq, device_id, params, seq_ord) "
                "VALUES (?, ?, ?, ?)",
                (seq, device_id, json.dumps(params, ensure_ascii=False), ordinal),
            )
            for name, value in params.items():
                conn.execute(
                    "INSERT INTO param_current(device_id, param_name, value, seq) "
                    "VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(device_id, param_name) DO UPDATE SET "
                    "value=excluded.value, seq=excluded.seq",
                    (device_id, name, float(value), seq),
                )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return seq


def create_commit_path(db_path: str, changes: list[dict[str, Any]]) -> int:
    with session(db_path) as conn:
        return create_commit(conn, changes)


# ---------------------------------------------------------------------------
# 主库读取（供副本回放与主库兜底查询）
# ---------------------------------------------------------------------------

def fetch_commits_after_path(
    db_path: str, last_seq: int, limit: int | None
) -> list[dict[str, Any]]:
    with session(db_path) as conn:
        return fetch_commits_after(conn, last_seq, limit)


def latest_seq_path(db_path: str) -> int:
    with session(db_path) as conn:
        return latest_seq(conn)


def fetch_commits_after(
    conn: sqlite3.Connection, last_seq: int, limit: int | None
) -> list[dict[str, Any]]:
    """按提交顺序取回待回放提交，每笔包含其全部原始改动（保留提交内顺序）。

    注意 LIMIT 作用在提交（commit_log 行）上，而不是 JOIN 后的改动行，
    否则一笔多设备提交会被截断成半个提交。
    """
    seq_sql = "SELECT seq FROM commit_log WHERE seq > ? ORDER BY seq ASC"
    params: list[Any] = [last_seq]
    if limit is not None and limit > 0:
        seq_sql += " LIMIT ?"
        params.append(int(limit))
    seq_rows = conn.execute(seq_sql, params).fetchall()
    if not seq_rows:
        return []
    seqs = [int(row["seq"]) for row in seq_rows]
    placeholders = ",".join("?" for _ in seqs)
    rows = conn.execute(
        f"SELECT cl.seq, cl.created_at, c.device_id, c.params, c.seq_ord "
        f"FROM commit_log cl "
        f"JOIN change_log c ON c.seq = cl.seq "
        f"WHERE cl.seq IN ({placeholders}) "
        f"ORDER BY cl.seq ASC, c.seq_ord ASC",
        seqs,
    ).fetchall()
    commits: dict[int, dict[str, Any]] = {}
    for row in rows:
        seq = int(row["seq"])
        item = commits.setdefault(
            seq,
            {"seq": seq, "created_at": row["created_at"], "changes": []},
        )
        item["changes"].append(
            {
                "device_id": row["device_id"],
                "params": json.loads(row["params"]),
            }
        )
    return [commits[s] for s in sorted(commits)]


def latest_seq(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COALESCE(MAX(seq), 0) AS s FROM commit_log").fetchone()
    return int(row["s"])


def read_snapshot_primary(
    conn: sqlite3.Connection, device_ids: list[str], min_commit: int
) -> dict[str, Any]:
    """主库一致快照：同一只读事务里取水位与参数。min_commit 不满足时 not_ready。"""
    conn.execute("BEGIN")
    try:
        snapshot = latest_seq(conn)
        if snapshot < min_commit:
            conn.execute("ROLLBACK")
            return {"ready": False, "source": "primary", "snapshot_commit": snapshot}
        placeholders = ",".join("?" for _ in device_ids)
        rows = conn.execute(
            f"SELECT device_id, param_name, value, seq "
            f"FROM param_current WHERE device_id IN ({placeholders})",
            device_ids,
        ).fetchall() if device_ids else []
        devices = _group_device_rows(rows)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return {
        "ready": True,
        "source": "primary",
        "snapshot_commit": snapshot,
        "devices": devices,
    }


# ---------------------------------------------------------------------------
# 副本回放与读取
# ---------------------------------------------------------------------------

def apply_commit(conn: sqlite3.Connection, commit: dict[str, Any]) -> int:
    """按原始改动回放一笔提交；参数与回放进度在同一个事务里落库。"""
    seq = int(commit["seq"])
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT last_seq FROM replay_progress WHERE id = 1"
        ).fetchone()
        last_seq = int(row["last_seq"])
        if seq <= last_seq:
            # 已回放（如轮询重试），幂等跳过
            conn.execute("ROLLBACK")
            return last_seq
        if seq != last_seq + 1:
            conn.execute("ROLLBACK")
            raise RuntimeError(
                f"回放顺序断裂：本地 last_seq={last_seq}，收到 seq={seq}"
            )
        for ordinal, change in enumerate(commit["changes"]):
            device_id = change["device_id"]
            params = change["params"]
            conn.execute(
                "INSERT INTO change_log(seq, device_id, params, seq_ord) "
                "VALUES (?, ?, ?, ?)",
                (seq, device_id, json.dumps(params, ensure_ascii=False), ordinal),
            )
            for name, value in params.items():
                conn.execute(
                    "INSERT INTO param_current(device_id, param_name, value, seq) "
                    "VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(device_id, param_name) DO UPDATE SET "
                    "value=excluded.value, seq=excluded.seq",
                    (device_id, name, float(value), seq),
                )
        conn.execute(
            "INSERT INTO commit_log(seq, created_at, change_cnt) "
            "VALUES (?, ?, ?) "
            "ON CONFLICT(seq) DO NOTHING",
            (seq, commit.get("created_at"), len(commit["changes"])),
        )
        conn.execute(
            "UPDATE replay_progress SET last_seq = ?, "
            "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = 1",
            (seq,),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return seq


def replica_last_seq(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT last_seq FROM replay_progress WHERE id = 1"
    ).fetchone()
    return int(row["last_seq"]) if row is not None else 0


def read_snapshot_replica(
    conn: sqlite3.Connection, device_ids: list[str], min_commit: int
) -> dict[str, Any]:
    """副本一致快照：水位（回放进度）与参数同一只读事务读取。"""
    conn.execute("BEGIN")
    try:
        snapshot = replica_last_seq(conn)
        if snapshot < min_commit:
            conn.execute("ROLLBACK")
            return {"ready": False, "source": "replica", "snapshot_commit": snapshot}
        placeholders = ",".join("?" for _ in device_ids)
        rows = conn.execute(
            f"SELECT device_id, param_name, value, seq "
            f"FROM param_current WHERE device_id IN ({placeholders})",
            device_ids,
        ).fetchall() if device_ids else []
        devices = _group_device_rows(rows)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return {
        "ready": True,
        "source": "replica",
        "snapshot_commit": snapshot,
        "devices": devices,
    }


def read_snapshot_replica_path(
    db_path: str, device_ids: list[str], min_commit: int
) -> dict[str, Any]:
    """便捷封装：自带连接的副本快照读取，便于丢进线程池。"""
    with session(db_path) as conn:
        return read_snapshot_replica(conn, device_ids, min_commit)


def read_snapshot_primary_path(
    db_path: str, device_ids: list[str], min_commit: int
) -> dict[str, Any]:
    """便捷封装：自带连接的主库快照读取，便于丢进线程池。"""
    with session(db_path) as conn:
        return read_snapshot_primary(conn, device_ids, min_commit)


def _group_device_rows(rows: list[sqlite3.Row]) -> dict[str, dict[str, Any]]:
    devices: dict[str, dict[str, Any]] = {}
    for row in rows:
        device = devices.setdefault(
            row["device_id"], {"device_id": row["device_id"], "params": {}}
        )
        device["params"][row["param_name"]] = {
            "value": row["value"],
            "commit": int(row["seq"]),
        }
    return devices
