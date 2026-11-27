"""本机端到端演示：起主库 + 副本两个 uvicorn，跑通全部查询语义。

用法：python scripts/demo.py
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from urllib import request as urlreq
from urllib.error import HTTPError

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def wait_port(port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"端口 {port} 未就绪")


def call(method: str, url: str, payload: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urlreq.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("content-type", "application/json")
    try:
        with urlreq.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read())
    except HTTPError as exc:
        return exc.code, json.loads(exc.read())


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="paramstore-demo-")
    env = os.environ.copy()
    env["PYTHONPATH"] = ROOT
    primary_db = os.path.join(tmp, "primary.db")
    replica_db = os.path.join(tmp, "replica.db")
    log = open(os.path.join(tmp, "servers.log"), "w")

    procs: list[subprocess.Popen] = []
    try:
        procs.append(subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", "18000"],
            cwd=ROOT, env={**env, "APP_ROLE": "primary", "DB_PATH": primary_db,
                           "REPLAY_AUTO_START": "false"},
            stdout=log, stderr=subprocess.STDOUT,
        ))
        wait_port(18000)
        procs.append(subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", "18001"],
            cwd=ROOT, env={**env, "APP_ROLE": "replica", "DB_PATH": replica_db,
                           "PRIMARY_URL": "http://127.0.0.1:18000",
                           "REPLICA_MAX_CATCHUP": "2",
                           "REPLICA_POLL_INTERVAL": "1.0"},
            stdout=log, stderr=subprocess.STDOUT,
        ))
        wait_port(18001)

        p, r = "http://127.0.0.1:18000", "http://127.0.0.1:18001"

        print("1. 多设备提交：")
        _, c1 = call("POST", f"{p}/commits", {"changes": [
            {"device_id": "DEV-1", "params": {"offset": 1.1}},
            {"device_id": "DEV-2", "params": {"offset": 2.2}},
        ]})
        print("  ->", c1)

        print("2. 等待副本回放后从副本读（轮询间隔 1s）：")
        time.sleep(2.2)
        print("  ->", call("GET", f"{r}/devices/params?device_id=DEV-1&device_id=DEV-2&min_commit=1")[1])

        print("3. 主库再写一笔，50ms 内副本追不上 -> 主库兜底：")
        _, c2 = call("POST", f"{p}/commits",
                     {"changes": [{"device_id": "DEV-3", "params": {"offset": 3.3}}]})
        print("  commit =", c2["commit"])
        print("  ->", call("GET", f"{r}/devices/params?device_id=DEV-3&min_commit={c2['commit']}&max_wait_ms=50")[1])

        print("4. 两边都达不到的最低号 -> 未就绪：")
        print("  ->", call("GET", f"{r}/devices/params?device_id=DEV-3&min_commit=999&max_wait_ms=50")[1])

        print("5. 副本追平后回到 replica：")
        time.sleep(2.2)
        print("  ->", call("GET", f"{r}/devices/params?device_id=DEV-3&min_commit={c2['commit']}")[1])
        print("  progress ->", call("GET", f"{r}/progress")[1])
        return 0
    finally:
        for proc in procs:
            proc.send_signal(signal.SIGINT)
        for proc in procs:
            proc.wait(timeout=5)
        log.close()
        print(f"\n服务日志与数据库位于: {tmp}")


if __name__ == "__main__":
    raise SystemExit(main())
