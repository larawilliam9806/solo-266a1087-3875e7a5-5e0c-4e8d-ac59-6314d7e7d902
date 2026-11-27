"""运行配置，全部来自环境变量。"""

from __future__ import annotations

import os
from dataclasses import dataclass

ROLE_PRIMARY = "primary"
ROLE_REPLICA = "replica"


@dataclass(frozen=True)
class Settings:
    role: str = ROLE_PRIMARY
    db_path: str = "/data/primary.db"
    # 副本拉取主库的地址（replica 角色使用）
    primary_url: str = "http://primary:8000"
    # 副本一轮最多回放几笔提交；<=0 表示不限
    max_catchup: int = 5
    # 副本轮询间隔（秒）
    poll_interval: float = 0.2
    # 每笔提交回放后的模拟追平延迟（秒）
    replay_lag: float = 0.0
    # 是否在应用启动时自动启动回放线程（测试时关闭）
    replay_auto_start: bool = True


def _as_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_settings() -> Settings:
    role = os.getenv("APP_ROLE", ROLE_PRIMARY).strip().lower()
    if role not in {ROLE_PRIMARY, ROLE_REPLICA}:
        raise ValueError(f"非法 APP_ROLE: {role!r}，应为 primary 或 replica")
    return Settings(
        role=role,
        db_path=os.getenv("DB_PATH", "./data/primary.db"),
        primary_url=os.getenv("PRIMARY_URL", "http://primary:8000").rstrip("/"),
        max_catchup=int(os.getenv("REPLICA_MAX_CATCHUP", "5")),
        poll_interval=float(os.getenv("REPLICA_POLL_INTERVAL", "0.2")),
        replay_lag=float(os.getenv("REPLICA_LAG", "0.0")),
        replay_auto_start=_as_bool(os.getenv("REPLAY_AUTO_START", "true")),
    )
