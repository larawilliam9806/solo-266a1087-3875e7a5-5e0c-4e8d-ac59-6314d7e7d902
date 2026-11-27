"""副本路由：带最低提交号与最长等候时间的批量设备查询。"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from .. import db
from ..schemas import HealthResponse, ProgressResponse

router = APIRouter()


def _settings(request: Request):
    return request.app.state.settings


@router.get("/devices/params")
async def query_devices(
    request: Request,
    device_id: list[str] = Query(default=None, alias="device_id"),
    min_commit: int = 0,
    max_wait_ms: int = 0,
):
    """按设备编号批量查询。

    路由规则：
    1. 副本水位 >= min_commit：直接读副本（即使主库已有更新）；
    2. 否则轮询等待至 max_wait_ms，期间写入 / 回放照常；
    3. 到时副本仍未达到：查主库，主库达到则返回 source=primary；
    4. 两边都没达到：ready=false（未就绪）。
    """
    settings = _settings(request)
    device_ids = device_id or []
    max_wait = max(max_wait_ms, 0) / 1000.0
    loop = asyncio.get_running_loop()

    # 1. 先看副本是否已达到最低提交号（SQLite 在线程池执行，不阻塞事件循环）
    result = await loop.run_in_executor(
        None, db.read_snapshot_replica_path, settings.db_path, device_ids, min_commit
    )
    if result["ready"]:
        result["min_commit"] = min_commit
        return JSONResponse(result)

    # 2. 未达到则等待；等待不阻塞写入与回放线程
    poll = max(settings.poll_interval, 0.01)
    deadline = loop.time() + max_wait
    while loop.time() < deadline:
        await asyncio.sleep(min(poll, max(deadline - loop.time(), 0.0)))
        result = await loop.run_in_executor(
            None,
            db.read_snapshot_replica_path,
            settings.db_path,
            device_ids,
            min_commit,
        )
        if result["ready"]:
            result["min_commit"] = min_commit
            return JSONResponse(result)

    # 3. 到时仍未达到，兜底查主库
    try:
        primary_result = await request.app.state.primary_client.read_snapshot(
            device_ids, min_commit
        )
    except Exception as exc:  # 主库不可达
        return JSONResponse(
            {
                "ready": False,
                "source": "unavailable",
                "snapshot_commit": result["snapshot_commit"],
                "min_commit": min_commit,
                "devices": None,
                "reason": f"副本未达到最低提交号且主库不可用: {exc}",
            },
            status_code=502,
        )

    if primary_result.get("ready"):
        primary_result["min_commit"] = min_commit
        primary_result["reason"] = "replica_stale_fallback_primary"
        return JSONResponse(primary_result)

    # 4. 两边都没达到
    return JSONResponse(
        {
            "ready": False,
            "source": "none",
            "snapshot_commit": result["snapshot_commit"],
            "min_commit": min_commit,
            "devices": None,
            "reason": "primary_and_replica_both_below_min_commit",
        }
    )


@router.get("/progress", response_model=ProgressResponse)
async def progress(request: Request) -> ProgressResponse:
    settings = _settings(request)
    loop = asyncio.get_running_loop()

    def _replica_seq() -> int:
        with db.session(settings.db_path) as conn:
            return db.replica_last_seq(conn)

    replayed = await loop.run_in_executor(None, _replica_seq)
    try:
        latest = await request.app.state.primary_client.primary_latest()
    except Exception:
        latest = replayed
    return ProgressResponse(
        role=settings.role,
        db_path=settings.db_path,
        latest_commit=latest,
        replayed_commit=replayed,
        lag=max(latest - replayed, 0),
    )


@router.get("/health", response_model=HealthResponse)
async def health(request: Request) -> HealthResponse:
    settings = _settings(request)

    def _seq() -> int:
        with db.session(settings.db_path) as conn:
            return db.replica_last_seq(conn)

    replayed = await asyncio.get_running_loop().run_in_executor(None, _seq)
    return HealthResponse(
        status="ok",
        role=settings.role,
        detail={"replayed_commit": replayed, "db_path": settings.db_path},
    )
