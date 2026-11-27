"""主库路由：提交写入、内部回放接口、兜底快照、健康检查。"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from .. import db
from ..schemas import (
    CommitRequest,
    CommitResponse,
    HealthResponse,
    ProgressResponse,
)

router = APIRouter()


def _settings(request: Request):
    return request.app.state.settings


@router.post("/commits", response_model=CommitResponse)
async def create_commit(payload: CommitRequest, request: Request) -> CommitResponse:
    """多台设备的改动一起提交，返回一个提交号。"""
    settings = _settings(request)
    changes = [item.model_dump() for item in payload.changes]
    loop = asyncio.get_running_loop()
    seq = await loop.run_in_executor(None, db.create_commit_path, settings.db_path, changes)
    return CommitResponse(commit=seq, change_count=len(changes))


@router.get("/internal/commits")
async def internal_commits(request: Request, after: int = 0, limit: int = 100):
    """副本回放线程按提交顺序拉取每笔原始改动。"""
    settings = _settings(request)
    loop = asyncio.get_running_loop()
    commits = await loop.run_in_executor(
        None, db.fetch_commits_after_path, settings.db_path, after, limit
    )
    return {"after": after, "limit": limit, "commits": commits}


@router.get("/internal/watermark")
async def internal_watermark(request: Request):
    settings = _settings(request)
    loop = asyncio.get_running_loop()
    latest = await loop.run_in_executor(
        None, db.latest_seq_path, settings.db_path
    )
    return {"latest_commit": latest}


@router.post("/internal/snapshot")
async def internal_snapshot(request: Request):
    """副本等待超时后的主库兜底读取（同事务水位 + 参数）。"""
    settings = _settings(request)
    body = await request.json()
    device_ids = [str(x) for x in body.get("device_ids", [])]
    min_commit = int(body.get("min_commit", 0))
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None, db.read_snapshot_primary_path, settings.db_path, device_ids, min_commit
    )
    return JSONResponse(result)


@router.get("/progress", response_model=ProgressResponse)
async def progress(request: Request) -> ProgressResponse:
    settings = _settings(request)
    loop = asyncio.get_running_loop()
    latest = await loop.run_in_executor(None, db.latest_seq_path, settings.db_path)
    return ProgressResponse(
        role=settings.role, db_path=settings.db_path, latest_commit=latest
    )


@router.get("/health", response_model=HealthResponse)
async def health(request: Request) -> HealthResponse:
    settings = _settings(request)
    loop = asyncio.get_running_loop()
    latest = await loop.run_in_executor(None, db.latest_seq_path, settings.db_path)
    return HealthResponse(
        status="ok",
        role=settings.role,
        detail={"latest_commit": latest, "db_path": settings.db_path},
    )
