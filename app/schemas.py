"""请求 / 响应模型。"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


class DeviceChange(BaseModel):
    device_id: str = Field(..., min_length=1)
    params: dict[str, float] = Field(..., min_length=1)


class CommitRequest(BaseModel):
    changes: list[DeviceChange] = Field(..., min_length=1)

    @field_validator("changes")
    @classmethod
    def _unique_device(cls, value: list[DeviceChange]) -> list[DeviceChange]:
        ids = [item.device_id for item in value]
        if len(ids) != len(set(ids)):
            raise ValueError("同一提交内 device_id 不可重复")
        return value


class CommitResponse(BaseModel):
    commit: int
    change_count: int


class ParamValue(BaseModel):
    value: float
    commit: int


class DeviceParams(BaseModel):
    device_id: str
    params: dict[str, ParamValue]


class QueryResponse(BaseModel):
    ready: bool
    source: str
    snapshot_commit: int
    min_commit: int
    devices: dict[str, DeviceParams] | None = None
    reason: str | None = None


class ProgressResponse(BaseModel):
    role: str
    db_path: str
    latest_commit: int
    replayed_commit: int | None = None
    lag: int | None = None


class HealthResponse(BaseModel):
    status: str
    role: str
    detail: dict[str, Any] | None = None
