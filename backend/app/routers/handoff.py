"""合并批次交接路由：批次确认记录、受理进度、清理预演的持久化与恢复入口。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.compaction import CompactionService
from app.services.handoff import handoff_service

router = APIRouter(prefix="/api/compaction-handoff", tags=["合并批次交接"])


class BatchSubmitPayload(BaseModel):
    基准快照: str
    计划文件: list[str] = Field(default_factory=list)
    实际替换文件: list[str] = Field(default_factory=list)
    interrupted: bool = False


class ClaimPayload(BaseModel):
    operator: str = "值班员"


def _ok(message: str, **extra: Any) -> dict[str, Any]:
    return {"ok": True, "message": message, **extra}


def _fail(message: str) -> dict[str, Any]:
    return {"ok": False, "message": message}


@router.get("/workbench")
def workbench() -> dict[str, Any]:
    """统计与预演工作台：累计替换数、批次确认记录、预演领取队列。"""
    return handoff_service.workbench()


@router.get("/recover")
def recover() -> dict[str, Any]:
    """中断恢复视图：统计待确认、预演待生成、预演待领取分别列出。"""
    return handoff_service.recover_pending()


@router.get("/batches")
def list_batches() -> dict[str, Any]:
    return {"items": handoff_service.list_batches(), "total": len(handoff_service.list_batches())}


@router.get("/batches/{batch_id}")
def get_batch(batch_id: int) -> dict[str, Any]:
    batch = handoff_service.find_batch(batch_id)
    if batch is None:
        return _fail(f"合并批次 {batch_id} 不存在")
    rehearsal = handoff_service._rehearsal_of_batch(batch_id)
    progress = handoff_service._progress_of_batch(batch["批次编号"])
    return _ok("批次确认记录", batch=batch, rehearsal=rehearsal, progress=progress)


@router.post("/tasks/{entry_id}/batches")
def submit_task_batch(entry_id: int, payload: BatchSubmitPayload) -> dict[str, Any]:
    """合并任务完成后按实际替换文件提交批次（支持重送幂等与中断模拟）。"""
    try:
        result = CompactionService().submit_compaction_batch(
            entry_id,
            payload.model_dump(),
            interrupted=payload.interrupted,
        )
    except ValueError as exc:
        return _fail(str(exc))
    verb = "重送已幂等合并" if result["resubmitted"] else "批次已登记"
    tail = "，统计已确认、预演交接中断" if result["interrupted"] else ""
    return _ok(f"{verb}，统计按实际替换数受理{tail}", **result)


@router.post("/batches/{batch_id}/statistics")
def confirm_statistics(batch_id: int, payload: ClaimPayload | None = None) -> dict[str, Any]:
    """单独确认/恢复一笔批次的统计受理（每批只记一次）。"""
    try:
        result = handoff_service.confirm_statistics(batch_id)
    except ValueError as exc:
        return _fail(str(exc))
    return _ok("统计已确认" if not result.get("replayed") else "统计此前已确认，未重复累计", **result)


@router.post("/batches/{batch_id}/rehearsal")
def ensure_rehearsal(batch_id: int) -> dict[str, Any]:
    """生成或恢复该批次的清理预演（一批一份，重复调用幂等）。"""
    try:
        rehearsal = handoff_service.ensure_rehearsal(batch_id)
    except ValueError as exc:
        return _fail(str(exc))
    return _ok("清理预演已就绪，可领取", rehearsal=rehearsal)


@router.get("/rehearsals")
def list_rehearsals() -> dict[str, Any]:
    return {"items": handoff_service.list_rehearsals(), "total": len(handoff_service.list_rehearsals())}


@router.post("/rehearsals/{rehearsal_id}/claim")
def claim_rehearsal(rehearsal_id: int, payload: ClaimPayload | None = None) -> dict[str, Any]:
    """值班员领取预演：返回本次复核路径、所属批次与其余可领取预演。"""
    operator = payload.operator if payload else "值班员"
    try:
        result = handoff_service.claim_rehearsal(rehearsal_id, operator=operator)
    except ValueError as exc:
        return _fail(str(exc))
    return _ok("预演已领取", **result)


@router.post("/rehearsals/{rehearsal_id}/reschedule")
def reschedule_rehearsal(rehearsal_id: int) -> dict[str, Any]:
    """重新安排尚未领取的预演；已领取范围冻结。"""
    try:
        rehearsal = handoff_service.reschedule_rehearsal(rehearsal_id)
    except ValueError as exc:
        return _fail(str(exc))
    return _ok("预演已重新安排", rehearsal=rehearsal)
