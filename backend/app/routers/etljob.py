"""ETL任务接口：维护加工任务，并在启动前对输入源做连接诊断准入核对。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, AdmissionResult, EntryPayload, PageResult
from app.services.etljob import START_ACTION, service

router = APIRouter(prefix="/api/etljob", tags=["ETL任务"])

LIST_FIELDS = ["任务编号", "输入源", "输出目标", "转换脚本", "依赖任务", "调度表达式", "数据分区", "失败重试次数"]
STATUSES = ["草稿", "待调度", "运行中", "已完成", "已失败"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按任务编号检索"),
    status: str | None = Query(default=None, description="草稿、待调度、运行中、已完成、已失败"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按任务编号与状态过滤ETL任务列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出ETL任务清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "etljob", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条加工任务明细（含输入源准入记录）；不存在时给出可读说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"加工任务 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条加工任务，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="加工任务已登记", entry=entry)


@router.post("/{entry_id}/input-check", response_model=AdmissionResult)
def check_inputs(entry_id: int) -> AdmissionResult:
    """只核对、不启动：逐条说明每个输入源采用哪次诊断、为何可用或不可用。"""
    try:
        report = service.check_inputs(entry_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return AdmissionResult(**report)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """执行任务动作；“启动任务”先过输入源准入，拒绝时保留明确依据，不用上次成功遮本次失败。"""
    action = str(payload.values.get("action") or "").strip()
    report = None
    if action == START_ACTION:
        try:
            report = service.check_inputs(entry_id)
        except KeyError as exc:
            return ActionResult(ok=False, message=str(exc))
        if not report["允许启动"]:
            reasons = "；".join(
                item["结论"] for item in report["输入源检查"] if item["判定"] != "准入通过"
            )
            return ActionResult(
                ok=False,
                message=f"输入源准入未通过，任务未启动：{reasons}",
                entry=service.get_entry(entry_id),
                detail={"准入编号": report["准入编号"], "输入源检查": report["输入源检查"]},
            )
    entry, message = service.run_action(entry_id, action, admission_report=report)
    if entry is None:
        return ActionResult(ok=False, message=message)
    detail = None
    if action == START_ACTION:
        detail = {"准入编号": report["准入编号"], "输入源检查": report["输入源检查"]}
    return ActionResult(ok=True, message=message, entry=entry, detail=detail)
