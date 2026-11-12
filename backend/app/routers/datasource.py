"""数据源连接接口。

“建立连接”不再只是改标记：
- POST /{id}/diagnose 对当前配置执行只读探测，返回分阶段结论，并落库一份诊断记录；
- POST /{id}/diagnose/cancel 只取消本次调用对应的等待者，不影响同配置的其他诊断；
- PATCH /{id} 修改配置，指纹相关字段或凭据变化后旧诊断立即过期；
- 认证凭据只入进程内保险库，任何响应都不回显。
"""
from __future__ import annotations

import threading
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, AdmissionResult, EntryPayload, PageResult
from app.services.datasource import DIAGNOSE_ACTION, service

router = APIRouter(prefix="/api/datasource", tags=["数据源连接"])

LIST_FIELDS = ["配置编号", "连接类型", "接入地址", "认证方式", "连接池大小", "字符编码", "查询超时秒数", "重试上限"]
STATUSES = ["草稿", "待校验", "已连接", "已断开"]

# 诊断接口允许同步等待的在途取消令牌：waiter_id -> 取消事件。
_pending_waiters: dict[str, threading.Event] = {}
_pending_lock = threading.Lock()


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按配置编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、已连接、已断开"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按配置编号与状态过滤数据源连接列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出数据源连接清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "datasource", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条数据源配置明细（含最近一次诊断摘要）；不存在时给出可读说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"数据源配置 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条数据源配置，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="数据源配置已登记", entry=entry)


@router.patch("/{entry_id}", response_model=ActionResult)
def update_entry(entry_id: int, payload: EntryPayload) -> ActionResult:
    """修改配置；认证方式、接入地址、超时/重试或凭据变化时，旧诊断立即失效。"""
    entry, message = service.update_entry(entry_id, payload.values)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """兼容旧调用链：提交校验、断开连接照旧；“建立连接”内部转为只读连接诊断。"""
    action = str(payload.values.get("action") or "").strip()
    if action == DIAGNOSE_ACTION:
        return diagnose_entry(entry_id, payload)
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/{entry_id}/diagnose", response_model=ActionResult)
def diagnose_entry(entry_id: int, payload: EntryPayload | None = None) -> ActionResult:
    """对当前配置执行只读连接诊断，返回分阶段结论；并发同配置请求只发出一次探测。"""
    existing = service.get_entry(entry_id)
    if existing is None:
        return ActionResult(ok=False, message=f"数据源配置 {entry_id} 不存在或已归档")

    import uuid as _uuid

    # 允许调用方自带 waiter_id（用于随后调用取消接口）；未提供则由服务端生成。
    client_waiter = str((payload.values.get("waiter_id") if payload and payload.values else "") or "").strip()
    waiter_id = client_waiter or _uuid.uuid4().hex
    cancel_event = threading.Event()
    with _pending_lock:
        _pending_waiters[waiter_id] = cancel_event
    try:
        result = service.diagnose(entry_id, waiter_id=waiter_id, cancel_event=cancel_event)
        if result.result == "已取消":
            return ActionResult(
                ok=False,
                message="诊断等待已取消，未形成准入结论",
                entry=service.get_entry(entry_id),
                detail={"waiter_id": waiter_id, "诊断": result.to_dict()},
            )
        ok = result.result == "成功"
        message = (
            "连接诊断通过：地址格式、握手、认证与只读探针全部成功"
            if ok else f"连接诊断未通过，失败阶段：{result.failed_phase or '未知'}；{result.message}"
        )
        return ActionResult(
            ok=ok,
            message=message,
            entry=service.get_entry(entry_id),
            detail={"waiter_id": waiter_id, "诊断": result.to_dict()},
        )
    except KeyError as exc:
        return ActionResult(ok=False, message=str(exc))
    finally:
        with _pending_lock:
            _pending_waiters.pop(waiter_id, None)


@router.post("/{entry_id}/diagnose/cancel", response_model=ActionResult)
def cancel_diagnose(entry_id: int, payload: EntryPayload) -> ActionResult:
    """取消一个等待中的诊断调用；不取消同配置其他调用所需的探测。"""
    waiter_id = str(payload.values.get("waiter_id") or "").strip()
    if not waiter_id:
        return ActionResult(ok=False, message="缺少 waiter_id，无法定位要取消的等待者")
    with _pending_lock:
        event = _pending_waiters.get(waiter_id)
    if event is None:
        return ActionResult(ok=False, message="该等待者不存在或已结束，无需取消")
    event.set()
    cancelled = service.cancel_diagnosis(entry_id, waiter_id)
    return ActionResult(ok=cancelled, message="等待者已取消，共享探测不受影响" if cancelled else "等待者已结束")


@router.get("/{entry_id}/diagnoses")
def list_diagnoses(entry_id: int) -> dict[str, Any]:
    """列出该配置的历史诊断记录（新到旧），供核对 ETL 准入采用的是哪一次结论。"""
    if service.get_entry(entry_id) is None:
        raise HTTPException(status_code=404, detail=f"数据源配置 {entry_id} 不存在或已归档")
    items = service.list_diagnoses(entry_id)
    return {"module": "datasource_diagnosis", "数据源配置ID": entry_id, "total": len(items), "items": items}
