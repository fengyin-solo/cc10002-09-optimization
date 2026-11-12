"""数据源连接业务规则。

在原“登记 + 状态流转”的基础上，把“建立连接”升级为可核对的连接诊断：
- 诊断动作调用 `diagnosis.engine`，产出分阶段、带共享预算与尝试次数的结论并落库；
- 认证方式 / 接入地址 / 查询超时 / 重试上限 / 凭据变更后，旧诊断标记为过期，
  不能再作为 ETL 输入源准入依据；
- 认证凭据只保存在进程内的凭据保险库（不落库、不进任何响应），诊断时临时取用；
- ETL 准入与页面诊断共用同一份诊断记录（见 `latest_diagnosis`）。
"""
from __future__ import annotations

import threading
from typing import Any

from app.services.diagnosis import (
    RESULT_CANCELLED,
    RESULT_SUCCESS,
    DiagnosisResult,
    ProbeConfig,
    engine,
)
from app.store import store

MODULE = "datasource"
DIAG_MODULE = "datasource_diagnosis"
REQUIRED_FIELDS = ["配置编号", "连接类型", "接入地址"]
EDITABLE_FIELDS = [
    "配置编号", "连接类型", "接入地址", "认证方式", "连接池大小",
    "字符编码", "查询超时秒数", "重试上限",
]
# 参与配置指纹的字段：这些值（连同认证凭据）一变，旧诊断立即失效。
FINGERPRINT_FIELDS = ("接入地址", "认证方式", "查询超时秒数", "重试上限")
STATUS_ORDER = ["草稿", "待校验", "已连接", "已断开"]
# “建立连接”保留为动作名以兼容旧调用链，内部改为发起只读诊断，不再只是改标记。
ACTION_RULES = {"提交校验": "待校验", "断开连接": "已断开"}
DIAGNOSE_ACTION = "建立连接"
NEGATIVE_ACTIONS: list[str] = []

_DEFAULT_TIMEOUT = 5.0
_DEFAULT_RETRIES = 1
_DEFAULT_POOL = 2
_POOL_LIMIT = 8


def _to_float(value: Any, default: float) -> float:
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return default
    if number <= 0:
        return default
    # 上限保护：防止异常配置让一次诊断长期占用连接名额。
    return min(number, 60.0)


def _to_int(value: Any, default: int, *, upper: int | None = None) -> int:
    try:
        number = int(float(str(value).strip()))
    except (TypeError, ValueError):
        number = default
    number = max(number, 0)
    if upper is not None:
        number = min(number, upper)
    return number


class DatasourceService:
    def __init__(self) -> None:
        # 凭据保险库：datasource_id -> 凭据明文。仅进程内、仅诊断时读取，不参与序列化。
        self._secrets: dict[int, str] = {}
        self._secret_lock = threading.Lock()

    # ------------------------------------------------------------------ 查询

    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("配置编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        visible = [self.public_view(row) for row in rows[start:start + size]]
        for view in visible:
            view["已登记认证凭据"] = self.describe_secret(int(view["id"]))
        return visible, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        row = store.find(MODULE, entry_id)
        if row is None:
            return None
        view = self.public_view(row)
        view["已登记认证凭据"] = self.describe_secret(entry_id)
        latest = self.latest_diagnosis(entry_id)
        if latest is not None:
            view["最近诊断"] = latest.to_dict()
        return view

    # ------------------------------------------------------------------ 登记

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in EDITABLE_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        entry["诊断结论"] = None
        entry["配置指纹"] = None
        rows.append(entry)
        secret = str(values.get("认证凭据") or "")
        with self._secret_lock:
            if secret:
                self._secrets[entry["id"]] = secret
        self._sync_pool_cap(entry)
        view = self.public_view(entry)
        view["已登记认证凭据"] = bool(secret)
        return view, []

    def update_entry(
        self, entry_id: int, values: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, str]:
        """修改配置。指纹相关字段或凭据变更时，旧诊断立即失效。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"数据源配置 {entry_id} 不存在或已归档"
        before = self._fingerprint_inputs(entry)
        for field_name in EDITABLE_FIELDS:
            if field_name in values and str(values.get(field_name) or "").strip():
                entry[field_name] = values[field_name]
        # 认证凭据用键存在性判断：允许显式传空字符串清空凭据（不能被真值判断吞掉）。
        if "认证凭据" in values:
            new_secret = str(values.get("认证凭据") or "")
            with self._secret_lock:
                if new_secret:
                    self._secrets[entry_id] = new_secret
                else:
                    self._secrets.pop(entry_id, None)
        after = self._fingerprint_inputs(entry)
        if before != after:
            self._invalidate_diagnoses(entry_id, reason="认证方式、接入地址或超时/重试配置已变更")
            entry["诊断结论"] = None
            entry["配置指纹"] = None
            if entry.get("status") == "已连接":
                entry["status"] = "待校验"
                entry["pending"] = True
        self._sync_pool_cap(entry)
        view = self.public_view(entry)
        view["已登记认证凭据"] = self.describe_secret(entry_id)
        return view, "数据源配置已更新"

    # ------------------------------------------------------------------ 动作

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"数据源配置 {entry_id} 不存在或已归档"
        if action == DIAGNOSE_ACTION:
            result = self.diagnose(entry_id)
            if result.result == RESULT_SUCCESS:
                return self.public_view(entry), "连接诊断通过，配置标记为已连接"
            if result.result == RESULT_CANCELLED:
                return self.public_view(entry), "连接诊断已取消，配置状态未改变"
            return self.public_view(entry), f"连接诊断未通过（失败阶段：{result.failed_phase or '未知'}）"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据源连接可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return self.public_view(entry), f"数据源配置已{action}"

    # ------------------------------------------------------------------ 诊断

    def diagnose(
        self,
        entry_id: int,
        *,
        waiter_id: str | None = None,
        cancel_event: threading.Event | None = None,
    ) -> DiagnosisResult:
        """对当前配置发起只读诊断；并发同配置调用由引擎单飞合并。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            raise KeyError(f"数据源配置 {entry_id} 不存在或已归档")
        config = self.build_probe_config(entry)
        self._sync_pool_cap(entry)

        def _sink(result: DiagnosisResult) -> None:
            self._persist_result(entry, result)

        result = engine.diagnose(
            config,
            waiter_id=waiter_id,
            cancel_event=cancel_event,
            record_sink=_sink,
        )
        # 取消的等待者不产生落库记录（sink 只在 flight 真实完成时触发），也不改配置状态。
        if result.result != RESULT_CANCELLED:
            self._apply_status(entry, result)
        return result

    def cancel_diagnosis(self, entry_id: int, waiter_id: str) -> bool:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return False
        return engine.cancel(self.build_probe_config(entry), waiter_id)

    def list_diagnoses(self, entry_id: int) -> list[dict[str, Any]]:
        rows = [
            dict(row) for row in store.rows(DIAG_MODULE)
            if int(row.get("数据源配置ID", 0)) == entry_id
        ]
        rows.sort(key=lambda row: int(row.get("记录序号", 0)), reverse=True)
        return rows

    def latest_diagnosis(self, entry_id: int) -> DiagnosisResult | None:
        """ETL 准入与页面共用的取数口径：只认最新一条诊断记录。"""
        rows = self.list_diagnoses(entry_id)
        if not rows:
            return None
        return self._row_to_result(rows[0])

    def current_fingerprint(self, entry_id: int) -> str | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        return engine._fingerprint(self.build_probe_config(entry))  # noqa: SLF001

    def build_probe_config(self, entry: dict[str, Any]) -> ProbeConfig:
        entry_id = int(entry["id"])
        with self._secret_lock:
            secret = self._secrets.get(entry_id, "")
        return ProbeConfig(
            datasource_id=entry_id,
            接入地址=str(entry.get("接入地址") or ""),
            认证方式=str(entry.get("认证方式") or "无认证"),
            查询超时秒数=_to_float(entry.get("查询超时秒数"), _DEFAULT_TIMEOUT),
            重试上限=_to_int(entry.get("重试上限"), _DEFAULT_RETRIES, upper=5),
            连接池大小=_to_int(entry.get("连接池大小"), _DEFAULT_POOL, upper=_POOL_LIMIT),
            字符编码=str(entry.get("字符编码") or ""),
            认证凭据=secret,
        )

    # ------------------------------------------------------------------ 内部

    def _persist_result(self, entry: dict[str, Any], result: DiagnosisResult) -> None:
        payload = result.to_dict()
        # 单调序列号：秒级墙钟在同秒多次诊断时无法区分新旧，准入必须取到真正最新的一条。
        payload["记录序号"] = self._next_seq()
        store.rows(DIAG_MODULE).append(payload)
        # 同一配置只保留最近 20 条，控制故障源反复探测时的内存占用。
        own = [
            row for row in store.rows(DIAG_MODULE)
            if int(row.get("数据源配置ID", 0)) == int(entry["id"])
        ]
        own.sort(key=lambda row: int(row.get("记录序号", 0)))
        for stale_row in own[:-20]:
            store.rows(DIAG_MODULE).remove(stale_row)
        entry["配置指纹"] = result.config_fingerprint

    def _next_seq(self) -> int:
        self._seq = getattr(self, "_seq", 0) + 1
        return self._seq

    def _apply_status(self, entry: dict[str, Any], result: DiagnosisResult) -> None:
        if result.result == RESULT_SUCCESS:
            entry["status"] = "已连接"
            entry["pending"] = False
            entry["abnormal"] = False
        else:
            # 诊断失败不允许再显示“已连接”，避免旧的成功标记遮住本次失败。
            entry["status"] = "待校验"
            entry["pending"] = True
            entry["abnormal"] = True
        entry["诊断结论"] = result.result

    def _invalidate_diagnoses(self, entry_id: int, *, reason: str) -> None:
        for row in store.rows(DIAG_MODULE):
            if int(row.get("数据源配置ID", 0)) == entry_id and not row.get("是否过期"):
                row["是否过期"] = True
                original = str(row.get("结论说明", ""))
                row["结论说明"] = f"已过期：{reason}；原结论：{original}" if original else f"已过期：{reason}"

    def _fingerprint_inputs(self, entry: dict[str, Any]) -> tuple[Any, ...]:
        config = self.build_probe_config(entry)
        return config.fingerprint_parts()

    def _sync_pool_cap(self, entry: dict[str, Any]) -> None:
        pool = _to_int(entry.get("连接池大小"), _DEFAULT_POOL, upper=_POOL_LIMIT)
        engine.ensure_config_cap(int(entry["id"]), pool)

    @staticmethod
    def _row_to_result(row: dict[str, Any]) -> DiagnosisResult:
        return DiagnosisResult(
            diagnosis_id=str(row.get("诊断编号", "")),
            datasource_id=int(row.get("数据源配置ID", 0)),
            config_fingerprint=str(row.get("配置指纹", "")),
            result=str(row.get("诊断结论", "")),
            failed_phase=row.get("失败阶段"),
            steps=[],  # 列表/准入只需要结论头，步骤由明细接口按需读取
            attempts=int(row.get("尝试次数", 0)),
            budget_seconds=float(row.get("预算秒数", 0.0)),
            elapsed_ms=int(row.get("总耗时毫秒", 0)),
            created_at=str(row.get("诊断时间", "")),
            stale=bool(row.get("是否过期", False)),
            message=str(row.get("结论说明", "")),
        )

    @staticmethod
    def public_view(entry: dict[str, Any]) -> dict[str, Any]:
        """对外视图：永远不包含认证凭据，只提示是否已登记凭据。"""
        view = dict(entry)
        view["已登记认证凭据"] = False  # 由路由层结合保险库状态补充更直接，这里先给安全默认
        return view

    def describe_secret(self, entry_id: int) -> bool:
        with self._secret_lock:
            return bool(self._secrets.get(entry_id))


service = DatasourceService()
