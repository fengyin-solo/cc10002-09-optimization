"""ETL 任务业务规则。

输入源准入与数据源诊断共用同一份结论：
- 输入源可以是“已绑定”的数据源配置（写配置编号或 ds://<配置ID>），也可以是历史
  遗留的自由文字；自由文字只展示、不猜测绑定，准入结论明确标注“未绑定”。
- 已绑定输入源只接受“最新一条、配置指纹与当前配置一致、未过期、结论为成功”的诊断；
  诊断缺失、过期、指纹不一致或失败都会拒绝准入，并保留拒绝依据。
- “启动任务”先过准入：本次失败不能用上次成功遮住，每次检查都追加一条准入记录。
"""
from __future__ import annotations

import re
import time
import uuid
from typing import Any

from app.services.datasource import service as datasource_service
from app.services.diagnosis import RESULT_SUCCESS
from app.store import store

MODULE = "etljob"
ADMISSION_MODULE = "etljob_admission"
REQUIRED_FIELDS = ["任务编号", "输入源", "输出目标"]
STATUS_ORDER = ["草稿", "待调度", "运行中", "已完成", "已失败"]
ACTION_RULES = {"提交调度": "待调度", "终止任务": "已失败", "归档结果": "已完成"}
START_ACTION = "启动任务"
NEGATIVE_ACTIONS: list[str] = []

# 输入源之间的分隔符：逗号、中文逗号、分号、换行。
_SPLIT_PATTERN = re.compile(r"[,，;；\n]+")
_DS_URI_PATTERN = re.compile(r"^ds://(\d+)$")

VERDICT_PASS = "准入通过"
VERDICT_REJECT = "拒绝准入"
VERDICT_UNBOUND = "未绑定（仅文字记录）"


class EtljobService:
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
            rows = [row for row in rows if keyword in str(row.get("任务编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        page_rows = [self._with_admission(row) for row in rows[start:start + size]]
        return page_rows, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        row = store.find(MODULE, entry_id)
        if row is None:
            return None
        view = self._with_admission(row)
        view["准入记录"] = self.list_admissions(entry_id)
        return view

    # ------------------------------------------------------------------ 登记

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({
            field: values.get(field)
            for field in ["任务编号", "输入源", "输出目标", "转换脚本", "依赖任务",
                          "调度表达式", "数据分区", "失败重试次数"]
            if field in values
        })
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    # ------------------------------------------------------------------ 动作

    def run_action(
        self, entry_id: int, action: str, *, admission_report: dict[str, Any] | None = None
    ) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"加工任务 {entry_id} 不存在或已归档"
        if action == START_ACTION:
            report = admission_report or self.check_inputs(entry_id)
            if report["允许启动"]:
                entry["status"] = "运行中"
                entry["pending"] = False
                entry["abnormal"] = False
                return self._with_admission(entry), "输入源准入通过，加工任务已启动"
            reasons = "；".join(item["结论"] for item in report["输入源检查"] if item["判定"] != VERDICT_PASS)
            return self._with_admission(entry), f"输入源准入未通过，任务未启动：{reasons}"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于ETL任务可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return self._with_admission(entry), f"加工任务已{action}"

    # ------------------------------------------------------------------ 准入

    def check_inputs(self, entry_id: int) -> dict[str, Any]:
        """对任务的全部输入源逐条核对诊断结论；每次调用都留痕，不沿用旧结论。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            raise KeyError(f"加工任务 {entry_id} 不存在或已归档")
        raw_sources = [part.strip() for part in _SPLIT_PATTERN.split(str(entry.get("输入源") or "")) if part.strip()]
        checks: list[dict[str, Any]] = []
        for raw in raw_sources:
            checks.append(self._check_one(raw))

        allowed = all(item["判定"] != VERDICT_REJECT for item in checks)
        report = {
            "准入编号": uuid.uuid4().hex,
            "加工任务ID": entry_id,
            "检查时间": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "记录序号": self._next_seq(),
            "允许启动": allowed,
            "输入源检查": checks,
            "说明": ("全部已绑定输入源均有当前配置下的成功诊断" if allowed
                    else "存在未通过准入的已绑定输入源，拒绝启动并保留依据"),
        }
        store.rows(ADMISSION_MODULE).append(report)
        # 每个任务保留最近 20 条准入记录，失败记录优先保留。
        own = [row for row in store.rows(ADMISSION_MODULE) if int(row.get("加工任务ID", 0)) == entry_id]
        own.sort(key=lambda row: int(row.get("记录序号", 0)))
        for old in own[:-20]:
            store.rows(ADMISSION_MODULE).remove(old)
        return report

    def _next_seq(self) -> int:
        self._seq = getattr(self, "_seq", 0) + 1
        return self._seq

    def list_admissions(self, entry_id: int) -> list[dict[str, Any]]:
        rows = [
            dict(row) for row in store.rows(ADMISSION_MODULE)
            if int(row.get("加工任务ID", 0)) == entry_id
        ]
        rows.sort(key=lambda row: int(row.get("记录序号", 0)), reverse=True)
        return rows

    # ------------------------------------------------------------------ 内部

    def _check_one(self, raw: str) -> dict[str, Any]:
        bound = self._resolve_binding(raw)
        if bound is None:
            # 兼容历史自由文字：可查看、不猜测绑定，不参与通过/拒绝判定。
            return {
                "输入源文字": raw,
                "绑定": False,
                "数据源配置ID": None,
                "判定": VERDICT_UNBOUND,
                "采用诊断编号": None,
                "结论": "未绑定数据源配置，仅保留文字记录，无法核对连接诊断",
            }

        ds_id, config = bound
        latest = datasource_service.latest_diagnosis(ds_id)
        current_fp = datasource_service.current_fingerprint(ds_id)
        base = {
            "输入源文字": raw,
            "绑定": True,
            "数据源配置ID": ds_id,
            "配置编号": config.get("配置编号"),
        }
        if latest is None:
            return {
                **base,
                "判定": VERDICT_REJECT,
                "采用诊断编号": None,
                "结论": "该数据源配置从未执行连接诊断，拒绝准入",
            }
        if latest.stale:
            return {
                **base,
                "判定": VERDICT_REJECT,
                "采用诊断编号": latest.diagnosis_id,
                "结论": f"诊断 {latest.diagnosis_id} 已因配置变更过期，不能作为准入依据",
            }
        if latest.config_fingerprint != current_fp:
            return {
                **base,
                "判定": VERDICT_REJECT,
                "采用诊断编号": latest.diagnosis_id,
                "结论": (
                    f"诊断 {latest.diagnosis_id} 的配置指纹 {latest.config_fingerprint} "
                    f"与当前配置指纹 {current_fp} 不一致，需重新诊断"
                ),
            }
        if latest.result != RESULT_SUCCESS:
            return {
                **base,
                "判定": VERDICT_REJECT,
                "采用诊断编号": latest.diagnosis_id,
                "结论": (
                    f"诊断 {latest.diagnosis_id} 结论为「{latest.result}」"
                    f"（失败阶段：{latest.failed_phase or '未知'}）：{latest.message}"
                ),
            }
        return {
            **base,
            "判定": VERDICT_PASS,
            "采用诊断编号": latest.diagnosis_id,
            "结论": (
                f"采用诊断 {latest.diagnosis_id}（{latest.created_at}），"
                f"指纹 {latest.config_fingerprint} 与当前配置一致，只读探针成功"
            ),
        }

    @staticmethod
    def _resolve_binding(raw: str) -> tuple[int, dict[str, Any]] | None:
        match = _DS_URI_PATTERN.match(raw)
        if match:
            ds_id = int(match.group(1))
            config = store.find("datasource", ds_id)
            return (ds_id, config) if config is not None else None
        # 按配置编号精确匹配；匹配不到就按未绑定文字处理，绝不猜测。
        for config in store.rows("datasource"):
            if str(config.get("配置编号") or "").strip() == raw:
                return int(config["id"]), config
        return None

    def _with_admission(self, row: dict[str, Any]) -> dict[str, Any]:
        view = dict(row)
        records = self.list_admissions(int(row["id"]))
        view["最近准入"] = records[0] if records else None
        return view


service = EtljobService()
