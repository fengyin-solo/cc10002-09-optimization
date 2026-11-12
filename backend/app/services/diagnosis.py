"""连接诊断引擎：把“建立连接”从改标记升级为可核对的只读探测。

设计要点（数据源页面、ETL 输入源准入共用同一份结论）：

1. 分阶段探测：地址格式 → 握手 → 认证 → 读取探针，结论逐段给出成败与耗时，
   失败时能看清卡在哪一步；全程只读，只发固定的 GET 探针，不执行用户填入的任何语句。
2. 共享截止预算：一次诊断的所有步骤和重试共用同一个 deadline，每一步拿到的只是
   “剩余预算”，不会在每个步骤重新获得整段查询超时。
3. 单飞合并：同一数据源、同一配置指纹的并发诊断只发出一次真实探测，其它调用作为
   等待者挂在同一个 flight 上，结论共享。
4. 连接名额与公平性：真实探测由固定大小的 FIFO 工作线程池消费（先到先得），
   数据源自己的“连接池大小”作为该配置同时在探的名额上限，拿不到本配置名额时
   重新排队，让其它数据源先获得全局名额；等待者可单独取消，取消一个等待者不会
   取消别人仍在等待的探测；名额在探测结束（含超时）后立刻释放。
5. 取消语义：等待者取消只影响它自己，已取消的结论在取消时刻固定，之后探测成功的
   回调不能再把它改成成功。
6. 脱敏：认证凭据（口令/令牌）只用于计算配置指纹和拼装探测请求，不写诊断记录、
   不进错误信息；对外输出统一经过 redact 处理。
"""
from __future__ import annotations

import http.client
import socket
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

PHASE_FORMAT = "地址格式"
PHASE_HANDSHAKE = "握手"
PHASE_AUTH = "认证"
PHASE_READ = "读取探针"
PHASES = (PHASE_FORMAT, PHASE_HANDSHAKE, PHASE_AUTH, PHASE_READ)

ATTEMPT_OK = "ok"
ATTEMPT_AUTH_REJECTED = "auth_rejected"
ATTEMPT_TRANSIENT = "transient"
ATTEMPT_BAD_URL = "bad_url"

RESULT_SUCCESS = "成功"
RESULT_FAILED = "失败"
RESULT_CANCELLED = "已取消"

_SECRET_HINTS = ("password", "passwd", "secret", "token", "authorization", "credential",
                 "口令", "密钥", "令牌", "密码", "凭据")

# 同一 flight 在拿不到本配置名额时最多重新入队的次数，超过后短暂阻塞等待，
# 避免单个数据源的队头长期挡住其它数据源（防队头饥饿）。
MAX_REQUEUE = 8


def redact_text(text: str) -> str:
    """把可能携带认证内容的底层错误文本压成不含凭据的概括。"""
    lowered = str(text).lower()
    if any(hint in lowered for hint in _SECRET_HINTS):
        return "底层错误信息包含认证字段，已按安全要求隐去"
    if "://" in text and "@" in text.split("://", 1)[1]:
        head, rest = text.split("://", 1)
        return f"{head}://***@{rest.split('@', 1)[1]}"
    return text


def now_ts() -> float:
    return time.monotonic()


def wall_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


# ---------------------------------------------------------------------------
# 数据结构
# ---------------------------------------------------------------------------

@dataclass
class ProbeConfig:
    """一次探测依赖的、参与配置指纹的全部输入。"""

    datasource_id: int
    接入地址: str
    认证方式: str
    查询超时秒数: float
    重试上限: int
    连接池大小: int
    字符编码: str = ""
    认证凭据: str = ""  # 仅运行期使用，绝不落库、不外显

    def fingerprint_parts(self) -> tuple[Any, ...]:
        # 认证凭据纳入指纹：凭据变更后旧诊断立刻失效；指纹是单向散列，不可逆。
        return (
            self.接入地址.strip(),
            self.认证方式.strip(),
            self.认证凭据,
            round(float(self.查询超时秒数), 3),
            int(self.重试上限),
        )


@dataclass
class StepRecord:
    phase: str
    status: str  # 成功 / 失败 / 跳过 / 取消
    detail: str = ""
    elapsed_ms: int = 0
    attempt: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "阶段": self.phase,
            "状态": self.status,
            "说明": self.detail,
            "耗时毫秒": self.elapsed_ms,
            "尝试序号": self.attempt,
        }


@dataclass
class DiagnosisResult:
    """一次诊断的完整结论，落库后成为 ETL 输入源准入的唯一依据。"""

    diagnosis_id: str
    datasource_id: int
    config_fingerprint: str
    result: str
    failed_phase: str | None
    steps: list[StepRecord] = field(default_factory=list)
    attempts: int = 0
    budget_seconds: float = 0.0
    elapsed_ms: int = 0
    created_at: str = field(default_factory=wall_ts)
    read_only: bool = True
    stale: bool = False  # 配置变更后由服务层翻成 True，准入时直接拒绝
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "诊断编号": self.diagnosis_id,
            "数据源配置ID": self.datasource_id,
            "配置指纹": self.config_fingerprint,
            "诊断结论": self.result,
            "失败阶段": self.failed_phase,
            "步骤": [step.to_dict() for step in self.steps],
            "尝试次数": self.attempts,
            "预算秒数": round(self.budget_seconds, 3),
            "总耗时毫秒": self.elapsed_ms,
            "诊断时间": self.created_at,
            "只读探测": self.read_only,
            "是否过期": self.stale,
            "结论说明": self.message,
        }


class DiagnosisCancelled(Exception):
    """等待者在拿到探测结论前主动取消。"""


# ---------------------------------------------------------------------------
# 真实探测驱动：标准库 http.client，只读 GET，不执行任何用户语句
# ---------------------------------------------------------------------------

class HttpProbeDriver:
    """一次 HTTP 尝试，区分握手 / 认证 / 读取阶段。

    每次调用使用调用方给的剩余预算作为 socket 超时，驱动自身不持有超时配置，
    共享截止预算完全由上层引擎掌控。
    """

    def attempt(
        self,
        config: ProbeConfig,
        parsed: tuple[str, str, int, str],
        timeout: float,
    ) -> tuple[str, str | None, str]:
        scheme, host, port, path = parsed
        if scheme == "https":
            conn: http.client.HTTPConnection = http.client.HTTPSConnection(
                host, port, timeout=max(timeout, 0.05))
        else:
            conn = http.client.HTTPConnection(host, port, timeout=max(timeout, 0.05))
        try:
            headers = self._build_headers(config)
            try:
                conn.request("GET", path or "/", headers=headers)
            except TimeoutError:
                return ATTEMPT_TRANSIENT, PHASE_HANDSHAKE, "连接握手在剩余预算内未完成（超时）"
            except socket.gaierror as exc:
                return ATTEMPT_TRANSIENT, PHASE_HANDSHAKE, f"地址解析失败：{redact_text(str(exc))}"
            except (ConnectionRefusedError, socket.timeout) as exc:
                return ATTEMPT_TRANSIENT, PHASE_HANDSHAKE, f"握手被拒绝或超时：{redact_text(str(exc))}"
            except OSError as exc:
                return ATTEMPT_TRANSIENT, PHASE_HANDSHAKE, f"握手阶段网络异常：{redact_text(str(exc))}"

            try:
                response = conn.getresponse()
            except TimeoutError:
                return ATTEMPT_TRANSIENT, PHASE_READ, "等待响应在剩余预算内未完成（超时）"
            except OSError as exc:
                return ATTEMPT_TRANSIENT, PHASE_READ, f"读取响应头失败：{redact_text(str(exc))}"

            if response.status in (401, 403):
                # 认证被明确拒绝：不重试、不回显任何请求头内容。
                return ATTEMPT_AUTH_REJECTED, PHASE_AUTH, f"服务端以 HTTP {response.status} 拒绝认证"
            if response.status >= 500:
                return ATTEMPT_TRANSIENT, PHASE_AUTH, f"服务端暂时不可用（HTTP {response.status}）"

            # 读取探针：仅丢弃固定探针路径的响应体，不解析、不执行任何用户内容。
            try:
                body = response.read(4096)
            except TimeoutError:
                return ATTEMPT_TRANSIENT, PHASE_READ, "读取探针数据在剩余预算内未完成（超时）"
            except OSError as exc:
                return ATTEMPT_TRANSIENT, PHASE_READ, f"读取探针数据失败：{redact_text(str(exc))}"
            return ATTEMPT_OK, None, f"只读探针成功，HTTP {response.status}，读取 {len(body)} 字节"
        finally:
            conn.close()  # 无论成败都归还底层 socket

    @staticmethod
    def _build_headers(config: ProbeConfig) -> dict[str, str]:
        scheme = (config.认证方式 or "").strip()
        secret = config.认证凭据
        lowered = scheme.lower()
        if not scheme or scheme == "无认证" or lowered == "none":
            return {"User-Agent": "connection-diagnosis/1.0", "Accept": "*/*"}
        if "basic" in lowered:
            import base64

            encoded = base64.b64encode(secret.encode("utf-8")).decode("ascii")
            return {"Authorization": f"Basic {encoded}", "User-Agent": "connection-diagnosis/1.0"}
        # bearer / token / 密钥 / 令牌 等统一按 Bearer 处理；凭据只在此处进入请求头。
        token = secret if secret else scheme
        return {"Authorization": f"Bearer {token}", "User-Agent": "connection-diagnosis/1.0"}


# ---------------------------------------------------------------------------
# Flight：同一份配置的并发诊断合并到同一个飞行单元
# ---------------------------------------------------------------------------

class _Waiter:
    __slots__ = ("waiter_id", "event", "cancelled", "result")

    def __init__(self, waiter_id: str) -> None:
        self.waiter_id = waiter_id
        self.event = threading.Event()
        self.cancelled = False
        self.result: DiagnosisResult | None = None


class _Flight:
    def __init__(
        self,
        key: str,
        config: ProbeConfig,
        fingerprint: str,
        sink: Callable[[DiagnosisResult], None] | None,
    ) -> None:
        self.key = key
        self.config = config
        self.fingerprint = fingerprint
        self.waiters: list[_Waiter] = []
        self.result: DiagnosisResult | None = None
        self.done = threading.Event()
        self.sink = sink
        self.requeue_count = 0


# ---------------------------------------------------------------------------
# 诊断引擎
# ---------------------------------------------------------------------------

class DiagnosisEngine:
    """进程内单例：预算、单飞合并、公平排队、名额释放与取消隔离。"""

    def __init__(
        self,
        driver: HttpProbeDriver | None = None,
        *,
        global_slots: int = 4,
        max_pool_size: int = 8,
    ) -> None:
        self._driver = driver or HttpProbeDriver()
        self._lock = threading.RLock()
        self._cond = threading.Condition(self._lock)
        self._flights: dict[str, _Flight] = {}
        self._queue: deque[_Flight] = deque()
        self._global_slots = max(1, int(global_slots))
        self._max_pool_size = max_pool_size
        # 每个数据源配置的在探计数与名额上限（来自“连接池大小”）。
        self._config_active: dict[int, int] = {}
        self._config_cap: dict[int, int] = {}
        self._seq = 0
        self._workers = [
            threading.Thread(target=self._worker_loop, name=f"probe-worker-{i}", daemon=True)
            for i in range(self._global_slots)
        ]
        for worker in self._workers:
            worker.start()

    # -- 对外主入口 ---------------------------------------------------------

    def diagnose(
        self,
        config: ProbeConfig,
        *,
        waiter_id: str | None = None,
        cancel_event: threading.Event | None = None,
        record_sink: Callable[[DiagnosisResult], None] | None = None,
    ) -> DiagnosisResult:
        """对当前配置执行只读诊断；并发同配置调用合并到同一次真实探测。

        cancel_event：调用方专用取消信号，只影响这一个等待者，不影响同 flight 的其他人。
        record_sink：flight 真正完成探测时由引擎回调一次（落库），与调用方是否取消无关。
        """
        fingerprint = self._fingerprint(config)
        key = f"ds-{config.datasource_id}:{fingerprint}"
        waiter = _Waiter(waiter_id or uuid.uuid4().hex)

        with self._lock:
            flight = self._flights.get(key)
            if flight is None:
                flight = _Flight(key, config, fingerprint, record_sink)
                self._flights[key] = flight
                self._queue.append(flight)  # 入队顺序即全局 FIFO 公平顺序
                self._cond.notify()
            flight.waiters.append(waiter)

        watcher: threading.Thread | None = None
        if cancel_event is not None:
            def _watch() -> None:
                cancel_event.wait()
                if cancel_event.is_set():
                    self._cancel_locked(key, waiter.waiter_id)

            watcher = threading.Thread(target=_watch, name="diagnosis-cancel-watch", daemon=True)
            watcher.start()

        try:
            waiter.event.wait(timeout=self._hard_wait(config))
        finally:
            # watcher 是 daemon 线程，cancel_event 触发或进程结束时自然退出，无需显式回收。
            pass
        result = waiter.result or self._cancelled_result(config, fingerprint)
        return result

    def cancel(self, config: ProbeConfig, waiter_id: str) -> bool:
        """取消某个等待者；不影响同 flight 的其他人，也不中断已在执行的探测。"""
        fingerprint = self._fingerprint(config)
        return self._cancel_locked(f"ds-{config.datasource_id}:{fingerprint}", waiter_id)

    def ensure_config_cap(self, datasource_id: int, pool_size: int) -> None:
        """按数据源配置的“连接池大小”刷新该配置自己的在探名额上限。"""
        sized = min(max(int(pool_size), 1), self._max_pool_size)
        with self._lock:
            self._config_cap[datasource_id] = sized

    # -- 取消实现 ------------------------------------------------------------

    def _cancel_locked(self, key: str, waiter_id: str) -> bool:
        with self._lock:
            flight = self._flights.get(key)
            if flight is None:
                return False
            target = next((w for w in flight.waiters if w.waiter_id == waiter_id), None)
            if target is None or target.cancelled:
                return False
            target.cancelled = True
            # 结论在取消时刻固定；之后 flight 成功也不能覆盖它。
            target.result = self._cancelled_result(flight.config, flight.fingerprint)
            flight.waiters.remove(target)
            target.event.set()
            return True

    # -- 工作线程：FIFO 出队 + 数据源名额；全局名额即工作线程数量 -----------

    def _next_queued(self) -> _Flight:
        """阻塞等待并取出队首 flight；队列为空时在条件变量上挂起，不空转。"""
        with self._cond:
            while not self._queue:
                self._cond.wait()
            return self._queue.popleft()

    def _requeue(self, flight: _Flight) -> None:
        with self._cond:
            self._queue.append(flight)  # 排到队尾，其它数据源先拿全局名额
            self._cond.notify()

    def _worker_loop(self) -> None:
        while True:
            flight = self._next_queued()
            ds_id = flight.config.datasource_id
            try:
                with self._lock:
                    if not flight.waiters and flight.result is None:
                        # 等待者全部取消且尚未探测：跳过真实探测，不向故障源浪费连接。
                        self._discard_flight(flight)
                        continue
                    cap = self._config_cap.get(ds_id, 1)
                    active = self._config_active.get(ds_id, 0)
                    if active >= cap:
                        if flight.requeue_count < MAX_REQUEUE:
                            flight.requeue_count += 1
                            self._requeue(flight)
                            continue
                        # 重排次数过多则短等本配置名额，防止队头饥饿无限循环。
                        slot_ready = self._wait_config_slot(ds_id, cap, 0.25)
                        if not slot_ready:
                            self._requeue(flight)
                            continue
                    self._config_active[ds_id] = active + 1
                try:
                    result = self._run_pipeline(flight.config, flight.fingerprint)
                finally:
                    with self._lock:
                        # 超时/失败同样释放名额，较迟的成功回调也不会再占用。
                        self._config_active[ds_id] = max(
                            self._config_active.get(ds_id, 1) - 1, 0)
                with self._lock:
                    flight.result = result
                    self._finish_flight(flight, result)
            except Exception as exc:  # 防御：worker 线程不能死
                with self._lock:
                    fallback = DiagnosisResult(
                        diagnosis_id=uuid.uuid4().hex,
                        datasource_id=ds_id,
                        config_fingerprint=flight.fingerprint,
                        result=RESULT_FAILED,
                        failed_phase=PHASE_READ,
                        message=f"诊断引擎内部异常：{redact_text(str(exc))}",
                    )
                    flight.result = fallback
                    self._finish_flight(flight, fallback)

    def _wait_config_slot(self, ds_id: int, cap: int, seconds: float) -> bool:
        deadline = now_ts() + seconds
        while now_ts() < deadline:
            with self._lock:
                if self._config_active.get(ds_id, 0) < cap:
                    return True
            time.sleep(0.02)
        return False

    def _discard_flight(self, flight: _Flight) -> None:
        flight.done.set()
        self._flights.pop(flight.key, None)

    def _finish_flight(self, flight: _Flight, result: DiagnosisResult) -> None:
        for waiter in flight.waiters:
            if not waiter.cancelled and waiter.result is None:
                waiter.result = result  # 只写一次；已取消者的结论不被覆盖
                waiter.event.set()
        flight.waiters = [w for w in flight.waiters if w.cancelled]
        flight.done.set()
        self._flights.pop(flight.key, None)
        if flight.sink is not None:
            try:
                flight.sink(result)  # 落库与等待者解耦：flight 完成即留痕
            except Exception:
                pass

    # -- 分阶段流水线 + 共享截止预算 ----------------------------------------

    def _run_pipeline(self, config: ProbeConfig, fingerprint: str) -> DiagnosisResult:
        start = now_ts()
        timeout = max(float(config.查询超时秒数), 0.05)
        retries = max(int(config.重试上限), 0)
        # 关键：预算 = 单次超时 × (重试上限 + 1)，所有步骤与重试共享这一个 deadline。
        budget = timeout * (retries + 1)
        deadline = start + budget
        steps: list[StepRecord] = []
        diagnosis_id = uuid.uuid4().hex

        # 阶段 1：地址格式（本地校验，不占网络预算）。
        parsed = self._parse_address(config.接入地址)
        if parsed is None:
            steps.append(StepRecord(PHASE_FORMAT, "失败",
                                    f"接入地址不是合法的 http(s) URL：{redact_text(config.接入地址)}"))
            for phase in (PHASE_HANDSHAKE, PHASE_AUTH, PHASE_READ):
                steps.append(StepRecord(phase, "跳过", "地址格式未通过，后续阶段不执行"))
            return self._build_result(diagnosis_id, config, fingerprint, RESULT_FAILED,
                                      PHASE_FORMAT, steps, 0, budget, start,
                                      "地址格式校验失败，未发起任何网络探测")
        steps.append(StepRecord(PHASE_FORMAT, "成功",
                                f"已识别 {parsed[0]} 端点 {parsed[1]}:{parsed[2]}{parsed[3] or '/'}"))

        attempts = 0
        last_failed_phase: str | None = None
        last_detail = ""
        while True:
            attempts += 1
            remaining = deadline - now_ts()
            if remaining <= 0:
                detail = "共享截止预算已耗尽，不再发起新的尝试"
                steps.append(StepRecord(PHASE_HANDSHAKE, "失败", detail, attempt=attempts))
                last_failed_phase = last_failed_phase or PHASE_HANDSHAKE
                last_detail = detail
                break

            attempt_start = now_ts()
            verdict, failed_phase, detail = self._driver.attempt(config, parsed, remaining)
            elapsed = max(int((now_ts() - attempt_start) * 1000), 1)

            if verdict == ATTEMPT_OK:
                steps.append(StepRecord(PHASE_HANDSHAKE, "成功", "TCP/TLS 握手完成",
                                        elapsed_ms=elapsed, attempt=attempts))
                steps.append(StepRecord(PHASE_AUTH, "成功", "服务端接受本次认证身份",
                                        attempt=attempts))
                steps.append(StepRecord(PHASE_READ, "成功", detail, attempt=attempts))
                return self._build_result(diagnosis_id, config, fingerprint, RESULT_SUCCESS,
                                          None, steps, attempts, budget, start,
                                          "只读探针全部阶段通过")

            self._append_failure_steps(steps, failed_phase, detail, elapsed, attempts)
            last_failed_phase = failed_phase
            last_detail = detail

            if verdict in (ATTEMPT_AUTH_REJECTED, ATTEMPT_BAD_URL):
                break  # 认证被明确拒绝 / 地址问题：重试无意义，不浪费名额与预算
            if attempts > retries:
                last_detail = f"{detail}；重试上限 {retries} 次已用完"
                break
            if deadline - now_ts() <= 0:
                last_detail = f"{detail}；共享截止预算已耗尽"
                break

        return self._build_result(diagnosis_id, config, fingerprint, RESULT_FAILED,
                                  last_failed_phase, steps, attempts, budget, start,
                                  last_detail or "探测失败")

    @staticmethod
    def _append_failure_steps(
        steps: list[StepRecord], failed_phase: str | None, detail: str,
        elapsed: int, attempt: int,
    ) -> None:
        order = {phase: idx for idx, phase in enumerate(PHASES)}
        failed_idx = order.get(failed_phase or "", len(PHASES))
        for phase in (PHASE_HANDSHAKE, PHASE_AUTH, PHASE_READ):
            idx = order[phase]
            if idx < failed_idx:
                steps.append(StepRecord(phase, "成功", f"{phase}阶段已通过",
                                        elapsed_ms=elapsed if phase == PHASE_HANDSHAKE else 0,
                                        attempt=attempt))
            elif idx == failed_idx:
                steps.append(StepRecord(phase, "失败", detail,
                                        elapsed_ms=elapsed, attempt=attempt))
            else:
                steps.append(StepRecord(phase, "跳过", f"前置阶段「{failed_phase}」未通过",
                                        attempt=attempt))

    # -- 工具 ----------------------------------------------------------------

    def _hard_wait(self, config: ProbeConfig) -> float:
        # 等待者最长等待 = 预算 + 排队冗余，避免队列拥塞时无限挂起。
        return max(float(config.查询超时秒数), 0.05) * (max(int(config.重试上限), 0) + 1) + 15.0

    @staticmethod
    def _parse_address(address: str) -> tuple[str, str, int, str] | None:
        try:
            parsed = urlparse(str(address).strip())
        except ValueError:
            return None
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            return None
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"
        return parsed.scheme, parsed.hostname, port, path

    @staticmethod
    def _fingerprint(config: ProbeConfig) -> str:
        import hashlib

        joined = "\x1f".join(str(part) for part in config.fingerprint_parts())
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def _cancelled_result(config: ProbeConfig, fingerprint: str) -> DiagnosisResult:
        return DiagnosisResult(
            diagnosis_id=uuid.uuid4().hex,
            datasource_id=config.datasource_id,
            config_fingerprint=fingerprint,
            result=RESULT_CANCELLED,
            failed_phase=None,
            steps=[StepRecord(phase, "取消", "等待者已取消，未采用该探测结论") for phase in PHASES],
            message="诊断在等待结果期间被取消，未形成可用结论",
        )

    @staticmethod
    def _build_result(
        diagnosis_id: str, config: ProbeConfig, fingerprint: str, result: str,
        failed_phase: str | None, steps: list[StepRecord], attempts: int,
        budget: float, start: float, message: str,
    ) -> DiagnosisResult:
        return DiagnosisResult(
            diagnosis_id=diagnosis_id,
            datasource_id=config.datasource_id,
            config_fingerprint=fingerprint,
            result=result,
            failed_phase=failed_phase,
            steps=steps,
            attempts=attempts,
            budget_seconds=budget,
            elapsed_ms=max(int((now_ts() - start) * 1000), 0),
            message=message,
        )


# 模块级单例：路由、数据源服务、ETL 准入共用同一个引擎与同一份排队语义。
engine = DiagnosisEngine()
