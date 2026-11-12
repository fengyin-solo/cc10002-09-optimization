"""诊断引擎单元测试：单飞合并、取消隔离、共享预算、名额释放与公平性。

用可控的假驱动替代真实网络，避免时序抖动；真实 HTTP 链路放在 test_integration.py。
"""
from __future__ import annotations

import threading
import time
import unittest

from app.services.diagnosis import (
    ATTEMPT_AUTH_REJECTED,
    ATTEMPT_OK,
    ATTEMPT_TRANSIENT,
    PHASE_AUTH,
    PHASE_FORMAT,
    PHASE_HANDSHAKE,
    PHASE_READ,
    RESULT_CANCELLED,
    RESULT_FAILED,
    RESULT_SUCCESS,
    DiagnosisEngine,
    ProbeConfig,
    redact_text,
)


def make_config(
    datasource_id: int = 1,
    *,
    address: str = "http://127.0.0.1:18080/healthy",
    timeout: float = 1.0,
    retries: int = 1,
    pool: int = 2,
    secret: str = "",
) -> ProbeConfig:
    return ProbeConfig(
        datasource_id=datasource_id,
        接入地址=address,
        认证方式="Bearer令牌" if secret else "无认证",
        查询超时秒数=timeout,
        重试上限=retries,
        连接池大小=pool,
        认证凭据=secret,
    )


class ScriptedDriver:
    """按数据源 ID 分派的假驱动：记录真实探测次数与每次拿到的剩余预算。

    每个数据源可配置独立脚本、延迟与门闩（entered/release），用于精确编排并发时序。
    """

    def __init__(self) -> None:
        self._scripts: dict[int, list[tuple[str, str | None, str]]] = {}
        self._delays: dict[int, float] = {}
        self.gates: dict[int, dict[str, threading.Event]] = {}
        self.calls: dict[int, int] = {}
        self.timeouts: dict[int, list[float]] = {}
        self.lock = threading.Lock()

    def configure(self, datasource_id: int, script, *, delay: float = 0.0) -> None:
        self._scripts[datasource_id] = list(script)
        self._delays[datasource_id] = delay
        self.gates[datasource_id] = {"entered": threading.Event(), "release": threading.Event()}
        self.calls[datasource_id] = 0
        self.timeouts[datasource_id] = []

    def attempt(self, config, parsed, timeout):
        ds_id = config.datasource_id
        with self.lock:
            idx = self.calls.get(ds_id, 0)
            self.calls[ds_id] = idx + 1
            self.timeouts.setdefault(ds_id, []).append(timeout)
            gate = self.gates[ds_id]["entered"]
            gate.set()
        if self._delays.get(ds_id):
            self.gates[ds_id]["release"].wait(timeout=self._delays[ds_id])
        script = self._scripts[ds_id]
        return script[min(idx, len(script) - 1)]


class EngineUnitTest(unittest.TestCase):
    def test_bad_url_fails_at_format_phase_without_network(self) -> None:
        driver = ScriptedDriver()
        driver.configure(1, [(ATTEMPT_OK, None, "ok")])
        engine = DiagnosisEngine(driver, global_slots=2)
        result = engine.diagnose(make_config(address="not-a-url"))
        self.assertEqual(result.result, RESULT_FAILED)
        self.assertEqual(result.failed_phase, PHASE_FORMAT)
        self.assertEqual(driver.calls.get(1, 0), 0)  # 地址格式失败绝不发起网络探测
        statuses = {step.phase: step.status for step in result.steps}
        self.assertEqual(statuses[PHASE_HANDSHAKE], "跳过")
        self.assertEqual(statuses[PHASE_AUTH], "跳过")
        self.assertEqual(statuses[PHASE_READ], "跳过")

    def test_auth_rejection_is_terminal_and_not_retried(self) -> None:
        driver = ScriptedDriver()
        driver.configure(1, [(ATTEMPT_AUTH_REJECTED, PHASE_AUTH, "服务端以 HTTP 401 拒绝认证")])
        engine = DiagnosisEngine(driver, global_slots=2)
        result = engine.diagnose(make_config(retries=3, secret="bad-secret"))
        self.assertEqual(result.result, RESULT_FAILED)
        self.assertEqual(result.failed_phase, PHASE_AUTH)
        self.assertEqual(driver.calls[1], 1)  # 认证明确拒绝不重试
        joined = " ".join(step.detail for step in result.steps)
        self.assertNotIn("bad-secret", joined)

    def test_shared_deadline_budget_not_refreshed_per_step(self) -> None:
        # timeout=0.3、retries=2，预算只有 0.9 秒；每次拿到的 socket 超时必须是剩余预算。
        driver = ScriptedDriver()
        driver.configure(1, [
            (ATTEMPT_TRANSIENT, PHASE_HANDSHAKE, "握手超时"),
            (ATTEMPT_TRANSIENT, PHASE_HANDSHAKE, "握手超时"),
            (ATTEMPT_OK, None, "ok"),
        ], delay=0.12)
        engine = DiagnosisEngine(driver, global_slots=2)
        result = engine.diagnose(make_config(timeout=0.3, retries=2))
        self.assertEqual(result.result, RESULT_SUCCESS)
        self.assertEqual(result.attempts, 3)
        self.assertAlmostEqual(result.budget_seconds, 0.9, places=2)
        self.assertEqual(len(driver.timeouts[1]), 3)
        for earlier, later in zip(driver.timeouts[1], driver.timeouts[1][1:]):
            self.assertGreater(earlier, later)  # 共享 deadline 的直接证据
        self.assertLess(driver.timeouts[1][-1], 0.7)

    def test_budget_exhaustion_stops_new_attempts(self) -> None:
        driver = ScriptedDriver()
        # 预算 = 0.15 × 6 = 0.9 秒；每次尝试耗时 0.2 秒，预算会先于重试上限耗尽。
        driver.configure(1, [(ATTEMPT_TRANSIENT, PHASE_READ, "读取超时")], delay=0.2)
        engine = DiagnosisEngine(driver, global_slots=2)
        result = engine.diagnose(make_config(timeout=0.15, retries=5))
        self.assertEqual(result.result, RESULT_FAILED)
        self.assertLess(result.attempts, 6)  # 重试上限允许 6 次，实际被预算截断
        self.assertLessEqual(result.elapsed_ms, 1200)
        self.assertIn("预算", result.message)

    def test_concurrent_same_config_emits_single_probe(self) -> None:
        driver = ScriptedDriver()
        driver.configure(1, [(ATTEMPT_OK, None, "ok")], delay=0.25)
        engine = DiagnosisEngine(driver, global_slots=2)
        config = make_config()
        results: list = []
        barrier = threading.Barrier(4)

        def worker() -> None:
            barrier.wait()
            results.append(engine.diagnose(config))

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        self.assertEqual(driver.calls[1], 1)  # 四个并发调用只发出一次真实探测
        self.assertEqual(len(results), 4)
        self.assertTrue(all(item.result == RESULT_SUCCESS for item in results))
        self.assertEqual(len({item.diagnosis_id for item in results}), 1)

    def test_cancel_one_waiter_keeps_shared_probe_for_others(self) -> None:
        driver = ScriptedDriver()
        driver.configure(1, [(ATTEMPT_OK, None, "ok")], delay=0.45)
        engine = DiagnosisEngine(driver, global_slots=2)
        config = make_config()
        box: dict[str, object] = {}

        def keep_waiting() -> None:
            box["other"] = engine.diagnose(config, waiter_id="w-other")

        other_thread = threading.Thread(target=keep_waiting)
        other_thread.start()
        self.assertTrue(driver.gates[1]["entered"].wait(timeout=2))

        cancel_event = threading.Event()

        def cancel_path() -> None:
            box["cancelled"] = engine.diagnose(
                config, waiter_id="w-cancel", cancel_event=cancel_event)

        cancel_thread = threading.Thread(target=cancel_path)
        cancel_thread.start()
        time.sleep(0.08)
        cancel_event.set()  # 只取消一个等待者
        cancel_thread.join(timeout=2)
        self.assertEqual(box["cancelled"].result, RESULT_CANCELLED)  # type: ignore[union-attr]

        driver.gates[1]["release"].set()
        other_thread.join(timeout=2)
        self.assertEqual(box["other"].result, RESULT_SUCCESS)  # type: ignore[union-attr]
        self.assertEqual(driver.calls[1], 1)  # 共享探测没有被连带取消

    def test_all_waiters_cancelled_before_pickup_skips_probe(self) -> None:
        driver = ScriptedDriver()
        # ds9 先占住唯一工作线程；ds2 排队，等待者在出队前全部取消。
        driver.configure(9, [(ATTEMPT_OK, None, "ok")], delay=0.4)
        driver.configure(2, [(ATTEMPT_OK, None, "ok")], delay=0.05)
        engine = DiagnosisEngine(driver, global_slots=1)
        engine.ensure_config_cap(9, 1)
        engine.ensure_config_cap(2, 1)

        holder = threading.Thread(
            target=lambda: engine.diagnose(make_config(9, pool=1), waiter_id="holder"))
        holder.start()
        self.assertTrue(driver.gates[9]["entered"].wait(timeout=2))

        event = threading.Event()
        box: list = []
        queued = threading.Thread(
            target=lambda: box.append(
                engine.diagnose(make_config(2, pool=1), waiter_id="q1", cancel_event=event)))
        queued.start()
        time.sleep(0.1)
        event.set()  # 出队前取消
        queued.join(timeout=2)
        self.assertEqual(box[0].result, RESULT_CANCELLED)

        driver.gates[9]["release"].set()
        holder.join(timeout=2)
        time.sleep(0.2)  # 排队 flight 出队时应发现无等待者，直接跳过
        self.assertEqual(driver.calls.get(2, 0), 0)  # 没有向故障数据源浪费连接

    def test_late_success_cannot_overwrite_cancelled_result(self) -> None:
        driver = ScriptedDriver()
        driver.configure(1, [(ATTEMPT_OK, None, "ok")], delay=0.4)
        engine = DiagnosisEngine(driver, global_slots=1)
        config = make_config()
        event = threading.Event()
        box: list = []

        def caller() -> None:
            box.append(engine.diagnose(config, waiter_id="lonely", cancel_event=event))

        thread = threading.Thread(target=caller)
        thread.start()
        self.assertTrue(driver.gates[1]["entered"].wait(timeout=2))
        event.set()
        thread.join(timeout=2)
        self.assertEqual(box[0].result, RESULT_CANCELLED)
        driver.gates[1]["release"].set()
        time.sleep(0.35)  # 等待“较迟到达的成功回调”
        self.assertEqual(box[0].result, RESULT_CANCELLED)  # 结论在取消时刻已固定

    def test_slot_released_after_timeout(self) -> None:
        driver = ScriptedDriver()
        driver.configure(7, [
            (ATTEMPT_TRANSIENT, PHASE_HANDSHAKE, "握手超时"),
            (ATTEMPT_OK, None, "ok"),
        ], delay=0.05)
        engine = DiagnosisEngine(driver, global_slots=2)
        config = make_config(7, pool=1, timeout=0.2, retries=0)
        first = engine.diagnose(config)
        self.assertEqual(first.result, RESULT_FAILED)
        second = engine.diagnose(config)  # 名额已释放，可以立刻再探
        self.assertEqual(second.result, RESULT_SUCCESS)
        self.assertEqual(engine._config_active[7], 0)  # type: ignore[attr-defined]

    def test_fairness_other_datasource_not_starved(self) -> None:
        driver = ScriptedDriver()
        driver.configure(1, [(ATTEMPT_OK, None, "ok")], delay=0.08)
        driver.configure(2, [(ATTEMPT_OK, None, "ok")], delay=0.05)
        engine = DiagnosisEngine(driver, global_slots=2)
        engine.ensure_config_cap(1, 1)
        engine.ensure_config_cap(2, 1)
        ds1 = make_config(1, pool=1, timeout=0.2, retries=0)
        ds2 = make_config(2, pool=1, timeout=0.2, retries=0)
        done: list = []

        def spam_ds1() -> None:
            for _ in range(4):
                done.append(engine.diagnose(ds1).result)

        spam_thread = threading.Thread(target=spam_ds1)
        spam_thread.start()
        time.sleep(0.05)
        start = time.monotonic()
        ds2_result = engine.diagnose(ds2)  # 不能被 ds1 队头饿死
        elapsed = time.monotonic() - start
        spam_thread.join(timeout=5)
        self.assertEqual(ds2_result.result, RESULT_SUCCESS)
        self.assertLess(elapsed, 2.0)
        self.assertEqual(len(done), 4)

    def test_redact_never_exposes_credentials(self) -> None:
        self.assertNotIn("hunter2", redact_text("connection failed token=hunter2"))
        self.assertNotIn("hunter2", redact_text("http://user:hunter2@127.0.0.1/db"))
        self.assertIn("127.0.0.1", redact_text("http://user:hunter2@127.0.0.1/db"))
        self.assertIn("超时", redact_text("连接握手超时"))


if __name__ == "__main__":
    unittest.main()
