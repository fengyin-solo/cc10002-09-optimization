"""端到端集成测试：真实 FastAPI 服务 + 本地可控探测端点。

覆盖题目要求的验收面：
- 正常端点 / 延迟端点 / 拒绝认证端点，对照两份数据源配置与多个加工输入；
- 并发诊断的单飞合并、排队等待者取消、共享截止预算耗尽；
- 修改配置（认证方式/地址/超时/凭据）后旧诊断过期、必须重新诊断；
- ETL 输入源准入说明“采用哪次诊断、为何可用/不可用”，拒绝时保留依据；
- 未绑定的自由文字输入源可查看、不猜测绑定；
- 认证凭据不出现在任何响应里，诊断只读且不执行用户语句。
"""
from __future__ import annotations

import importlib.util
import json
import pathlib
import sys
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

import uvicorn

BACKEND_ROOT = pathlib.Path(__file__).resolve().parents[1]
REPO_ROOT = BACKEND_ROOT.parent
sys.path.insert(0, str(BACKEND_ROOT))


def _load_probe_server():
    spec = importlib.util.spec_from_file_location(
        "probe_server", REPO_ROOT / "tools" / "probe_server.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


probe_server_mod = _load_probe_server()

# 端口在 setUpClass 里用 0 动态分配，避免与机器上残留的手工冒烟服务冲突；
# http_json 通过这个可变全局读取实际地址。
API_BASE = "http://127.0.0.1:0"


def http_json(method: str, path: str, payload: dict | None = None, *, timeout: float = 10.0):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(
        API_BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


class IntegrationTest(unittest.TestCase):
    api_thread: threading.Thread | None = None
    api_server: uvicorn.Server | None = None
    probe_httpd: ThreadingHTTPServer | None = None
    probe_thread: threading.Thread | None = None
    probe_port: int = 0

    @classmethod
    def setUpClass(cls) -> None:
        global API_BASE

        # 1) 本地可控探测端点：端口 0 让内核分配空闲端口（正常/延迟/拒绝认证/500）。
        cls.probe_httpd = ThreadingHTTPServer(
            ("127.0.0.1", 0), probe_server_mod.ProbeHandler)
        cls.probe_port = cls.probe_httpd.server_address[1]
        cls.probe_thread = threading.Thread(
            target=cls.probe_httpd.serve_forever, name="probe-server", daemon=True)
        cls.probe_thread.start()

        # 2) 真实 uvicorn 进程内服务（与线上同一 ASGI 应用），同样使用动态端口。
        from app.main import app

        config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="error")
        cls.api_server = uvicorn.Server(config)
        cls.api_thread = threading.Thread(
            target=cls.api_server.run, name="api-server", daemon=True)
        cls.api_thread.start()
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline and not cls.api_server.started:
            time.sleep(0.02)
        bound_port = cls.api_server.servers[0].sockets[0].getsockname()[1]
        API_BASE = f"http://127.0.0.1:{bound_port}"
        health_deadline = time.monotonic() + 5
        while time.monotonic() < health_deadline:
            try:
                urllib.request.urlopen(API_BASE + "/api/health", timeout=1)
                break
            except OSError:
                time.sleep(0.05)

        # 3) 把种子里的本地地址对齐到本次动态探测端口，建立已知测试前提。
        base = f"http://127.0.0.1:{cls.probe_port}"
        http_json("PATCH", "/api/datasource/1", {
            "values": {"接入地址": f"{base}/healthy", "认证方式": "无认证", "认证凭据": "",
                       "查询超时秒数": "2", "重试上限": "1"}})
        http_json("PATCH", "/api/datasource/2", {
            "values": {"接入地址": f"{base}/secure", "认证方式": "Bearer令牌", "认证凭据": "",
                       "查询超时秒数": "2", "重试上限": "1"}})
        http_json("PATCH", "/api/datasource/3", {
            "values": {"接入地址": f"{base}/slow", "认证方式": "无认证",
                       "查询超时秒数": "1", "重试上限": "1"}})

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.api_server is not None:
            cls.api_server.should_exit = True
            if cls.api_thread is not None:
                cls.api_thread.join(timeout=5)
        if cls.probe_httpd is not None:
            cls.probe_httpd.shutdown()
            cls.probe_httpd.server_close()
        if cls.probe_thread is not None:
            cls.probe_thread.join(timeout=5)

    def probe_url(self, path: str) -> str:
        """拼出本次动态探测端点的完整地址（path 形如 /healthy）。"""
        return f"http://127.0.0.1:{self.probe_port}{path}"

    # ------------------------------------------------------------- 基础探测

    def test_01_healthy_endpoint_diagnosis_passes_all_phases(self) -> None:
        status, body = http_json("POST", "/api/datasource/1/diagnose",
                                 {"values": {"waiter_id": "w-healthy"}})
        self.assertEqual(status, 200)
        self.assertTrue(body["ok"], body)
        diag = body["detail"]["诊断"]
        self.assertEqual(diag["诊断结论"], "成功")
        self.assertIsNone(diag["失败阶段"])
        self.assertTrue(diag["只读探测"])
        phases = {step["阶段"]: step["状态"] for step in diag["步骤"]}
        self.assertEqual(phases, {
            "地址格式": "成功", "握手": "成功", "认证": "成功", "读取探针": "成功"})
        # 配置状态由真实诊断驱动，而不是“建立连接”只改标记。
        self.assertEqual(body["entry"]["status"], "已连接")
        # 历史诊断可核对，ETL 准入将引用这一份编号（最新一条即本次诊断）。
        _, listing = http_json("GET", "/api/datasource/1/diagnoses")
        self.assertGreaterEqual(listing["total"], 1)
        self.assertEqual(listing["items"][0]["诊断编号"], diag["诊断编号"])

    def test_02_bad_url_fails_at_format_and_never_connects(self) -> None:
        status, body = http_json("PATCH", "/api/datasource/2",
                                 {"values": {"接入地址": "jdbc:not-a-valid-http-url"}})
        self.assertTrue(body["ok"])
        status, body = http_json("POST", "/api/datasource/2/diagnose", {"values": {}})
        self.assertEqual(status, 200)
        self.assertFalse(body["ok"])
        diag = body["detail"]["诊断"]
        self.assertEqual(diag["失败阶段"], "地址格式")
        self.assertEqual(diag["尝试次数"], 0)
        self.assertEqual(body["entry"]["status"], "待校验")  # 失败不能显示已连接

    def test_03_auth_rejected_locates_phase_and_hides_credentials(self) -> None:
        # 恢复 ds2 到 /secure，但故意登记错误凭据。
        http_json("PATCH", "/api/datasource/2", {
            "values": {"接入地址": self.probe_url("/secure"), "认证凭据": "wrong-token-xyz"}})
        status, body = http_json("POST", "/api/datasource/2/diagnose", {"values": {}})
        self.assertFalse(body["ok"])
        diag = body["detail"]["诊断"]
        self.assertEqual(diag["失败阶段"], "认证")
        self.assertEqual(diag["尝试次数"], 1)  # 认证拒绝不重试
        raw = json.dumps(body, ensure_ascii=False)
        self.assertNotIn("wrong-token-xyz", raw)  # 错误信息与响应均不暴露认证内容
        self.assertIn("401", diag["结论说明"])

    def test_04_correct_credential_then_diagnosis_passes(self) -> None:
        http_json("PATCH", "/api/datasource/2",
                  {"values": {"认证凭据": "probe-ok-token"}})
        status, body = http_json("POST", "/api/datasource/2/diagnose", {"values": {}})
        self.assertTrue(body["ok"], body)
        self.assertEqual(body["detail"]["诊断"]["诊断结论"], "成功")
        raw = json.dumps(body, ensure_ascii=False)
        self.assertNotIn("probe-ok-token", raw)  # 正确凭据同样不外显
        self.assertTrue(body["entry"]["已登记认证凭据"])

    # ------------------------------------------------------------- 预算耗尽

    def test_05_slow_endpoint_exhausts_shared_budget(self) -> None:
        # ds3：超时 1 秒、重试 1 次 → 总预算 2 秒；端点固定延迟 3 秒，预算必然耗尽。
        status, body = http_json("POST", "/api/datasource/3/diagnose",
                                 {"values": {}}, timeout=8)
        self.assertEqual(status, 200)
        self.assertFalse(body["ok"])
        diag = body["detail"]["诊断"]
        self.assertIn(diag["失败阶段"], ("握手", "读取探针"))
        self.assertLessEqual(diag["总耗时毫秒"], 2600)  # 不会无限占用
        self.assertEqual(round(diag["预算秒数"], 3), 2.0)
        self.assertIn("预算", diag["结论说明"])
        # 预算耗尽后名额必须释放：再次诊断能立即获得连接名额而不是排队卡死。
        start = time.monotonic()
        status, body2 = http_json("POST", "/api/datasource/3/diagnose",
                                  {"values": {}}, timeout=8)
        self.assertLess(time.monotonic() - start, 3.0)
        self.assertFalse(body2["ok"])

    # ----------------------------------------------------- 单飞合并与取消

    def test_06_concurrent_diagnosis_coalesces_and_cancel_is_isolated(self) -> None:
        # 把 ds1 临时切到延迟端点，制造足够的排队窗口。
        http_json("PATCH", "/api/datasource/1",
                  {"values": {"接入地址": self.probe_url("/slow"),
                              "查询超时秒数": "5", "重试上限": "0"}})
        _, before_listing = http_json("GET", "/api/datasource/1/diagnoses")
        before_total = before_listing["total"]
        results: list = []
        errors: list = []

        def call(waiter_id: str) -> None:
            try:
                results.append((waiter_id, http_json(
                    "POST", "/api/datasource/1/diagnose",
                    {"values": {"waiter_id": waiter_id}}, timeout=10)))
            except Exception as exc:  # pragma: no cover - 失败时留痕
                errors.append(repr(exc))

        threads = [threading.Thread(target=call, args=(f"w-{i}",)) for i in range(3)]
        for thread in threads:
            thread.start()
        time.sleep(0.4)  # 等三个请求都挂到同一 flight
        # 只取消其中一个等待者。
        cancel_status, cancel_body = http_json(
            "POST", "/api/datasource/1/diagnose/cancel",
            {"values": {"waiter_id": "w-1"}})
        self.assertTrue(cancel_status == 200 and cancel_body["ok"])
        for thread in threads:
            thread.join(timeout=10)
        self.assertEqual(errors, [])
        by_waiter = {waiter_id: payload for waiter_id, (_, payload) in results}
        self.assertEqual(by_waiter["w-1"]["detail"]["诊断"]["诊断结论"], "已取消")
        # 其他人的探测没有被连带取消，延迟端点在 5 秒预算内成功。
        self.assertTrue(by_waiter["w-0"]["ok"], by_waiter["w-0"])
        self.assertTrue(by_waiter["w-2"]["ok"], by_waiter["w-2"])
        # 两个未取消的等待者共享同一次探测结论；取消者持独立的取消结论编号。
        self.assertEqual(
            by_waiter["w-0"]["detail"]["诊断"]["诊断编号"],
            by_waiter["w-2"]["detail"]["诊断"]["诊断编号"])
        self.assertNotEqual(
            by_waiter["w-0"]["detail"]["诊断"]["诊断编号"],
            by_waiter["w-1"]["detail"]["诊断"]["诊断编号"])
        _, listing = http_json("GET", "/api/datasource/1/diagnoses")
        # 三个并发等待者合并为一次真实探测，只新增一条诊断记录（取消者不留痕）。
        self.assertEqual(listing["total"], before_total + 1)
        # 恢复 ds1 的常规配置，避免超时/重试差异污染后续用例的指纹前提。
        http_json("PATCH", "/api/datasource/1", {
            "values": {"接入地址": self.probe_url("/healthy"),
                       "查询超时秒数": "2", "重试上限": "1"}})

    # ----------------------------------------------------- 配置变更失效

    def test_07_config_change_invalidates_old_diagnosis_for_admission(self) -> None:
        # 恢复 ds1 到正常端点并取得成功诊断（显式重置，保证不依赖上一用例残留状态）。
        http_json("PATCH", "/api/datasource/1", {
            "values": {"接入地址": self.probe_url("/healthy"),
                       "认证方式": "无认证", "认证凭据": "",
                       "查询超时秒数": "2", "重试上限": "1"}})
        _, ok_body = http_json("POST", "/api/datasource/1/diagnose", {"values": {}})
        self.assertTrue(ok_body["ok"], ok_body)
        diag_id = ok_body["detail"]["诊断"]["诊断编号"]

        # ETLJ-0001 的输入源是 DATA-0001：此时应准入通过并写明采用哪次诊断。
        status, admission = http_json("POST", "/api/etljob/1/input-check")
        self.assertTrue(admission["允许启动"])
        check = admission["输入源检查"][0]
        self.assertEqual(check["判定"], "准入通过")
        self.assertEqual(check["采用诊断编号"], diag_id)

        # 改地址：旧诊断立即过期，准入必须拒绝，不能用上次成功遮住本次失败。
        http_json("PATCH", "/api/datasource/1",
                  {"values": {"接入地址": self.probe_url("/secure")}})
        _, listing = http_json("GET", "/api/datasource/1/diagnoses")
        self.assertTrue(all(item["是否过期"] for item in listing["items"]))
        status, admission = http_json("POST", "/api/etljob/1/input-check")
        self.assertFalse(admission["允许启动"])
        self.assertEqual(admission["输入源检查"][0]["判定"], "拒绝准入")
        self.assertIn("过期", admission["输入源检查"][0]["结论"])

        # 启动任务同样被挡下；动作调用会独立再核对一次并保留自己的准入编号与依据。
        status, action_body = http_json("POST", "/api/etljob/1/actions",
                                        {"values": {"action": "启动任务"}})
        self.assertFalse(action_body["ok"])
        self.assertIn("准入未通过", action_body["message"])
        self.assertTrue(action_body["detail"]["准入编号"])
        self.assertEqual(
            action_body["detail"]["输入源检查"][0]["判定"], "拒绝准入")
        _, detail = http_json("GET", "/api/etljob/1")
        self.assertEqual(detail["status"], "草稿")  # 未启动，状态不变
        self.assertTrue(len(detail["准入记录"]) >= 2)  # 每次核对都留痕

        # 重新诊断（新地址需要认证，当前无凭据 → 认证失败 → 仍然拒绝）。
        _, rediag = http_json("POST", "/api/datasource/1/diagnose", {"values": {}})
        self.assertFalse(rediag["ok"])
        _, admission2 = http_json("POST", "/api/etljob/1/input-check")
        self.assertFalse(admission2["允许启动"])
        self.assertEqual(admission2["输入源检查"][0]["判定"], "拒绝准入")
        self.assertIn("认证", admission2["输入源检查"][0]["结论"])

    # ------------------------------------------------- 多输入源与未绑定文字

    def test_08_multiple_inputs_bound_and_unbound_text_coexist(self) -> None:
        # 显式把 ds2 恢复为“正确凭据 + 当前成功诊断”，不依赖用例执行顺序。
        http_json("PATCH", "/api/datasource/2", {
            "values": {"接入地址": self.probe_url("/secure"),
                       "认证方式": "Bearer令牌", "认证凭据": "probe-ok-token"}})
        _, ds2_diag = http_json("POST", "/api/datasource/2/diagnose", {"values": {}})
        self.assertTrue(ds2_diag["ok"], ds2_diag)
        # ETLJ-0002：ds://2（已绑定）+ 历史遗留 FTP 文字（未绑定）。
        status, admission = http_json("POST", "/api/etljob/2/input-check")
        self.assertEqual(status, 200)
        bound, unbound = admission["输入源检查"]
        self.assertTrue(bound["绑定"])
        self.assertEqual(bound["数据源配置ID"], 2)
        self.assertEqual(bound["判定"], "准入通过")  # ds2 在 test_04 已诊断成功
        self.assertFalse(unbound["绑定"])
        self.assertEqual(unbound["判定"], "未绑定（仅文字记录）")
        self.assertIn("FTP", unbound["输入源文字"])
        self.assertIsNone(unbound["采用诊断编号"])  # 不猜测成某个配置
        # 未绑定文字不阻断，但结论里明确它无法核对。
        self.assertTrue(admission["允许启动"])

        # ETLJ-0003：两个已绑定输入源，其中 ds1 当前诊断失败 → 整体拒绝。
        _, admission3 = http_json("POST", "/api/etljob/3/input-check")
        self.assertFalse(admission3["允许启动"])
        verdicts = {item["数据源配置ID"]: item["判定"] for item in admission3["输入源检查"]}
        self.assertEqual(verdicts[1], "拒绝准入")
        self.assertEqual(verdicts[2], "准入通过")
        failing = next(item for item in admission3["输入源检查"] if item["数据源配置ID"] == 1)
        self.assertTrue(failing["采用诊断编号"])  # 拒绝也写明依据的是哪次诊断

    def test_09_unknown_text_is_not_guessed_as_a_config(self) -> None:
        # 新建一个输入源文字与任何配置编号都不匹配的任务。
        http_json("POST", "/api/etljob", {"values": {
            "任务编号": "ETLJ-0099", "输入源": "手工台账.xlsx", "输出目标": "tmp.t1"}})
        _, admission = http_json("POST", "/api/etljob/4/input-check")
        only = admission["输入源检查"][0]
        self.assertFalse(only["绑定"])
        self.assertEqual(only["判定"], "未绑定（仅文字记录）")

    def test_10_credential_change_requires_rediagnosis(self) -> None:
        # 显式建立“ds2 凭据正确且诊断成功”的前提。
        http_json("PATCH", "/api/datasource/2", {
            "values": {"接入地址": self.probe_url("/secure"),
                       "认证方式": "Bearer令牌", "认证凭据": "probe-ok-token"}})
        _, ok = http_json("POST", "/api/datasource/2/diagnose", {"values": {}})
        self.assertTrue(ok["ok"], ok)
        # ds2 当前凭据正确、诊断成功；更换凭据后指纹变化，旧诊断不得继续准入。
        _, before = http_json("POST", "/api/etljob/2/input-check")
        self.assertTrue(before["允许启动"])
        http_json("PATCH", "/api/datasource/2",
                  {"values": {"认证凭据": "rotated-new-token"}})
        _, admission = http_json("POST", "/api/etljob/2/input-check")
        bound = next(item for item in admission["输入源检查"] if item["绑定"])
        self.assertEqual(bound["判定"], "拒绝准入")
        # 拒绝依据必须点明旧诊断已不可用：过期或指纹不一致，二者都是明确的重新诊断理由。
        self.assertTrue("指纹" in bound["结论"] or "过期" in bound["结论"], bound["结论"])
        # 任何列表/详情响应都不能回显凭据明文。
        _, listing = http_json("GET", "/api/datasource?size=20")
        self.assertNotIn("rotated-new-token", json.dumps(listing, ensure_ascii=False))
        _, detail = http_json("GET", "/api/datasource/2")
        self.assertNotIn("rotated-new-token", json.dumps(detail, ensure_ascii=False))

    def test_11_diagnosis_is_read_only_no_user_statement_executed(self) -> None:
        # 即使用户在地址外的字段里塞入“语句”，探测只访问固定 URL 的 GET，
        # 不会把任何用户输入当作语句执行；这里验证探测请求方法面（服务端只接受 GET 探针）。
        http_json("PATCH", "/api/datasource/1", {
            "values": {"接入地址": self.probe_url("/healthy"),
                       "转换脚本": "DROP TABLE users;"}})
        status, body = http_json("POST", "/api/datasource/1/diagnose", {"values": {}})
        self.assertTrue(body["ok"], body)
        self.assertEqual(body["detail"]["诊断"]["诊断结论"], "成功")


if __name__ == "__main__":
    unittest.main()
