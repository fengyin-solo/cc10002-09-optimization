"""本地可控的探测端点：正常 / 延迟 / 拒绝认证，专用于连接诊断的端到端验证。

端点约定（与 backend/app/seed.py 中的两份数据源配置对应）：
- GET /healthy   始终 200，模拟可正常握手、认证、读取的数据源；
- GET /slow      延迟 SLOW_SECONDS 后响应，用于触发共享截止预算耗尽；
- GET /secure    要求 Authorization: Bearer probe-ok-token，否则 401，
                 用于验证认证阶段失败定位与错误信息不泄露凭据；
- GET /boom      始终 500，用于验证瞬态失败与重试预算。

只依赖标准库，运行：python3 tools/probe_server.py [port]
"""
from __future__ import annotations

import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DEFAULT_PORT = 18080
SLOW_SECONDS = 3.0
EXPECTED_TOKEN = "probe-ok-token"


class ProbeHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: object) -> None:  # 安静输出
        sys.stderr.write("[probe-server] " + fmt % args + "\n")

    def do_GET(self) -> None:  # noqa: N802 - 标准库命名
        if self.path.startswith("/healthy"):
            self._respond(200, b'{"status":"ok"}')
            return
        if self.path.startswith("/slow"):
            time.sleep(SLOW_SECONDS)
            self._respond(200, b'{"status":"slow-but-ok"}')
            return
        if self.path.startswith("/secure"):
            auth = self.headers.get("Authorization", "")
            if auth == f"Bearer {EXPECTED_TOKEN}":
                self._respond(200, b'{"status":"authed"}')
            else:
                # 故意不回显收到的 Authorization 内容，验证服务端脱敏口径。
                self._respond(401, b'{"error":"authentication required"}')
            return
        if self.path.startswith("/boom"):
            self._respond(500, b'{"error":"temporary failure"}')
            return
        self._respond(404, b'{"error":"not found"}')

    def _respond(self, status: int, body: bytes) -> None:
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # 诊断端在共享预算耗尽后会主动断开，属于预期行为，不刷错误日志。
            pass


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT
    server = ThreadingHTTPServer(("127.0.0.1", port), ProbeHandler)
    sys.stderr.write(f"[probe-server] listening on http://127.0.0.1:{port}\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
