.PHONY: install backend frontend test probe

install:
	cd backend && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
	cd frontend && npm install

backend:
	cd backend && ./run.sh

frontend:
	cd frontend && npm run dev

# 连接诊断与 ETL 输入源准入的全部后端测试（引擎单元 + 端到端）。
# 端到端测试会自行启动本地可控探测端点，无需手工先起 probe_server。
test:
	cd backend && .venv/bin/python -m unittest discover -s tests -v

# 单独启动本地可控探测端点：正常 /healthy、延迟 /slow、拒绝认证 /secure、500 /boom。
probe:
	python3 tools/probe_server.py 18080
