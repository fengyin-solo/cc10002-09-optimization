# 数据指标分析平台

面向指标口径定义、多维度聚合分析、AB实验评估、数据服务发布与数据应用交付的数据指标分析平台。

这是一个前后端分离的管理平台：前端 React + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 React + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

#### 连接诊断与输入源准入

“建立连接”不是改状态标记，而是一次可核对的只读连接诊断，数据源页面与 ETL 输入源准入共用同一份结论：

- `POST /api/datasource/{id}/diagnose`：对当前配置依次执行 **地址格式 → 握手 → 认证 → 读取探针** 四个只读阶段，返回逐阶段成败、失败阶段、尝试次数与共享预算；并发同配置请求只发出一次探测（单飞合并）。
- `POST /api/datasource/{id}/diagnose/cancel`：只取消当前等待者，不影响同配置其他调用所需的探测。
- `PATCH /api/datasource/{id}`：修改配置；认证方式、接入地址、查询超时、重试或凭据变化后，历史诊断立即标记过期，不能再作为准入依据。认证凭据只存进程内保险库，任何响应都不回显。
- `GET /api/datasource/{id}/diagnoses`：查看该配置的历史诊断记录。
- `POST /api/etljob/{id}/input-check`：只核对不启动，逐条说明每个输入源采用哪次诊断、为何可用或不可用；未绑定的自由文字输入源仅展示，不猜测绑定。
- `POST /api/etljob/{id}/actions`（启动任务）：先过输入源准入，拒绝时保留明确依据，不能用上次成功遮住本次失败。

一次诊断的所有步骤与重试共用同一个截止预算（`查询超时秒数 × (重试上限 + 1)`），每个阶段只拿到剩余预算；探测由固定大小的 FIFO 工作线程池消费，数据源各自的“连接池大小”是该配置的在探名额上限，超时后立即释放名额。

本地端点验证（正常 / 延迟 / 拒绝认证）：

```bash
python3 tools/probe_server.py 18080   # 提供 /healthy /slow /secure /boom
cd backend && .venv/bin/python -m unittest discover -s tests -v
```

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 指标定义 | `metricdef` | 指标口径 | 指标编号、指标名称、计算公式 |
| 维度管理 | `dimension` | 分析维度 | 维度编号、维度名称、数据来源表 |
| 数据源连接 | `datasource` | 数据源配置 | 配置编号、连接类型、接入地址 |
| 聚合查询 | `aggregation` | 聚合规则 | 规则编号、聚合函数、目标指标 |
| 分析看板 | `dashboard` | 看板配置 | 看板编号、看板名称、关联指标 |
| 分群分析 | `cohort` | 用户分群 | 分群编号、分群名称、筛选条件 |
| AB实验 | `abtest` | 实验方案 | 方案编号、实验名称、对照组配置 |
| 分析报告 | `report` | 报告模板 | 模板编号、模板名称、包含指标 |
| 指标预警 | `alert` | 预警规则 | 规则编号、关联指标、阈值上界 |
| 数据挖掘 | `datamine` | 挖掘任务 | 任务编号、输入数据集、算法类型 |
| 数据API | `api` | API服务 | 服务编号、输出数据集、查询参数定义 |
| ETL任务 | `etljob` | 加工任务 | 任务编号、输入源、输出目标 |
| 数据质量 | `quality` | 质量规则 | 规则编号、校验数据集、目标字段 |
| 数据建模 | `datamodel` | 模型定义 | 模型编号、模型名称、事实表名 |
| 数据导出 | `export` | 导出任务 | 任务编号、查询SQL、导出格式 |
| 数据订阅 | `subscribe` | 订阅任务 | 任务编号、数据源标识、订阅频率 |
| 数据血缘 | `lineage` | 血缘关系 | 关系编号、上游节点、下游节点 |
| 人群圈选 | `segment` | 圈选规则 | 规则编号、规则名称、筛选条件组合 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据分析与数据应用
