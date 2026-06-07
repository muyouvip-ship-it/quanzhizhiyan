# 量化之神

面向 A 股投研、策略回测、实时监控与 QMT 交易联动的一体化量化工作台。

## 新对话接手方式

新的 AI 对话开始时，先按顺序阅读这 3 个文件：

1. `README.md`：项目总览、模块边界、运行方式和关键约定。
2. `AI_PROGRESS.md`：最近进度、已改文件、验证结果和下一步。
3. `AI_RULES.md`：长期有效的协作规则、命名规范和固定工作流。

每次任务收尾时，AI 必须更新 `AI_PROGRESS.md`，写清楚：

- 本次做了什么。
- 改了哪些文件。
- 跑了哪些验证。
- 当前风险或未完成事项。
- 下一步建议。

`AI_RULES.md` 只放长期有效内容，不记录临时进度。

## 产品定位

量化之神不是单一聊天机器人，也不是单一回测脚本，而是围绕 A 股市场的研究、数据、策略、监控和交易执行平台。目标是把“看市场、做研究、写策略、跑回测、盯信号、管仓位、复盘沉淀”收进一个可长期使用的工作台。

核心用户：

- 投研用户：智能分析、资讯之眼、股票市场、历史报告、每日复盘。
- 策略研究员：策略管理、策略 DSL、策略回测、因子进化、回测结果分析。
- 交易执行用户：实时监控、虚拟仓、实盘仓、人工审批、风控熔断。
- 数据运维用户：行情源治理、日 K/分钟线同步、QMT bridge、日志调试。

## 当前重点方向

近期最值得继续收紧的方向：

- 产品口径统一：同一概念在前后端、文档、页面文案和接口字段中保持一致。
- 实盘安全边界：实盘仓只读、交易能力默认走虚拟仓或显式审批，避免误操作。
- 数据可信度展示：页面明确展示数据源、同步时间、缓存/实时状态和多源校验结果。
- 策略可解释性：策略筛选、回测、实时信号和最终建议要能解释原因，避免结论反转。

## 核心模块

| 模块 | 前端路由 | 说明 |
|---|---|---|
| 控制台 | `/` | 工作台入口、最近报告和核心状态摘要 |
| 资讯之眼 | `/news-eye` | 多源资讯、个股新闻、情绪和 AI 解读 |
| 催化选股 | `/catalyst-selection` | 事件驱动 AI 量化闭环、主线机会榜、AI 监控池和闭环审计 |
| 股票市场 | `/stock-market` | 指数、榜单、板块、资金流、搜索和 K 线 |
| 智能分析 | `/analysis` | 多智能体股票分析和协同工作流 |
| 历史报告 | `/reports` | 分析报告列表、详情和历史沉淀 |
| 每日复盘 | `/daily-review` | 市场、持仓、自选、主题和次日候选复盘 |
| 自选 & 定时 | `/portfolio` | 自选股、持仓导入、定时分析任务 |
| 策略管理 | `/strategies` | 策略列表、模板、DSL、版本和编译 |
| 策略回测 | `/backtest` | 回测创建、进度、记录、对比和结果入口 |
| 实时监控 | `/realtime` | 监控实例、事件流、审批、风控和自动交易 |
| 虚拟仓 | `/virtual-warehouse` | QMT 模拟仓快照、委托、成交和下单联调 |
| 实盘仓 | `/live-warehouse` | QMT 实盘资产、持仓、委托、成交核对；只读 |
| 跟踪看板 | `/tracking-board` | 持仓和自选跟踪摘要 |
| 日志调试 | `/debug/logs` | 后端、任务、运行日志检索 |
| 设置 | `/settings` | 模型、QMT、数据源、自动更新和系统配置 |

更完整的产品说明见 `产品文档.md`；AI 量化闭环最终版使用说明见 `AI_QUANT_FINAL_USAGE.md`。

## 技术架构

- 后端：FastAPI + SQLAlchemy + PostgreSQL。
- 前端：React + TypeScript + Vite。
- 任务：后端 lifespan worker、独立 scheduler、部分后台线程任务。
- 数据：PostgreSQL 为主；日 K/分钟线可配合 Parquet/缓存；已移除 SQLite 使用口径。
- QMT：通过 Windows 侧 `qmt_bridge_server.py` 暴露 bridge；本机后端通过数据库中的用户 QMT 账户配置读取，不再依赖 `.env` 写死账户。
- 登录：邮箱验证码，API 使用 Bearer token。

## 关键数据与后台任务

- 日 K/分钟线数据：业务读取以最终表 `stock_daily_kline`、`stock_minute_kline` 为准；`raw_*`、`norm_*`、`pub_*` 和对账表保留为采集、标准化、发布审计与质量追踪过程层，Parquet 缓存服务回测加速。新增写入统一使用 `000001.SZ` / `600000.SH` / `920118.BJ` 这类带交易所后缀格式。
- 多源治理：设置页和相关接口需要展示 raw/norm/pub/market 关系、质量状态、发布状态和对账结果。
- 资讯之眼：资讯入库表为 `market_news_items`，后台 worker 会周期性抓取财联社、东方财富等来源。
- QMT 同步：`ENABLE_QMT_SYNC_WORKER=1` 后，后台同步实盘仓/虚拟仓快照；页面状态要区分“实时直连 / 后台在线 / 快照可用 / 未连接”。
- 实时监控：应在前端页面未打开时也由后端 worker 正常执行；同一监测 K 线内交易指令只能触发一次。

## 运行方式

### 后端

```bash
set -a; source .env; set +a
uv run uvicorn api.app:app --host 127.0.0.1 --port 8500
```

当前本地常用方式是用 `screen` 托管：

```bash
screen -dmS tradingagents-backend /bin/zsh -lc 'cd /Users/wolf/Documents/DaiMa/TradingAgents-AShare-main && set -a; source .env; set +a; exec /Users/wolf/.local/bin/uv run uvicorn api.app:app --host 127.0.0.1 --port 8500 >> .runtime/backend.log 2>&1'
```

### 前端

```bash
cd frontend
npm install
npm run dev
```

常用访问地址：`http://127.0.0.1:5174/`。

### 调度器

```bash
uv run tradingagents-scheduler
```

如果只启动后端和前端，不启动 scheduler，定时分析类任务不会按时间自动执行。部分 worker 由后端 lifespan 启动，具体以 `.env` 开关为准。

## 常用验证

```bash
python -m py_compile api/services/qmt_virtual_account_service.py
pytest tests/test_virtual_warehouse.py tests/test_qmt_sync_scheduler_service.py tests/test_realtime_monitor.py -q
cd frontend && npm run build
```

按改动范围选择更小或更大的测试集。涉及前端页面的改动，构建通过后还要用浏览器实际查看页面。

## 重要安全边界

- 实盘仓默认只读，不从本系统提交实盘委托或撤单。
- 虚拟仓可用于 QMT 模拟交易联调。
- QMT 账号配置走设置页/数据库，不再把账号写死在 `.env`。
- 自动交易要有风控、幂等、K 线周期去重和事件记录。
- 不要在文档、日志或提交中暴露真实 token、密码、数据库连接串和 bridge token。

## 相关文档

- `AI_PROGRESS.md`：AI 交接进度。
- `AI_RULES.md`：长期协作规则。
- `产品文档.md`：完整产品说明。
- `项目性能与功能拓展分析.md`：平台升级方向。
- `多源行情数据增量治理机制说明.md`：多源行情治理。
- `回测数据订阅与增量更新流程说明.md`：回测数据订阅和自动更新。
- `QMT虚拟仓接入说明.md`：QMT 虚拟仓接入。
- `QMT全市场分钟线下载方案.md`：QMT 分钟线数据方案。
