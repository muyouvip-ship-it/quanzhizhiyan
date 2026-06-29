# AI Progress

本文件记录最近交接进度。每次任务收尾时更新，保持最新内容在最上方。

## 2026-06-22 实时监控/数据链路/旧流程清理收口

### 本次做了什么

- 修复实时监控收益统计口径：成交事件缺少券商 `trade_time` 时，不再因为系统写入时间不匹配而漏算当日买入/卖出现金流。
- 补齐回测数据链路：新增筹码分布、资金流、财务快照表结构与通用导入；资金流/财务导入会回填日 K 富字段；研报更新接入资讯之眼东方财富研报源。
- 同步设置页与接口统计：回测数据任务统计纳入筹码、资金流、财务、研报；设置页数据类型加入资金流；自动更新服务新增资金流下载任务。
- 清理实时监控人工审批活跃流程：运行态统计不再输出 `approvals`，QMT 检查脚本改查收益对照；历史审批表仅保留归档兼容与自动清理。
- 清理开发态残留：前端生产环境不再自动写入 `dev-test-token-001`，`/auth/me` 未登录也不再返回模拟用户；删除设置页下载任务 `console.log`。
- 修复策略 DSL 文案：未知因子继续作为自定义因子待实现/待映射处理，但不再显示“占位因子”误导。
- 修复日 K Parquet 导出脚本连接泄漏：`export_daily_kline_to_parquet()` 与 `_resolve_bounds()` 现在正确关闭连接并释放 engine，避免测试和导出清理时锁表。
- 更新 README 和产品文档：催化选股入口归入选股中心，新策略管理/实时监控/实盘仓/数据源/人工审批历史状态说明同步到当前口径。
- 补齐测试环境：pytest 默认关闭接口限流，避免完整测试套件中验证码接口被全局限流污染。

### 改动文件

- `api/services/realtime_monitor_service.py`
- `api/generic_importer.py`
- `api/database.py`
- `api/backtest_data_api.py`
- `api/backtest_data_models.py`
- `api/services/backtest_data_auto_update_service.py`
- `api/services/catalyst_selection_service.py`
- `api/services/strategy_dsl_compiler.py`
- `frontend/src/services/api.ts`
- `frontend/src/stores/authStore.ts`
- `frontend/src/pages/Settings.tsx`
- `frontend/src/pages/CatalystSelection.tsx`
- `frontend/src/types/index.ts`
- `scripts/export_daily_kline_to_parquet.py`
- `scripts/qmt_realtime_monitor_check.py`
- `tests/conftest.py`
- `tests/test_generic_importer.py`
- `frontend/src/stores/authStore.test.ts`
- `README.md`
- `产品文档.md`
- 以及相关既有测试断言同步。

### 验证结果

- `.venv/bin/python -m py_compile api/services/realtime_monitor_service.py api/generic_importer.py api/backtest_data_api.py api/database.py api/services/backtest_data_auto_update_service.py api/services/catalyst_selection_service.py api/services/strategy_dsl_compiler.py`：通过。
- `.venv/bin/python -m py_compile scripts/export_daily_kline_to_parquet.py`：通过。
- `.venv/bin/python -m pytest tests/test_realtime_monitor.py tests/test_generic_importer.py tests/test_backtest_data_auto_update_service.py tests/test_news_eye_service.py::test_fetch_external_news_collects_research_reports_for_focus_symbols -q`：`50 passed`。
- `.venv/bin/python -m pytest tests/test_selection_center_service.py tests/test_strategy_platform_true_engine.py tests/test_strategy_platform_extensions.py -q`：`44 passed`。
- `.venv/bin/python -m pytest -q`：`583 passed, 9 skipped`。
- `cd frontend && npm test -- src/stores/authStore.test.ts src/pages/RealtimeMonitorV2.test.ts src/pages/settingsQmtStatus.test.ts src/components/sidebarNav.test.ts`：`17 passed`。
- `cd frontend && npm test`：`26 passed`。
- `cd frontend && npm run build`：通过。
- `git diff --check`：通过。

### 当前风险或未完成事项

- `realtime_approvals` 模型和归档清理函数仍保留，用于兼容历史数据并自动清理旧 pending 审批；当前实时监控主流程和前端类型已不再暴露人工审批队列。
- 完整 pytest 仍有 `catalyst_selection_service.py` 中 `datetime.utcnow()` 的弃用 warning，暂不影响运行，但后续可统一改为 timezone-aware UTC。
- 研报、公告、筹码和资金流链路已经接入导入/统计框架；真实覆盖率仍取决于后续定时任务和外部数据源可用性。

## 2026-06-03 AI量化闭环最终版使用说明

### 本次做了什么

- 新增最终版使用说明文档，覆盖启动、配置、催化选股、AI量化闭环状态、AI监控池、实时监控、结算反馈、接口、异常处理和验收命令。
- 明确最终版判断标准：`主动触发 event_driven`、端到端 `active`、闭环 `6/6`、远程 LLM 就绪、AI监控池带门禁分布。
- 文档强调安全边界：默认 `monitor_only`，实盘仓只读，不明文泄露 LLM key，页面结论仅用于研究和复盘。

### 改动文件

- `AI_QUANT_FINAL_USAGE.md`
- `AI_PROGRESS.md`

### 验证结果

- 本次为文档生成，内容依据当前运行态、前端路由、后端 catalyst-selection 接口和最新闭环验收结果整理。
- 最近运行态确认：后端 `127.0.0.1:8500` 健康，前端 `127.0.0.1:5174` 运行中；催化选股接口显示 `e2e_status=active`、`pass_rate=1.0`、`discovery_mode=event_driven`；AI监控池为 `monitor_only` 且 `monitor_pool` / `risk_config` 均带 `gate_counts`。

### 当前风险或未完成事项

- 文档没有重新跑完整测试；最近闭环收口已跑过 `tests/test_catalyst_selection_service.py tests/test_daily_review_market_behavior.py`、`tests/test_market_routes_formal.py tests/test_data_source_governance.py` 和前端 `npm run build`。
- 若后续 LLM provider、模型名、端口或默认执行模式调整，需要同步更新本文档。

### 下一步建议

- 在 README 的核心模块表中补一行 `/catalyst-selection`，让新入口在总览里也可见。
- 如果要发版，可把 `AI_QUANT_FINAL_USAGE.md` 作为最终验收说明附带给使用者。

## 2026-05-25 项目排查问题修复收口

### 本次做了什么

- 修复无数据库环境下后端模块导入即失败的问题：`api.database` 与 `api.core.strategy_db` 改为懒加载 PostgreSQL engine/session，保留实际使用数据库时必须配置 PostgreSQL 的约束。
- 补齐测试环境预加载：新增 `tests/conftest.py`，支持 `TEST_DATABASE_URL` 自动注入，并关闭测试时不必要的后台 worker。
- 修正 CI：后端 job 增加 PostgreSQL 服务与数据库环境变量；前端 job 改为在 `frontend/` 下执行，并补充 `npm test` 与 `npm run build`。
- 补齐入口与依赖：新增 `cli` 入口，补 `api.main.run()`，将 `python-dotenv` 写入 Python 依赖，`.env.example` 明确标出 `DATABASE_URL`。
- 清理前端 lint error：修复 React hooks 规则下的确定性错误，保留当前不阻断 CI 的 warning。

### 改动文件

- `.env.example`
- `.github/workflows/ci.yml`
- `api/database.py`
- `api/core/strategy_db.py`
- `api/main.py`
- `cli/__init__.py`
- `cli/main.py`
- `tests/conftest.py`
- `pyproject.toml`
- `requirements.txt`
- `frontend/package.json`
- `frontend/src/components/Charts.tsx`
- `frontend/src/components/Header.tsx`
- `frontend/src/components/KlinePanel.tsx`
- `frontend/src/components/ReportViewer.tsx`
- `frontend/src/components/TaskProgressBanner.tsx`
- `frontend/src/components/VirtualList.tsx`
- `frontend/src/hooks/useSSE.ts`
- `frontend/src/hooks/useTypeWriter.ts`
- `frontend/src/pages/Analysis.tsx`
- `frontend/src/pages/BacktestResult.tsx`
- `frontend/src/pages/Reports.tsx`
- `AI_PROGRESS.md`

### 验证结果

- `pytest -q`：`346 passed`
- `pytest tests/test_config_fallback.py -q`：`3 passed`
- `cd frontend && npm run lint`：通过，剩余 `18` 个 warning，无 error。
- `cd frontend && npm run build`：通过。
- `cd frontend && npm test`：`3` 个测试文件、`12` 个测试通过。
- `python -m compileall -q api scheduler cli tests`：通过。
- `TA_DISABLE_DOTENV=1 env -u DATABASE_URL -u STRATEGY_DATABASE_URL python ...`：确认无数据库环境可导入 `api.database`、`api.core.strategy_db`、`api.main`，实际 `init_db()` 时按预期提示必须配置 `DATABASE_URL`。

### 当前风险或未完成事项

- 前端 lint 仍有 `18` 个 hooks 依赖相关 warning，主要分布在 `TrackingBoardPanel`、`DebugLogs`、`Settings`、`VirtualWarehouse` 等页面；当前不阻断 CI，但后续可单独收敛。
- 工作区在本次任务开始前已有大量未提交改动，本次只针对排查出的启动、CI、入口和 lint error 问题做收口，未回滚其他现有改动。
- CI 的 PostgreSQL 测试库已配置为 GitHub Actions service；如果后续测试依赖扩展到 Redis/QMT/外部行情，还需要继续补 service 或 mock。

### 下一步建议

- 分批清理剩余前端 hooks warning，避免后续 React 编译器规则升级时变成 error。
- 为数据库懒加载层增加专门单测，覆盖“无 env 可导入、实际使用时报错、配置 PostgreSQL 后可初始化”的三种路径。
- 若要正式落库，先只 stage 本次相关文件，避免把工作区已有未跟踪数据文件和脚本混入同一个提交。

## 2026-05-22 资讯之眼与主线机会榜测试收口

### 本次做了什么

- 重启正式后端 `127.0.0.1:8500`，确认运行中服务已加载新代码，`/v1/system/market-data-status` 业务表返回 `stock_daily_kline` / `stock_minute_kline`。
- 定位并修复主线机会榜 LLM 个股推荐没有走已配置模型的问题：旧逻辑把错误缓存按“证据”全局复用，未配置用户的失败会污染已配置用户；现在缓存 key 带模型配置指纹，失败冷却也按模型配置隔离。
- 将默认 LLM 配置收敛到星火 MaaS：`https://maas-coding-api.cn-huabei-1.xf-yun.com/v2` + `astron-code-latest`，避免默认路径继续回到 OpenAI 或本地模型。
- 用浏览器打开 `http://127.0.0.1:5174/news-eye` 做页面冒烟：资讯之眼、主线机会榜、资讯列表、LLM 解读按钮均可见，历史回溯卡片未出现，浏览器控制台无 error。

### 改动文件

- `api/services/news_theme_service.py`
- `tests/test_news_theme_service.py`
- `tradingagents/default_config.py`
- `.env`
- `.env.example`
- `AI_PROGRESS.md`

### 验证结果

- `python -m py_compile api/services/news_theme_service.py tradingagents/default_config.py tests/test_news_theme_service.py`
- `pytest tests/test_news_theme_service.py tests/test_news_eye_service.py -q`：`29 passed`
- `set -a; source .env; set +a; pytest tests/test_market_data_pipeline_service.py tests/test_backtest_data_api_calendar.py tests/test_news_theme_service.py tests/test_news_eye_service.py -q`：`36 passed`
- `cd frontend && npm run build`：通过。
- `git diff --check`：通过。
- 正式接口验证：资讯总量 `18886`，最新资讯时间 `2026-05-22T19:29:02`，后台采集 `success`，活跃来源含财联社、东方财富、新浪、富途、同花顺和个股新闻，`dedupe_key` 重复组 `0`。
- 主线机会榜真实接口已触发 MaaS 模型调用，日志显示 `openai (astron-code-latest) at https://maas-coding-api.cn-huabei-1.xf-yun.com/v2`。

### 当前风险或未完成事项

- MaaS 外部接口返回 `AppIdNoAuthError` / 无 key 错误，说明当前保存的 key 或应用权限还不能调用 `astron-code-latest`；代码路径已经打到正确模型，但主线榜个股推荐仍会回落到 `fallback:positive_news`，需要更换/授权可用的 MaaS key 后再复测 LLM 结果。
- 浏览器截图命令超时，已用 DOM 与控制台日志完成页面冒烟校验。

## 2026-05-22 分钟 K 线最终表 symbol 归一化完成

### 本次做了什么

- 完成 `stock_minute_kline` 历史 symbol 归一化，将纯 6 位代码和 `bj` 前缀统一改为 `.SZ/.SH/.BJ` 后缀。
- 先用分批事务推进并清理 WAL/死元组；发现原脚本正则 `{{6}}` 导致日线候选失效、只走 `pg_stats` 抽样后，修正为完整候选扫描，避免漏批。
- 为加速 3 亿级分钟表维护，临时移除 `symbol` 相关索引/唯一约束，执行单通道全表标准化 UPDATE；随后删除标准化后重复键 `434` 行，并重建唯一约束与查询索引。
- 删除历史重复索引创建点，保留 canonical 索引：`stock_minute_kline_symbol_trade_time_key`、`idx_minute_symbol`、`idx_minute_time`，避免启动或 QMT 同步脚本再次创建重复索引。
- 新增维护脚本：
  - `scripts/maintenance_normalize_minute_symbols_single_pass.py`
  - `scripts/maintenance_normalize_minute_symbols_fast.py`

### 改动文件

- `api/database.py`
- `scripts/normalize_stock_minute_symbols.py`
- `scripts/maintenance_normalize_minute_symbols_single_pass.py`
- `scripts/maintenance_normalize_minute_symbols_fast.py`
- `scripts/qmt_minute_history_sync.py`
- `windows_qmt_bridge_update/scripts/qmt_minute_history_sync.py`
- `AI_PROGRESS.md`

### 数据库验证结果

- `scripts/maintenance_normalize_minute_symbols_single_pass.py` dry-run：`matched_rows=0`。
- `scripts/normalize_stock_minute_symbols.py --data-type all --max-symbols 20` dry-run：`plans=0 matched_rows=0 safe_update_rows=0 conflict_rows=0`。
- `stock_daily_kline`：`17,689,586` 行、`5,845` 个 symbol、范围 `1990-12-19 ~ 2026-05-20`、旧格式行数 `0`。
- `stock_minute_kline`：`345,012,237` 行、`5,190` 个 symbol、范围 `2020-01-02 09:30:00 ~ 2026-05-21 15:00:00`、旧格式行数 `0`。
- `stock_minute_kline` 重复键：`duplicate_groups=0`、`duplicate_rows=0`。
- `stock_minute_kline` 当前索引：`stock_minute_kline_pkey`、`stock_minute_kline_symbol_trade_time_key`、`idx_minute_symbol`、`idx_minute_time`。
- 完成后 `/System/Volumes/Data` 剩余约 `69GiB`。

### 当前风险或未完成事项

- symbol 格式混用问题已清零；后续新增写入仍需保持 `.SZ/.SH/.BJ` 后缀。
- 1 分钟 K 线虽然已统一格式，但是否覆盖全市场全历史仍取决于 QMT 补数任务；当前最终表范围为 `2020-01-02 ~ 2026-05-21`，不是对 1990 以来全市场分钟线的声明。
- 本次对正式大表做过维护窗口操作，已重建唯一约束和查询索引；后续如果再做 3 亿级维护，优先使用单通道脚本并预留充足磁盘。

## 2026-05-21 K 线最终业务表口径收敛

### 本次做了什么

- 按用户确认把日 K/1 分钟 K 线的业务读取口径收敛到最终表：`stock_daily_kline`、`stock_minute_kline`。
- 保留 `raw_*`、`norm_*`、`pub_*` 和对账表作为采集、标准化、发布审计与质量追踪过程层，不再让业务读路径默认依赖 `market_stock_*` 兼容视图。
- `reconcile_daily_trade_dates()` 和 `publish_minute_trade_date()` 写入 `pub_*` 后会默认同步写回最终 `stock_*` 表，不再受 `MARKET_DATA_WRITE_LEGACY_TABLES=0` 阻挡。
- QMT 盘中分钟线写入恢复为直接 upsert `stock_minute_kline`，同时继续回灌 raw/norm/pub/对账链路。
- 新增 `scripts/sync_published_kline_to_final.py`，用于把已有 `pub_stock_daily_kline` / `pub_stock_minute_kline` 安全同步到最终表；脚本默认 dry-run，显式 `--apply` 才写库。
- 将当前正式库已有发布审计层同步到最终表：`pub_stock_daily_kline -> stock_daily_kline` 写回 `76,914` 行，`pub_stock_minute_kline -> stock_minute_kline` 写回 `3,922` 行。
- 设置页“多源增量治理”文案改为“多源行情治理 / 最终表 / 发布审计”，避免继续把发布层当业务源。
- 统计接口对 3 亿级 `stock_minute_kline` 使用快速估算，避免 `COUNT(*)`、`MAX(updated_at)` 或 `MIN(DATE(trade_time))` 拖慢正式库。
- 新增 `scripts/normalize_stock_minute_symbols.py`，支持对最终日 K/分钟 K 表做 symbol 格式 dry-run 和分批归一化；新增 `--data-type`、`--start-symbol`、`--stop-symbol`、`--batch-size` 等控制项。
- 修正 `scripts/fetch_minute_kline_v3.py`、`scripts/fetch_minute_kline_v4.py`、`scripts/fetch_minute_kline_full.py`、`scripts/fetch_bj_minute_kline.py` 的新增分钟线写入格式，后续统一写 `.SZ/.SH/.BJ` 后缀，不再继续制造纯代码或 `bj` 前缀记录。
- 主线/市场周边的日 K 表选择顺序改为最终表优先：`news_theme_service`、`market.py`、`backtest_data_auto_update_service` 不再优先取 `pub_stock_daily_kline`。
- 继续执行剩余日 K symbol 归一化：从 `600395` 续跑并补掉上次失败批次遗留的 `600390`~`600393`，最终 `stock_daily_kline` 旧格式 symbol 清零。

### 改动文件

- `api/services/market_data_pipeline_service.py`
- `api/services/qmt_market_data_service.py`
- `api/backtest_data_api.py`
- `api/database.py`
- `scripts/qmt_minute_history_sync.py`
- `scripts/sync_published_kline_to_final.py`
- `scripts/normalize_stock_minute_symbols.py`
- `scripts/fetch_minute_kline_v3.py`
- `scripts/fetch_minute_kline_v4.py`
- `scripts/fetch_minute_kline_full.py`
- `scripts/fetch_bj_minute_kline.py`
- `api/routes/market.py`
- `api/services/news_theme_service.py`
- `api/services/backtest_data_auto_update_service.py`
- `frontend/src/pages/Settings.tsx`
- `tests/test_market_data_pipeline_service.py`
- `tests/test_backtest_data_api_calendar.py`
- `README.md`
- `AI_PROGRESS.md`

### 验证结果

- `.venv/bin/python -m py_compile api/services/market_data_pipeline_service.py api/backtest_data_api.py api/database.py api/services/qmt_market_data_service.py scripts/sync_published_kline_to_final.py scripts/qmt_minute_history_sync.py tests/test_market_data_pipeline_service.py tests/test_backtest_data_api_calendar.py`：通过。
- `set -a; source .env; set +a; .venv/bin/python -m pytest tests/test_market_data_pipeline_service.py tests/test_backtest_data_api_calendar.py -q`：`7 passed`，测试已改为隔离 PostgreSQL schema，不再清空正式行情表。
- `set -a; source .env; set +a; .venv/bin/python -m pytest tests/test_news_theme_service.py -q`：`15 passed`。
- `cd frontend && npm run build`：通过。
- `git diff --check`：通过。
- `.venv/bin/python -m py_compile scripts/normalize_stock_minute_symbols.py ...`：通过。
- `scripts/sync_published_kline_to_final.py --data-type all --apply`：写回 `76,914` 条日 K 发布审计行和 `3,922` 条分钟发布审计行。
- 正式库验证：`preferred_daily_kline_table()` 返回 `stock_daily_kline`，`preferred_minute_kline_table()` 返回 `stock_minute_kline`。
- 正式库验证：`stock_daily_kline` 日期范围已到 `2026-05-20`，`pub_stock_daily_kline` 日期范围为 `2026-04-20 ~ 2026-05-20`。
- 正式库验证：`stock_minute_kline` 最新时间为 `2026-05-21 15:00:00`，`2026-05-21` 当天最终分钟表覆盖 `3,653` 个 symbol。
- `/v1/system/market-data-status` 对应服务层验证：返回表为 `stock_daily_kline` / `stock_minute_kline`，分钟线 `2026-05-21` 摘要为 `final`，样本 symbol 覆盖率可正常返回。
- 日 K 主表 symbol 归一化 dry-run：初始检测到 `5,838` 个旧格式日 K symbol，涉及 `17,612,672` 行，冲突行 `0`。
- 已实际分批归一化日 K 主表全部旧格式 symbol：首轮约 `12,465,865` 行；继续从 `600395` 更新 `6,075,745` 行；最后补 `600390`~`600393` 共 `22,068` 行，冲突行均为 `0`。
- 日 K 主表最终复核：`stock_daily_kline` 总行数 `17,689,586`、`5,845` 个 symbol、`8,685` 个交易日，日期范围 `1990-12-19 ~ 2026-05-20`，`unsuffixed_symbols=0`、`bj_prefix_symbols=0`。
- 日 K 归一化完成后 dry-run：`scripts/normalize_stock_minute_symbols.py --data-type daily` 返回 `plans=0 matched_rows=0 safe_update_rows=0 conflict_rows=0`。
- 磁盘满触发 PostgreSQL 写入失败后，失败批次已回滚、已提交批次保留；随后执行 `VACUUM ANALYZE stock_daily_kline` 并复核数据库可连接、`stock_daily_kline` 总行数仍为 `17,689,586`，日期范围 `1990-12-19 ~ 2026-05-20`。

### 当前风险或未完成事项

- `stock_daily_kline` 的旧格式 symbol 已全部处理完；后续日 K 新增写入仍应保持 `.SZ/.SH/.BJ` 后缀标准。
- `stock_minute_kline` 仍存在历史 symbol 格式混用（纯代码与 `.SH/.SZ/.BJ` 后缀并存）。这次没有执行 3 亿级分钟表历史规范化，避免磁盘和 WAL 风险；脚本支持后续按 `--symbol` 或 `--start-symbol/--stop-symbol` 分批处理。
- 1 分钟 K 线仍不是全市场全历史完整覆盖；当前 `2026-05-21` 最终分钟表覆盖 `3,653` 个 symbol，而日 K 全市场通常约 5,000+。补全仍需继续跑 QMT/指定日期补数。
- 分钟线设置页统计为了保护性能，正式大表只返回估算总量，日期范围不再强行扫描计算；完整覆盖审计继续使用专门脚本或按日期/股票分片查询。
- 当前系统磁盘空间仍需关注：曾在大批更新时打满并触发 PostgreSQL 当前事务失败；清理项目缓存/日志后 `/System/Volumes/Data` 约 `390Gi/460Gi`，剩余约 `31Gi`。继续对正式 PostgreSQL 大表做更新前，仍建议保留更大空间余量；不要直接跑全量分钟表归一化。

## 2026-05-19 统一 LLM 到远端 MaaS 模型

### 本次做了什么

- 按用户要求将当前账号所有 LLM 链路统一到远端 OpenAI 兼容模型：`https://maas-coding-api.cn-huabei-1.xf-yun.com/v2` + `astron-code-latest`，不再使用本地 Ollama。
- 修正 `muyou_vip@163.com` 的资讯 LLM 覆盖配置：从 `ollama / http://127.0.0.1:11434/v1 / gpt-oss:20b` 改为 `openai / MaaS / astron-code-latest`，沿用主模型 API Key。
- 清理历史本地模型/失败的主线核心股推荐缓存，避免旧的 `Request timed out` 错误缓存继续让页面回退到新闻提取个股。
- 设置页的“资讯 LLM”默认不再指向本地 Ollama；未单独配置时会跟随上面的智能分析模型，文案也改为默认使用远端主模型。
- 主线核心股推荐 LLM 请求改为更轻量：默认每个主题只发送 3 条关键证据、每条 160 字；超时从 45 秒放宽到 120 秒。资讯解读调用超时也放宽到 120 秒。
- 重启 8500 后端，使新的超时和配置生效。

### 改动文件

- `api/services/news_theme_service.py`
- `api/services/news_eye_service.py`
- `frontend/src/pages/Settings.tsx`
- `AI_PROGRESS.md`

### 验证结果

- `.venv/bin/python -m py_compile api/services/news_theme_service.py api/services/news_eye_service.py`：通过。
- `set -a; source .env; set +a; .venv/bin/python -m pytest tests/test_news_eye_service.py tests/test_news_theme_service.py -q`：`28 passed`。
- `cd frontend && npm run build`：通过。
- 真实 8500 接口 `/v1/config` 验证：主模型与资讯模型均为 `openai / https://maas-coding-api.cn-huabei-1.xf-yun.com/v2 / astron-code-latest`，`has_api_key=true`。
- 真实 8500 接口 `/v1/news-eye/themes` 验证：`24h` 和 `premarket` 的人工智能推荐股来源均为 `llm:cache`，由远端 `openai/astron-code-latest` 回填。

## 2026-05-19 资讯之眼主线机会榜右侧卡片与推荐个股修复

### 本次做了什么

- 按用户截图要求移除“主线机会榜”右侧 `历史回溯 / Backtest Loop` 卡片，以及页面内对应的日期、周期、快照和表现回溯请求逻辑。
- 右侧 Evidence 消息保留两行预览，同时增加鼠标悬浮/键盘聚焦的全文浮层；后端证据项不再截断到 180 字，前端可展示完整新闻内容。
- 修复 Evidence 消息悬浮出现两层提示的问题：移除浏览器原生 `title` 提示，仅保留页面内自定义全文浮层。
- 收紧主线推荐个股兜底逻辑：不再把整条新闻里抓到的所有股票挂到每个主题，只保留同一主题利好语境中的个股。
- 过滤研报/来源/媒体名污染：`中邮证券/华泰证券/东方财富财经早餐` 这类来源或研报发布方不再进入 AI/金融主题推荐。
- 过滤概念词撞股票简称：如普通语境里的“智能机器人/机器人销售收入”不再误当作 `机器人` 股票推荐，除非出现代码、公告、股价等明确个股语境。
- 对 `金融街` 这类股票简称命中“金融”关键词的误归因做了排除；当前真实接口里 `金融` 主题在证据不足时返回空推荐，而不是塞入错误标的。
- 重启本地 8500 后端使新逻辑生效。

### 改动文件

- `api/services/news_theme_service.py`
- `frontend/src/pages/NewsEye.tsx`
- `tests/test_news_theme_service.py`
- `AI_PROGRESS.md`

### 验证结果

- `.venv/bin/python -m py_compile api/services/news_theme_service.py tests/test_news_theme_service.py`：通过。
- `set -a; source .env; set +a; .venv/bin/python -m pytest tests/test_news_theme_service.py -q`：`15 passed`。
- `set -a; source .env; set +a; .venv/bin/python -m pytest tests/test_news_eye_service.py tests/test_news_theme_service.py -q`：`28 passed`。
- `cd frontend && npm run build`：通过。
- 再次执行 `cd frontend && npm run build` 验证双层悬浮修复：通过。
- `git diff --check`：通过。
- 8500 后端已重启，当前监听 PID `24745`。
- 真实接口验证：`/v1/news-eye/themes?window=24h&limit=5&include_evidence=true` 中 `人工智能` 推荐仅剩 `中国移动`，不再出现 `东方财富/机器人/证券` 类污染；`金融` 推荐为空；证据内容长度包含 `986/1674` 等全文级内容。
- Safari 页面验收：`24h` 主线榜卡片正常显示；右侧不再出现 `历史回溯 / Backtest Loop`；Evidence 消息聚焦后出现全文浮层。

### 当前风险或未完成事项

- 当前 LLM 核心个股推荐仍依赖用户侧新闻分析模型配置；本地日志里仍能看到缺少模型 API key 时的异步失败，此时页面会使用更保守的利好证据兜底，不会强塞错误股票。
- `盘前/周末` 窗口在 15:00 之后会从当日 15:00 开始计算，盘后刚过时可能出现主题很少或为空；`24h/72h/7d` 不受这个窗口边界影响。

## 2026-05-19 主线机会榜消息等级口径修复

### 本次做了什么

- 按用户确认调整“主线机会榜”的消息等级展示口径。
- 后端不再把主题卡片的 `source_tier` 定义为“最高等级”，而是改为“主导等级”：按主题内去重消息的等级数量统计，数量最多的等级作为主标签，同数时取更高等级。
- 新增 `top_source_tier` 字段表示该主题内出现过的最高证据等级，用于单独提示“含 S 级证据”。
- 前端主线卡片和右侧 Evidence 详情从 `S级源` 改为 `主导X级`；当主导等级不是 S、但主题含政策 S 级证据时，额外显示 `含S级证据/含S级政策证据`。
- 摘要文案从“最高来源层级”改成“主导来源层级”，并单独保留“含S级政策催化”。
- 重启本地 8500 后端使新字段生效。

### 改动文件

- `api/services/news_theme_service.py`
- `frontend/src/pages/NewsEye.tsx`
- `frontend/src/types/index.ts`
- `tests/test_news_theme_service.py`
- `AI_PROGRESS.md`

### 验证结果

- `.venv/bin/python -m py_compile api/services/news_theme_service.py tests/test_news_theme_service.py`：通过。
- `set -a; source .env; set +a; .venv/bin/python -m pytest tests/test_news_theme_service.py -q`：`13 passed`。
- `set -a; source .env; set +a; .venv/bin/python -m pytest tests/test_news_eye_service.py tests/test_news_theme_service.py -q`：`26 passed`。
- `cd frontend && npm run build`：通过。
- `git diff --check`：通过。
- 当前 8500 后端 PID `4299` 正在监听。
- 真实接口验证：`/v1/news-eye/themes?window=premarket&limit=20&include_evidence=true` 返回 `200`，约 `0.147s`；当前前排主题显示为 `dominant=B/top=S/policy=True` 等组合，不再全量显示主标签 `S`。
- Safari 页面验收：主线机会榜卡片已显示 `政策催化`、`主导B级`、`含S级证据`，右侧详情显示 `主导 B 级`、`含S级政策证据`。

### 当前风险或未完成事项

- 当前“主导等级”是按消息条数而不是按分数权重统计；如果后续希望政策 S 级对主导等级有权重加成，可以再改成“加权主导等级”。

## 2026-05-18 资讯之眼页面显示异常与主线榜轮询卡顿修复

### 本次做了什么

- 排查用户反馈的“页面显示异常”，确认直接运行时症状是 `/v1/news-eye/themes` 在页面轮询时被同步 LLM 核心个股推荐拖住，接口曾耗时约 `1:45`，并伴随 `QueuePool limit ... connection timed out`。
- 将主线机会榜核心个股推荐改为缓存优先：命中缓存直接返回；缓存未命中时页面先返回利好/政策兜底标的，LLM 推荐放入后台异步生成缓存，不再阻塞页面请求。
- 后台 LLM 缓存任务改用独立 `SessionLocal`，避免模型调用期间占用页面请求的 DB session。
- 增加失败保护：LLM 超时/失败后写入短时错误缓存；同时增加全局单飞和失败冷却，避免新闻刷新导致后台反复启动 LLM 请求。
- 重启 8500 后端并保持 `NEWS_THEME_LLM_SYMBOLS_SYNC=0`，页面轮询使用异步推荐路径。

### 改动文件

- `api/services/news_theme_service.py`
- `tests/test_news_theme_service.py`
- `AI_PROGRESS.md`

### 验证结果

- `.venv/bin/python -m py_compile api/services/news_theme_service.py tests/test_news_theme_service.py`：通过。
- `set -a; source .env; set +a; .venv/bin/python -m pytest tests/test_news_theme_service.py -q`：`12 passed`。
- `set -a; source .env; set +a; .venv/bin/python -m pytest tests/test_news_eye_service.py tests/test_news_theme_service.py -q`：`25 passed`。
- `cd frontend && npm run build`：通过。
- `git diff --check`：通过。
- 8500 后端已重启，当前 PID `91917` 正在监听 `127.0.0.1:8500`。
- 接口实测：`/v1/news-eye/themes?window=premarket&limit=20&include_evidence=true` 返回 `200`，耗时约 `0.120s`；`/v1/news-eye/items?limit=80&offset=0` 返回 `200`，耗时约 `0.095s`。
- Safari 页面验收：`127.0.0.1:5174/news-eye` 可正常显示主线机会榜、数据源治理、最新资讯时间流；最新资讯时间显示到 `2026-05-18T18:44:50` 附近。

### 当前风险或未完成事项

- 当前本地 Ollama `gpt-oss:20b` 仍会超时；页面不会被阻塞，但 LLM 核心个股缓存会暂时退回利好/政策兜底标的。
- 若后续希望“必须等 LLM 推荐成功才展示个股”，需要先保证新闻分析模型本身可用或换成低延迟模型。

## 2026-05-18 主线机会榜改为 LLM 核心个股建议与时间流口径修复

### 本次做了什么

- 按用户反馈调整“主线机会榜”的推荐个股来源：不再主要依赖资讯正文里的个股名抽取。
- `/v1/news-eye/themes` 现在会把当前用户 ID 传入主题服务，主题服务优先使用用户的新闻分析模型/快思考模型，由 LLM 根据利好资讯和政策证据生成每个主题的核心 A 股标的。
- LLM 输出后会用本地 A 股股票库校验代码/名称，只接受 `.SH/.SZ/.BJ` 标的；非金融主题下自动过滤 `证券/期货/基金` 等研报来源型名称。
- 新增 `market_news_theme_symbol_suggestions` 缓存表，按主题证据 hash 缓存 LLM 推荐结果，避免页面 20 秒轮询时重复调用模型。
- LLM 不可用时，只用“利好/政策事件中高置信命中的相关股票”兜底，并继续过滤券商研报来源，避免乱推荐。
- 修复“市场资讯时间流”显示口径：后端资讯采集/同步时间改为北京时间写入；前端顶部状态从“最近更新”改为“最新资讯”，优先展示真实新闻发布时间，入库时间改为“采集入库”。
- 重启本地 8500 后端使接口生效。

### 改动文件

- `api/routes/news_eye.py`
- `api/services/news_eye_service.py`
- `api/services/news_theme_service.py`
- `frontend/src/pages/NewsEye.tsx`
- `tests/test_news_theme_service.py`
- `AI_PROGRESS.md`

### 验证结果

- `.venv/bin/python -m py_compile api/services/news_theme_service.py api/services/news_eye_service.py api/routes/news_eye.py`：通过。
- `set -a; source .env; set +a; .venv/bin/python -m pytest tests/test_news_theme_service.py -q`：`8 passed`。
- `set -a; source .env; set +a; .venv/bin/python -m pytest tests/test_news_eye_service.py tests/test_news_theme_service.py -q`：`21 passed`。
- `cd frontend && npm run build`：通过。
- `git diff --check`：通过。
- 服务层验证：无用户模型时 `人工智能` 仍只保留利好/政策高置信标的兜底，如 `浙海德曼`、`紫光股份`、`机器人`，不再把研报发布方混入推荐。
- 8500 后端已重启，日志显示 `News eye background worker started`，后台采集继续正常。

### 当前风险或未完成事项

- LLM 推荐需要当前登录用户已配置可用新闻分析模型或快思考模型；未配置时会退回高置信规则兜底。
- 当前 LLM 是基于本地资讯证据和模型知识生成核心 A 股标的，再用本地股票库校验；不是外部网页实时搜索。
- 若需要“强制必须 LLM、有模型才展示推荐个股”，可继续把无模型兜底改成空列表并在页面显示模型状态。

## 2026-05-18 主线机会榜推荐个股过滤修复

### 本次做了什么

- 修复资讯之眼“主线机会榜”里券商研报来源被当成推荐个股的问题。
- 场景示例：`华泰证券维持英伟达买入评级`、`华泰证券表示/指出/研报` 这类内容里，`华泰证券` 是观点来源，不是人工智能/芯片题材标的。
- 在主题相关个股汇总层增加研报来源过滤：对 `证券/期货/基金` 类名称，如果上下文是 `表示/指出/认为/研报/维持/评级/目标价/建议关注` 等研报语境，就不进入 `related_symbols` 推荐列表。
- 保留原始资讯作为主题证据，不删除新闻本身；只修正主线榜右侧推荐个股/相关标的展示。
- 重启本地 8500 后端，使页面接口生效。

### 改动文件

- `api/services/news_theme_service.py`
- `tests/test_news_theme_service.py`
- `AI_PROGRESS.md`

### 验证结果

- `.venv/bin/python -m py_compile api/services/news_theme_service.py`：通过。
- `set -a; source .env; set +a; .venv/bin/python -m pytest tests/test_news_theme_service.py -q`：`7 passed`。
- `set -a; source .env; set +a; .venv/bin/python -m pytest tests/test_news_eye_service.py tests/test_news_theme_service.py -q`：`20 passed`。
- `git diff --check`：通过。
- 正式库重算 `premarket` 主线榜后，`人工智能` 的 `related_symbols` 不再包含 `华泰证券`，保留 `奥普特`、`机器人`、`浙海德曼`、`紫光股份`、`华大基因`、`龙芯中科` 等实际命中标的。

### 当前风险或未完成事项

- 该修复只过滤研报来源型券商/基金/期货名称；如果以后还出现媒体名、机构名与 A 股简称重名，需要继续扩展上下文规则。

## 2026-05-18 资讯之眼多源采集与共享池去重修复

### 本次做了什么

- 恢复并启用资讯之眼通用采集源：`新浪7x24`、`富途快讯`、`同花顺全球直播`，与原有 `财联社电报`、`东方财富全球快讯`、`东方财富财经早餐` 一起写入共享资讯池。
- 为 `market_news_items` 增加 `dedupe_key` 内容指纹列和唯一索引，按“同日规范化资讯内容”在共享池层面去重，避免同一快讯被多个来源重复展示。
- 修复历史行 `dedupe_key` 回填和旧 `digest` 主键兼容：upsert 后用数据库实际返回的 `digest` 更新搜索索引，避免外键失败。
- 调整采集状态口径：通用源正常入库时，个股新闻源对少数股票的失败不再写成 `last_error`，避免页面误判为“采集异常”。
- 重启本地 8500 后端，使新采集逻辑生效。

### 改动文件

- `api/services/news_eye_service.py`
- `tests/test_news_eye_service.py`
- `AI_PROGRESS.md`

### 验证结果

- `.venv/bin/python -m py_compile api/services/news_eye_service.py`：通过。
- `set -a; source .env; set +a; .venv/bin/python -m pytest tests/test_news_eye_service.py tests/test_news_theme_service.py -q`：`19 passed`。
- `git diff --check`：通过。
- 正式 PostgreSQL 迁移验证：`market_news_items` 已有 `dedupe_key` 与唯一索引 `ux_market_news_items_dedupe_key`，历史共享池从 `9567` 条去重到 `9459` 条。
- 手动真实刷新验证：`active_sources=["财联社电报","东方财富全球快讯","东方财富财经早餐","新浪7x24","富途快讯","同花顺全球直播"]`，`saved=80`，`fallback=false`，`message="资讯刷新完成（manual-verification）"`。
- 后端重启后后台 worker 验证：`status=success`、`last_error=null`、`saved_count=120`，共享池 `total=9506` 且 `COUNT(DISTINCT dedupe_key)=9506`。

### 当前风险或未完成事项

- 个股源里部分旧自选/定时股票仍会出现 `东方财富个股新闻(... ) 拉取失败` 警告，但已不影响通用资讯源入库和页面主状态。
- 当前去重粒度是“同日规范化内容指纹”，不同日期重复发布的相同文本仍会保留，避免误删跨日复盘类资讯。

## 2026-05-16 v3 买卖点差异策略信号回放诊断

### 本次做了什么

- 新增 `scripts/diagnose_v3_strategy_execution.py`，把 v3 Excel 和新生成 CSV 的每笔交易回放到当前平台首日波段信号。
- 校验口径：买入日前一可交易日必须有 `first_day_band_cross`，已平仓卖出日前一可交易日必须有 `first_day_band_dead_cross`，成交价必须等于当前平台成交日收盘价。
- 生成策略信号诊断报告和明细，并更新 `首日波段回测与全市场波段交易v3差异校验.md`。

### 生成文件

- `data/artifacts/backtests/first_day_band_v3_aligned_20240101_20260430_20260516022712/v3_strategy_execution_diagnosis_report.md`
- `data/artifacts/backtests/first_day_band_v3_aligned_20240101_20260430_20260516022712/v3_sequence_strategy_execution_diagnosis.csv`
- `data/artifacts/backtests/first_day_band_v3_aligned_20240101_20260430_20260516022712/v3_unmatched_strategy_execution_diagnosis.csv`
- `data/artifacts/backtests/first_day_band_v3_aligned_20240101_20260430_20260516022712/v3_strategy_signal_validation_all.csv`
- `data/artifacts/backtests/first_day_band_v3_aligned_20240101_20260430_20260516022712/generated_strategy_signal_validation_all.csv`
- `data/artifacts/backtests/first_day_band_v3_aligned_20240101_20260430_20260516022712/v3_strategy_execution_diagnosis_summary.json`

### 校验结果

- 当前正式平台数据源：`duckdb:parquet:market_stock_daily_kline`，指标后端：`polars`。
- 新生成 CSV `260,938` 笔全部被当前平台买入金叉、卖出死叉和成交日收盘价支持。
- v3 `261,212` 笔中 `254,730` 笔被当前平台信号和价格支持，`6,482` 笔不被支持。
- 新生成结果确实漏了 `724` 笔起始边界交易：这些 v3 交易由 `2023-12-29` 金叉触发、`2024-01-02` 买入；当前脚本裁剪到 `2024-01-01` 后才模拟，没有携带待执行买单。
- v3 有部分上市未满 `250` 天的新股交易；当前首日波段 DSL 继承 `min_listing_days=250`，若要以 v3 为目标口径，应在 v3 对齐脚本中改为 `0`。
- 2026-04 末尾差异主要指向 v3 底层日 K 缺 `2026-04-20`、`2026-04-21`、`2026-04-24` 等已补齐交易日：同买卖时间持仓天数差集中在 `2026-04-23` 后，差值多为 `2` 或 `3` 天。

## 2026-05-16 v3 与新结果逐笔买卖时间/收益差异标记

### 本次做了什么

- 新增 `scripts/compare_v3_trade_details.py`，对 `/Users/wolf/Documents/DaiMa/strategy-backtest/全市场回测v3_波段交易.xlsx` 与 v3 对齐新结果逐笔比对。
- 按两种口径标记差异：严格同 `股票名称+买入时间+卖出时间+状态+重复序号`，以及同股票下第 N 笔交易对齐。
- 更新 `首日波段回测与全市场波段交易v3差异校验.md`，补充逐笔差异结论。

### 生成文件

- `data/artifacts/backtests/first_day_band_v3_aligned_20240101_20260430_20260516022712/v3_exact_time_return_differences.csv`
- `data/artifacts/backtests/first_day_band_v3_aligned_20240101_20260430_20260516022712/v3_exact_time_unmatched_trades.csv`
- `data/artifacts/backtests/first_day_band_v3_aligned_20240101_20260430_20260516022712/v3_sequence_aligned_differences.csv`
- `data/artifacts/backtests/first_day_band_v3_aligned_20240101_20260430_20260516022712/v3_trade_diff_summary_by_stock.csv`
- `data/artifacts/backtests/first_day_band_v3_aligned_20240101_20260430_20260516022712/v3_trade_diff_summary.json`
- `data/artifacts/backtests/first_day_band_v3_aligned_20240101_20260430_20260516022712/v3_trade_diff_report.md`

### 校验结果

- v3 `261,212` 笔，新结果 `260,938` 笔，股票名称数均为 `5,205`。
- 严格同买卖时间匹配 `253,757` 笔；其中 `732` 笔存在数值字段差异。
- 严格同买卖时间下买入价、卖出价、净收益没有超过容差的差异；`732` 笔主要是收益率 `0.01` 个百分点四舍五入差 `510` 笔、持仓天数口径差 `223` 笔。
- 严格时间键 v3 独有 `7,455` 笔，新结果独有 `7,181` 笔；这 `14,636` 条是后续需要继续追的数据/信号时间差异。
- 同股票第 N 笔对齐差异 `55,317` 行，该指标会被前序多/少一笔放大，主要用于定位错位开始点。
- `/Users/wolf/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 -m py_compile scripts/compare_v3_trade_details.py`：通过。

## 2026-05-16 全市场回测 v3 差异校验与上市日期元数据修复

### 本次做了什么

- 对比 `/Users/wolf/Documents/DaiMa/strategy-backtest/全市场回测v3_波段交易.xlsx` 与当前固定 1000 股 CSV，确认 v3 为 `261212` 笔，当前 CSV 为 `194989` 笔。
- 复核用户指出的“平安银行 v3 50 笔、当前 CSV 24 笔且从 2025-06 才开始”属实。
- 定位根因：Parquet/DB 同一股票存在 `000001` 与 `000001.SZ` 等重复写法，上市日期元数据先按原始 symbol 求最早日期再归一化，导致部分老股票最终保留了 `2024-09-04` 这类错误上市日；`min_listing_days=250` 又把 2024 至 2025 上半年的行过滤掉。
- 修复上市日期元数据归一化去重，并让旧 symbol metadata cache 通过 `metadata_version=2` 自动失效重建。
- 复核 v3 成交价口径：虽然汇总写 T+1，但样本价格对应 T+1 收盘价；当前 CSV 用的是 T+1 开盘价，收益口径不完全一致。
- 新增 `scripts/run_first_day_band_v3_aligned_backtest.py`，按 v3 股票名称池、`2026-04-30` 截止、T+1 收盘价、固定 1000 股、买入 `0.13%`/卖出 `0.23%` 费用重跑。
- v3 的 `5205` 个股票名称全部完成代码映射：当前名称优先用 AkShare，退市/改名名称用 v3 日期价格指纹反查平台行情。

### 改动/生成文件

- `api/services/strategy_platform_engine.py`
- `tests/test_strategy_platform_true_engine.py`
- `首日波段回测与全市场波段交易v3差异校验.md`
- `scripts/run_first_day_band_v3_aligned_backtest.py`
- `data/artifacts/backtests/first_day_band_v3_aligned_20240101_20260430_20260516022712/holding_trade_details.csv`
- `data/artifacts/backtests/first_day_band_v3_aligned_20240101_20260430_20260516022712/closed_trade_details.csv`
- `data/artifacts/backtests/first_day_band_v3_aligned_20240101_20260430_20260516022712/open_positions.csv`
- `data/artifacts/backtests/first_day_band_v3_aligned_20240101_20260430_20260516022712/v3_name_mapping.csv`
- `data/artifacts/backtests/first_day_band_v3_aligned_20240101_20260430_20260516022712/comparison_summary.json`
- `data/artifacts/backtests/first_day_band_v3_aligned_20240101_20260430_20260516022712/comparison_by_name.csv`
- `data/artifacts/market_cache/universe_metadata/symbol_metadata.json`
- `data/artifacts/market_cache/universe_metadata/symbol_metadata.parquet`

### 验证结果

- 修复前元数据样本：`000001.SZ` 同时出现 `1991-01-03` 与 `2024-09-04`，最终 metadata 错留 `2024-09-04`。
- 修复后元数据样本：`000001.SZ listing_date=1991-01-03`、`000026.SZ listing_date=1993-06-03`，重复上市日期行数为 `0`。
- 修复后平安银行通过 universe 过滤的日期范围恢复为 `2024-01-02` 至 `2026-05-14`，早期 `2024-01-05` 金叉信号恢复。
- 修复后全市场只计数复核：跑到 `2026-05-14` 买入 `274747`、卖出 `271304`、未平仓 `3443`；对齐 v3 结束日 `2026-04-30` 买入 `271937`、卖出 `269610`、未平仓 `2327`。
- v3 对齐重跑结果：总记录 `260938`，比 v3 `261212` 少 `274`；已平仓 `258659`，未平仓 `2279`；`股票名称+买入日+卖出日+状态` 精确交集 `253757`。
- 样本：平安银行 v3 `50` 笔、新结果 `51` 笔，第一买入日均为 `2024-01-08`；飞亚达 v3 `50` 笔、新结果 `50` 笔，第一买入日均为 `2024-01-03`。
- 剩余差异集中在 `2026-04`：新结果 `9413` 笔，v3 `6384` 笔；平台行情在 `2026-04-29` 出现大量金叉，导致 `2026-04-30` 买入未平仓较多，而 v3 同日只记录 `83` 笔买入。
- `.venv/bin/python -m py_compile api/services/strategy_platform_engine.py`：通过。
- `set -a; source .env; set +a; .venv/bin/python -m pytest tests/test_strategy_platform_true_engine.py -q`：`23 passed`。
- `git diff --check`：通过。

### 当前风险或未完成事项

- 当前 `first_day_band_fixed1000_20240101_20260514_20260515225826` CSV 已确认少单，不应继续作为最终结果引用。
- 新的 v3 对齐结果已可用于替代旧 CSV；若要完全复刻 v3 数字，还需要继续核对 v3 的底层日 K 数据源及末尾信号处理规则。

## 2026-05-15 首日波段固定 1000 股回测与死叉退出修正

### 本次做了什么

- 复核用户指出的飞亚达案例，确认 `2024-01-02` 首日波段金叉、`2024-01-08` 死叉，正确成交应为 `2024-01-03` 买入、`2024-01-09` 卖出。
- 修正策略平台回测引擎里首日波段 `cross_below` 退出判断，避免把 `first_day_band < first_day_band_b1` 的普通状态误当作死叉事件。
- 重新按“每只股票固定买入 1000 股，死叉全卖”的口径跑全市场回测，不再使用组合仓位比例、最大 20 只候选等资金分配约束。
- 对比 `/Users/wolf/Documents/DaiMa/strategy-backtest/全市场回测_波段交易.xlsx`，确认该 Excel 的“波段交易”实际是 MA5/MA20 均线金叉死叉、信号日收盘价成交，不能直接作为首日波段 `first_day_band/B1` 的基准。

### 改动/生成文件

- `api/services/strategy_platform_engine.py`
- `tests/test_strategy_platform_true_engine.py`
- `data/artifacts/backtests/first_day_band_fixed1000_20240101_20260514_20260515225826/holding_trade_details.csv`
- `data/artifacts/backtests/first_day_band_fixed1000_20240101_20260514_20260515225826/closed_trade_details.csv`
- `data/artifacts/backtests/first_day_band_fixed1000_20240101_20260514_20260515225826/open_positions.csv`
- `data/artifacts/backtests/first_day_band_fixed1000_20240101_20260514_20260515225826/holding_trade_report.md`
- `首日波段回测与全市场波段交易差异校验.md`

### 验证结果

- 飞亚达样本核对：`2024-01-02` 买入信号，`2024-01-03` 买入 1000 股；`2024-01-08` 卖出信号，`2024-01-09` 全卖。
- 固定 1000 股回测区间：`2024-01-01` 至最新日 K `2026-05-14`，数据源 `duckdb:parquet:market_stock_daily_kline`。
- 回测输出：买入成交 `194989` 笔、卖出成交 `191633` 笔、未平仓 `3356` 笔、峰值持仓市值约 `119,923,940`。
- Excel 对照复核：Excel 总记录 `82018`，成本单边约 `0.035%`，最新标记日期 `2026-04-30`；按 MA5/MA20 在当前行情库复算并限定到 Excel 名称交集后为 `81196` 条，关键样本平安银行、飞亚达买卖日期和价格能对上。
- `set -a; source .env; set +a; .venv/bin/python -m pytest tests/test_strategy_platform_true_engine.py -q`：`22 passed`

### 当前风险或未完成事项

- 固定 1000 股口径为“全信号执行、现金不约束”的策略检验口径，现金会显著为负；如要模拟真实账户，需要另加资金上限、排队规则或信号筛选。
- 未平仓记录按最新收盘价做浮动收益展示，未强制按最新日平仓。

## 2026-05-15 最终代码收口与远端推送

### 本次做了什么

- 收口当前 `main` 工作区累计改动，准备提交并推送到远端。
- 本次提交覆盖近期资讯之眼主线机会榜、设置页日 K 覆盖日历、量化小课堂补数、策略回测引擎/仓储、报告/分析页接口与前端类型等改动。
- 继续按根目录交接约定更新本进度文件，方便后续对话从当前提交继续。

### 改动文件

- 后端：`api/backtest_data_api.py`、`api/routes/news_eye.py`、`api/routes/strategy_platform.py`、`api/services/news_theme_service.py`、`api/services/strategy_platform_engine.py` 等。
- 前端：`frontend/src/pages/NewsEye.tsx`、`frontend/src/pages/Settings.tsx`、`frontend/src/pages/Analysis.tsx`、`frontend/src/pages/Reports.tsx`、`frontend/src/services/api.ts`、`frontend/src/types/index.ts`。
- 测试：`tests/test_backtest_data_api_calendar.py`、`tests/test_news_theme_service.py`、`tests/test_strategy_platform_repository.py`、`tests/test_strategy_platform_true_engine.py`、`tests/test_realtime_monitor.py`。
- 数据/文档：`data/quantclass/2026-04-20.csv`、`data/quantclass/2026-04-21.csv`、`data/quantclass/2026-04-24.csv`、`data/quantclass/2026-05-07.csv` 至 `2026-05-14.csv`、`项目数据来源与调用机制梳理.md`。

### 验证结果

- `git diff --check`
- `python -m py_compile api/backtest_data_api.py api/routes/news_eye.py api/schemas/news_eye.py api/services/news_theme_service.py api/services/news_eye_service.py api/services/strategy_platform_engine.py api/services/strategy_platform_repository.py`
- `set -a; source .env; set +a; pytest tests/test_backtest_data_api_calendar.py tests/test_news_theme_service.py tests/test_news_eye_service.py -q`：`18 passed`
- `pytest tests/test_strategy_platform_repository.py tests/test_strategy_platform_true_engine.py tests/test_realtime_monitor.py -q`：`50 passed`
- `cd frontend && npm run build`

### 当前风险或未完成事项

- 本次是累计改动收口，没有额外跑全量测试套件。
- 量化小课堂 CSV 为正式补数产物，后续如果继续自动下载，需要留意数据目录大小和是否需要归档策略。

### 下一步建议

- 后续继续从 `main` 最新提交开始开发。
- 若要支持普通微信群推送，建议另起 Windows 专用微信号 + `wxauto` relay，避免在主力 Mac 微信上做 GUI 自动化。

## 2026-05-14 日 K 缺口检查与量化小课堂补数

### 本次做了什么

- 按 A 股交易日历核对正式 PostgreSQL 日 K 数据，检查 `stock_daily_kline`、`raw_stock_daily_kline_quantclass`、`norm_stock_daily_kline`、`pub_stock_daily_kline`。
- 发现可用日 K 覆盖中近期缺失 3 个交易日：`2026-04-20`、`2026-04-21`、`2026-04-24`。
- 使用量化小课堂 `stock-trading-data-pro` 按指定日期下载并导入这 3 天数据。
- 导入后刷新 `data/artifacts/market_cache/daily_kline/daily_kline_2026.parquet`。
- 重新核对设置页日历接口，2026 年有数据天数从 `81` 天变为 `84` 天。
- 继续核查旧主表 `stock_daily_kline`，发现旧表缺 `2026-04-20`、`2026-04-21`、`2026-04-24` 和 `2026-05-06` 至 `2026-05-14` 共 10 个增量交易日；已从 `pub_stock_daily_kline` 镜像回旧表。

### 改动/生成文件

- `data/quantclass/2026-04-20.csv`
- `data/quantclass/2026-04-21.csv`
- `data/quantclass/2026-04-24.csv`
- `data/artifacts/market_cache/daily_kline/daily_kline_2026.parquet`
- PostgreSQL `stock_daily_kline` 旧主表新增/更新 `54928` 行，日期覆盖到 `2026-05-14`。

### 验证结果

- `2026-04-20` 导入 `5493` 行 / `5493` 只股票。
- `2026-04-21` 导入 `5498` 行 / `5498` 只股票。
- `2026-04-24` 导入 `5496` 行 / `5496` 只股票。
- 补数后 `raw_stock_daily_kline_quantclass`、`norm_stock_daily_kline`、`pub_stock_daily_kline` 三层均能查到上述 3 天数据。
- 近期交易日 `2026-04-14` 至 `2026-05-14` 复核结果：`MISSING_RECENT=[]`、`PARTIAL_RECENT=[]`。
- 认证请求 `GET /v1/backtest-data/daily-kline/coverage-calendar?year=2026` 返回约 `1.473s`，三天均 `has_data=true`，且 `is_trading_day=true`。
- 旧主表补齐后复核：`stock_daily_kline` 总行数 `17667600`，覆盖 `8681` 个交易日，日期范围 `1990-12-19` 至 `2026-05-14`；`2026-04-14` 至 `2026-05-14` 复核 `OLD_MISSING_RECENT=[]`、`OLD_PARTIAL_RECENT=[]`。

### 当前风险或未完成事项

- 当前代码默认 `MARKET_DATA_WRITE_LEGACY_TABLES=0`，后续增量仍会优先写 raw/norm/pub 发布链路；如果希望以后旧主表也同步写入，需要显式打开兼容镜像或改默认策略。
- 本次只补了实际缺失交易日和旧表近期缺口，没有重刷整个历史库。

### 下一步建议

- 后续若设置页要更直观，可在“已下载数据”中区分旧主表、发布层、统一视图的覆盖状态，避免看到旧主表滞后时误判缺数据。
- 可以把“交易日历对比 + 指定日期量化小课堂补数”沉淀成一个后台运维按钮或脚本。

## 2026-05-14 设置页日 K 覆盖日历超时修复

### 本次做了什么

- 修复设置页“回测数据 / 已下载数据 / 股票日 K 线数据视图”请求 `coverage-calendar?year=2026` 超过 15 秒的问题。
- 根因是接口读取 `preferred_daily_kline_table()` 后落到 `market_stock_daily_kline` 统一视图，视图会做跨表 `UNION` 和去重，`MIN/MAX(trade_date)` 与按日统计在正式 PostgreSQL 上容易超时。
- 日历接口改为优先读取轻量物理表 `stock_daily_kline`、`pub_stock_daily_kline`，只在没有物理表可用时兜底到首选表。
- 接口响应新增 `source_tables`，方便页面/调试知道当前覆盖日历来自哪些表。
- 新增针对性单测，防止后续把日历统计重新改回慢视图。
- 股票日 K 线数据视图补充休息日展示：后端每日对象新增 `is_rest_day/is_trading_day`，前端日期格右上角显示红色“休”角标。

### 改动文件

- `api/backtest_data_api.py`
- `frontend/src/pages/Settings.tsx`
- `tests/test_backtest_data_api_calendar.py`

### 验证结果

- `python -m py_compile api/backtest_data_api.py`
- `pytest tests/test_backtest_data_api_calendar.py -q`
- `cd frontend && npm run build`
- 正式后端 `127.0.0.1:8500`、前端代理 `127.0.0.1:5174` 均在运行。
- 认证后请求 `http://127.0.0.1:5174/v1/backtest-data/daily-kline/coverage-calendar?year=2026` 连续 3 次返回 `200 OK`，耗时约 `4.141s / 1.441s / 2.202s`。
- 返回结果显示 `year=2026`、`total_days_with_data=81`、`source_tables=['stock_daily_kline', 'pub_stock_daily_kline']`。
- 休息日字段验证：认证后请求同一接口返回 `is_rest_day/is_trading_day`，2026 年休息日计数为 `123`，例如 `2026-01-01` 返回 `is_rest_day=true`。

### 当前风险或未完成事项

- 未跑全量回归；此前尝试跑较重的 `tests/test_market_data_pipeline_service.py` 会卡在正式库初始化/查询路径，不适合作为这个小修复的快速验证。
- 如果未来 `stock_daily_kline` 或 `pub_stock_daily_kline` 数据量继续增大，建议为 `(trade_date, symbol)` 补充/确认索引，进一步压低冷启动查询耗时。

### 下一步建议

- 前端可在日历视图中展示 `source_tables` 或数据源提示，便于以后定位数据口径。
- 后续可给设置页统计接口增加更细的慢查询日志，超过阈值时输出表名、年份和耗时。

## 2026-05-10 资讯掘金主线机会榜

### 本次做了什么

- 在“资讯之眼”增加主线机会榜能力：标准主题映射、来源分层、政策催化、非线性共振评分、共识率/分歧提示、拥挤风险、证据消息和历史回溯。
- 新增后端 `news_theme_service`，从 `market_news_items` 和索引表生成主题快照，并提供后续 1/3/5 日板块表现回溯。
- 新闻刷新后会尝试同步刷新 `premarket`、`24h`、`72h`、`7d` 四个窗口的主线快照；失败只记录日志，不阻断资讯入库。
- 前端 `/news-eye` 顶部新增“主线机会榜”，支持时间窗口切换、点击主题筛选资讯流、证据展开和历史表现查看。
- 发现旧 `8500` 后端进程未加载新增接口导致页面无主线数据，已重启 `ta-backend` screen，并重算主线快照。

### 改动文件

- `api/services/news_theme_service.py`
- `api/routes/news_eye.py`
- `api/schemas/news_eye.py`
- `api/services/news_eye_service.py`
- `api/services/__init__.py`
- `frontend/src/pages/NewsEye.tsx`
- `frontend/src/services/api.ts`
- `frontend/src/types/index.ts`
- `tests/test_news_theme_service.py`

### 验证结果

- `pytest tests/test_news_theme_service.py tests/test_news_eye_service.py -q`
- `python -m py_compile api/services/news_theme_service.py api/routes/news_eye.py api/schemas/news_eye.py api/services/news_eye_service.py`
- `cd frontend && npm run build`
- `git diff --check`
- 临时启动后端 `127.0.0.1:8501`，确认新路由 `/v1/news-eye/themes` 已加载；未登录访问按预期返回 401。
- 正式后端 `127.0.0.1:8500` 已重启并加载新接口；后台日志显示登录态请求 `/v1/news-eye/themes` 返回 `200 OK`。

### 当前风险或未完成事项

- 第一版标准主题库为内置别名表，后续还需要接入同花顺/通达信概念库或本地股票池概念表。
- 历史表现第一版按 `sw_industry_l1` 聚合，概念主题如“算力”如果没有对应行业字段，可能没有表现数据。
- LLM 榜单摘要暂未自动调用模型，当前摘要和风险提示先由规则生成。

### 下一步建议

- 补充更完整的概念映射源，并把主题到成分股/板块指数的映射做成可维护配置。
- 增加评分参数的后台配置或实验记录，用历史回溯持续调新鲜度、来源层级和分歧因子权重。

## 2026-05-08 Git 收口与远端推送

### 本次做了什么

- 准备提交当前工作区全部改动。
- 将 `default-sqlite` 当前版本合并到 `main`。
- 推送到项目当前目标远端：`all-seeing-eye` 与 `quanzhizhiyan`。

### 改动文件

- 本次提交包含近期 QMT、实时监控、行情数据、设置页、股票市场、资讯之眼、文档交接文件等累计改动。
- 具体文件以本次 Git commit diff 为准。

### 验证结果

- 已在提交前完成相关验证：
  - `npm run build`
  - `pytest tests/test_virtual_warehouse.py tests/test_qmt_sync_scheduler_service.py tests/test_realtime_monitor.py -q`
  - `python -m py_compile api/services/qmt_virtual_account_service.py api/services/data_source_governance.py`

### 下一步建议

- 推送后如果继续开发，先从 `main` 拉取最新版本。
- 后续每次功能收口继续更新本文件顶部记录。

## 2026-05-08 交接文档初始化

### 本次做了什么

- 扩展根目录 `README.md`，补充项目定位、核心模块、运行方式、数据与后台任务、验证方式和安全边界。
- 新增 `AI_PROGRESS.md`，作为每次 AI 收尾时必须更新的进度文件。
- 新增 `AI_RULES.md`，作为长期有效的协作规则文件。

### 改动文件

- `README.md`
- `AI_PROGRESS.md`
- `AI_RULES.md`

### 验证结果

- 文档类改动，未运行代码测试。
- 已确认根目录存在现有 `产品文档.md`、`项目性能与功能拓展分析.md`、多源治理和 QMT 相关文档，可作为后续深读材料。

### 当前状态摘要

- 项目使用 PostgreSQL，不再按 SQLite 口径维护。
- QMT 账户配置应走设置页/数据库，不应写死在 `.env`。
- QMT 实盘仓和虚拟仓页面已区分“实时直连 / 后台在线 / 快照可用 / 未连接”，避免页面切走后误显示失联。
- 资讯之眼和股票市场接口近期已修复为按当前用户读取数据源与 QMT bridge 配置。
- 后端当前常用端口 `8500`，前端常用端口 `5174`。

### 下一步建议

- 后续任何代码任务结束时，追加或更新本文件顶部条目。
- 如果发生重要架构、数据口径、QMT 安全边界变化，同时更新 `README.md` 和 `AI_RULES.md`。
- 下一阶段继续收紧：产品口径统一、实盘安全边界、数据可信度展示、策略可解释性。
