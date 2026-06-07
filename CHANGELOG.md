# Changelog

All notable changes to TradingAgents-AShare.

## [0.3.0] - 2026-06-07

### Added
- **1 分钟 K 线自动补齐**：新增 `scripts/fill_minute_kline_gaps.py` 与 `api/services/minute_kline_gap_filler_service.py`，通过通达信 pytdx 在收盘后/盘前自动扫描并补齐 stock_minute_kline 缺口，通过 `ENABLE_MINUTE_KLINE_GAP_FILLER_WORKER=1` 启用。
- **版本查询接口**：`GET /healthz` 现在返回 `version`、`commit`、`build_date`；新增 `GET /v1/version` 端点。
- **已下载数据列表**：新增 `index_daily_kline`（指数日K线）和 `index_minute_kline`（指数1分钟K线）的中文显示名与图标。

### Changed
- 版本号从 `0.2.0` 升至 `0.3.0`。
- `index_daily_kline` 表纳入回测数据统计视图。

### Fixed
- 修复 `fill_minute_kline_gaps.py` 中 end_date 过滤边界问题（datetime 比较改为 `< end_dt + 1 day`）。
- 修复北交所 (BJ) 股票分钟线补齐时 market code 错误（TDX market=2）。

## [0.2.0] - 2026-05-27

### Added
- 日复盘技术诊断服务
- 新闻主题与数据覆盖更新
- QMT 工作流加固与数据治理
- 实时分钟线补充与再入场锚点
- 新闻眼源扩展与后台同步

### Changed
- 移除 SQLite 支持，统一使用 PostgreSQL
- 策略管理数据库更新
- API 路由和日志管理优化

### Fixed
- 协作工作流状态追踪
- 分析工作流与决策输出对齐
