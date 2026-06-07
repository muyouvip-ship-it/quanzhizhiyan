# AI量化闭环最终版使用说明

更新日期：2026-06-03

本文说明当前“量化之神”最终版 AI 量化闭环的使用方式。该版本的核心目标是把主动发现机会、理解新闻/政策事件、判断市场状态、动态排序标的、控制风险、结果反哺模型连成闭环。系统用于研究、复盘和监控，不构成直接买卖建议。

## 1. 访问入口

本地常用地址：

- 前端：`http://127.0.0.1:5174/`
- 后端：`http://127.0.0.1:8500/`
- 主页面：`http://127.0.0.1:5174/catalyst-selection`

当前最终版运行态应满足：

- 后端 `8500` 健康。
- 前端 `5174` 可访问。
- 催化选股页显示“AI量化闭环状态”。
- “主动触发”显示 `event_driven`。
- “端到端”显示 `active`，通过率接近或等于 `100%`。
- “远程LLM就绪”显示实际远端模型。
- “AI监控池”显示观察池、门禁分布和风险配置。

## 2. 启动与健康检查

### 后端

推荐使用项目当前方式在 `8500` 启动：

```bash
screen -dmS ta_backend_8500 uvicorn api.app:app --host 127.0.0.1 --port 8500
```

检查后端：

```bash
curl -sS http://127.0.0.1:8500/healthz
lsof -nP -iTCP:8500 -sTCP:LISTEN
```

正常结果：

```json
{"status":"ok"}
```

### 前端

```bash
cd frontend
npm run dev
```

检查前端：

```bash
lsof -nP -iTCP:5174 -sTCP:LISTEN
```

## 3. 必要配置

### LLM 配置

进入 `设置` 页面：

```text
http://127.0.0.1:5174/settings
```

确认：

- 主模型和资讯模型均使用远端 LLM。
- 当前账号的 LLM Runtime 可用。
- 不使用本地 Ollama 或本地模型。
- 页面不要明文展示或外传 API Key。

当前闭环验证过的运行模型口径：

- Provider：`volcengine-ark`
- Model：`deepseek-v4-flash`
- Runtime 来源：用户运行时配置

### 数据要求

闭环依赖以下数据：

- `market_news_items`：资讯和政策事件。
- `stock_daily_kline`：日 K 最终业务表。
- `stock_minute_kline`：分钟 K 最终业务表。
- 市场状态接口：用于成交额、涨跌家数、板块和资金流解释。
- 实时监控事件与反馈表：用于结果反哺。

如果页面提示市场状态缺失、分钟代理偏弱、事件反应不足，先检查行情同步和资讯采集。

## 4. 标准使用流程

### 第一步：看资讯之眼

进入：

```text
/news-eye
```

关注：

- 最新资讯时间是否正常。
- 资讯来源是否覆盖多源。
- 主题榜是否有重复或异常主题。
- 事件是否进入资讯池。

资讯之眼触发新事件后，会推动主线机会榜进入事件驱动刷新。

### 第二步：打开催化选股

进入：

```text
/catalyst-selection
```

常用窗口：

- `实时24h`：滚动资讯机会。
- `盘前`：09:25 前资讯。
- `72h`：主线延续。
- `7日`：中期热度。

优先看页面顶部：

- 当前窗口。
- 资讯时间范围。
- 市场基准日期。
- 市场状态新鲜度。
- 入选主线。

如果看到“入选主线：算力”“入选主线：医药”等，说明页面展示的是最终入选候选的占优主线，不再使用旧的第一主题硬套逻辑。

### 第三步：检查 AI量化闭环状态

在“AI量化闭环状态”区域检查六个阶段：

- 主动发现机会。
- 理解新闻/政策事件。
- 判断市场状态。
- 动态排序标的。
- 控制风险。
- 结果反哺模型。

理想状态：

```text
端到端 active · 通过 100%
主动触发 event_driven
远程LLM就绪
```

关键字段解释：

- `主动触发 event_driven`：机会来自资讯/事件驱动，而不是被动指标触发。
- `新事件 x/x`：本次触发纳入闭环的新事件数量。
- `入库 x/x`：资讯采集新入库和保存数量。
- `标的/语义`：LLM 对核心股和事件语义的使用次数。
- `反馈 x/x`：选中标的有多少命中过历史或实时反馈画像。
- `学习影响`：反馈学习对分数、名次、风控动作的影响。

### 第四步：看推荐标的

每个候选标的应检查：

- 排名和 score。
- 主线主题。
- 事件类型。
- LLM 事件理解。
- 市场确认。
- 分钟反应。
- 自适应反馈分。
- 风险门禁。

重点不是只看分数高低，而是看“为什么入选”和“是否允许执行”。

### 第五步：看 AI监控池

在催化选股页下方查看“AI监控池”。

它会把事件候选转成实时监控观察池：

- `watch_symbols`：需要持续观察的标的。
- `tradable_symbols`：门禁允许进入交易候选的标的。
- `entry_symbols`：可开仓或试探。
- `confirm_symbols`：等待确认。
- `blocked_symbols`：阻断。
- `reduce_only_symbols`：只减不加。
- `gate_counts`：门禁分布。

当前默认执行模式为：

```text
monitor_only
```

也就是只监控，不自动下单。交易动作由门禁控制。

### 第六步：进入实时监控

进入：

```text
/realtime
```

确认 AI监控池已创建或更新为 `running`。

关注：

- 监控实例状态。
- 最新 cycle。
- 分钟特征。
- signal 事件。
- 风控事件。
- 人工审批。
- 是否有重复触发。

实时监控应在前端页面未打开时仍由后端 worker 执行。

### 第七步：结算与反馈学习

在催化选股页使用“结算”。

结算后系统会：

- 生成选股结果。
- 更新主题、事件类型、风险门禁、分钟脉冲等反馈画像。
- 生成 learning impact。
- 影响后续 score、排序和风控门禁。

在“最近闭环审计”里检查：

- `闭环 6/6`。
- `端到端 已运行`。
- `反馈学习 已运行`。
- `实时反馈样本`。
- `结算画像`。
- `反哺回放命中`。

## 5. 日常使用节奏

### 盘前

1. 打开 `/news-eye`，确认盘前资讯采集正常。
2. 打开 `/catalyst-selection`，选择“盘前”。
3. 查看入选主线、候选标的和 AI量化闭环状态。
4. 查看 AI监控池，确认观察池和门禁分布。
5. 进入 `/realtime`，确认监控 running。

### 盘中

1. 查看“实时24h”窗口。
2. 关注事件池分钟脉冲和分钟市场代理。
3. 看风控门禁是否从 `blocked` 变成 `confirm`、`allow_probe` 或 `allow`。
4. 不直接追后排扩散，优先看核心标的承接。
5. 若仍为 `monitor_only`，系统只做监控和记录，不自动下单。

### 收盘后

1. 对当日选股运行结算。
2. 查看闭环审计。
3. 查看反馈学习样本是否更新。
4. 复盘学习影响：分数、名次、风险是否被反馈修正。
5. 检查次日候选主题和监控池。

## 6. 常用接口

以下接口需要登录 token。本地开发可使用 dev token：

```bash
AUTH='Authorization: Bearer dev-test-token-001'
```

### 催化选股

```bash
curl -sS -H "$AUTH" 'http://127.0.0.1:8500/v1/catalyst-selection?window=24h&limit=10'
```

### 强制重算

```bash
curl -sS -H "$AUTH" -X POST \
  'http://127.0.0.1:8500/v1/catalyst-selection/generate?window=24h&limit=10' \
  -H 'Content-Type: application/json' \
  -d '{"trade_date":"2026-06-03","force":true}'
```

### AI监控池

```bash
curl -sS -H "$AUTH" 'http://127.0.0.1:8500/v1/catalyst-selection/monitor-pool?window=24h&limit=10'
```

### 闭环审计

```bash
curl -sS -H "$AUTH" 'http://127.0.0.1:8500/v1/catalyst-selection/closed-loop-audits?limit=5'
```

### 事件刷新记录

```bash
curl -sS -H "$AUTH" 'http://127.0.0.1:8500/v1/catalyst-selection/event-refresh-runs?limit=10'
```

### 学习回放

```bash
curl -sS -H "$AUTH" 'http://127.0.0.1:8500/v1/catalyst-selection/learning-replay?limit=20'
```

## 7. 正常状态参考

最终版正常时，接口或页面应能看到类似结果：

```text
e2e_status = active
pass_rate = 1.0
discovery_mode = event_driven
trigger = news-eye:background
requirement_summary.active_count = 6
monitor_activation.updated_running = 2
suggested_execution_mode = monitor_only
gate_counts = {"blocked": 5}
```

实际 `gate_counts` 会随行情和风险反馈变化，不要求永远是 blocked。它表示当前风险门禁判断。

## 8. 异常处理

### 页面显示“等待窗口数据”

可能原因：

- 后端正在生成。
- 缓存过期，后台刷新中。
- 当前窗口没有可用资讯或行情。

处理：

- 等待几秒刷新页面。
- 查看“事件驱动刷新”区域。
- 检查 `/closed-loop-audits` 和 `/event-refresh-runs`。

### 没有远程 LLM 就绪

处理：

- 进入 `/settings`。
- 检查当前账号 LLM 配置。
- 确认 provider、base URL、model 和 key 属于同一套配置。
- 不要只填 key，不填模型和 base URL。

### 推荐标的和主线不一致

处理：

- 看“入选主线”，它应来自最终入选候选，而不是主题榜第一名。
- 看每个候选的 `theme_matches` 和 `reason_parts`。
- 检查是否使用 fallback：`fallback:positive_news` 表示 LLM 推荐失败或无结果。
- 检查 `mainline_alignment_reasons`。

### 全是 blocked

这不是错误。说明系统认为当前事件或市场承接不足，只允许观察。

重点看：

- 分钟反应是否 weak。
- 市场状态是否分化。
- 历史反馈是否偏弱。
- 风控门禁是否支持阻断。

### 闭环不是 6/6

按缺口定位：

- 主动发现缺失：资讯采集或事件触发缺失。
- 事件理解缺失：LLM 未就绪或调用失败。
- 市场状态缺失：日 K、市场统计或分钟代理缺失。
- 动态排序缺失：候选不足或日线特征缺失。
- 风控缺失：risk_control 未生成。
- 反馈学习缺失：缺少结算或实时反馈样本。

## 9. 验收命令

后端闭环相关测试：

```bash
.venv/bin/python -m pytest tests/test_catalyst_selection_service.py tests/test_daily_review_market_behavior.py -q
```

市场状态契约测试：

```bash
.venv/bin/python -m pytest tests/test_market_routes_formal.py tests/test_data_source_governance.py -q
```

前端构建：

```bash
cd frontend
npm run build
```

健康检查：

```bash
curl -sS http://127.0.0.1:8500/healthz
lsof -nP -iTCP:8500 -sTCP:LISTEN
lsof -nP -iTCP:5174 -sTCP:LISTEN
```

## 10. 安全边界

- 当前 AI监控池默认 `monitor_only`，不自动下单。
- 实盘仓保持只读口径。
- 自动交易必须显式配置策略、账户、审批和风控。
- 所有 LLM key 不应写入文档、日志或页面明文。
- 页面结果只用于研究、复盘和监控，不构成直接买卖建议。

## 11. 快速判断是否跑在最终版

打开 `/catalyst-selection`，同时满足以下条件即可认为是最终版闭环：

- 有“AI量化闭环状态”。
- 有“主动触发 event_driven”。
- 六阶段均为 active 或已运行。
- 有“远程LLM就绪”。
- 有“AI监控池”。
- 监控池显示 `门禁分布`。
- 最近闭环审计显示 `闭环 6/6`。
- 后端 `/healthz` 正常。
