# Windows QMT Bridge 更新说明

## 是否需要修正 Windows 文件

需要。当前 Mac 项目里的后端已经按“三仓隔离”调整完成，但 Windows 机器上运行的 QMT Bridge 也必须替换成最新版本，否则可能仍然只有模拟盘桥接，或无法启动实盘仓桥接。

## 需要复制到 Windows 的文件

把本目录下所有文件复制到 Windows：

- 源目录：`windows_qmt_bridge_update/`
- 目标目录：`D:\QMT\`

复制后 Windows 目录应为：

- `D:\QMT\start_qmt_bridge.bat`
- `D:\QMT\start_qmt_bridge.ps1`
- `D:\QMT\start_qmt_bridge_live.bat`
- `D:\QMT\start_qmt_bridge_live.ps1`
- `D:\QMT\run_qmt_minute_history_sync.bat`
- `D:\QMT\run_qmt_minute_history_sync.ps1`
- `D:\QMT\run_qmt_minute_history_import.bat`
- `D:\QMT\run_qmt_minute_history_import.ps1`
- `D:\QMT\scripts\qmt_bridge_server.py`
- `D:\QMT\scripts\qmt_minute_history_sync.py`

## 启动方式

### 模拟盘

1. 登录模拟 QMT。
2. 双击 `D:\QMT\start_qmt_bridge.bat`。
3. 默认监听端口：`8710`。
4. 对应账户：`39027628`。
5. 对应页面：虚拟仓。
6. 默认 `QMT_BRIDGE_ROLE=paper`、`QMT_BRIDGE_ACCOUNT_KEY=paper_sim`、`QMT_BRIDGE_ALLOW_TRADING=1`，允许模拟仓交易联调。

### 实盘

1. 登录实盘 QMT。
2. 双击 `D:\QMT\start_qmt_bridge_live.bat`。
3. 默认监听端口：`8711`。
4. 对应账户：`8886186680`。
5. 对应页面：实盘仓。
6. 默认 `QMT_BRIDGE_ROLE=live`、`QMT_BRIDGE_ACCOUNT_KEY=live_real`、`QMT_BRIDGE_ALLOW_TRADING=1`，允许实盘仓下单和撤单。

## 三仓隔离关系

- 虚拟仓：只读取模拟盘 QMT，不写入跟踪看板。
- 实盘仓：读取实盘 QMT，不写入跟踪看板，交易指令直接发送到实盘 QMT。
- 跟踪看板：保持原有独立逻辑，不读取模拟盘/实盘 QMT 仓位。
- 回测分钟线下载：固定使用模拟仓 `paper_sim/8710`，不调用实盘 `live_real/8711`。

## 连通性验证

在 Mac 项目机器上验证：

```bash
curl -sS -H "Authorization: Bearer your-bridge-token" http://192.168.10.1:8710/health
curl -sS -H "Authorization: Bearer your-bridge-token" http://192.168.10.1:8711/health
```

如果 `8711` 返回连接失败，说明实盘 Bridge 还没启动，或 Windows 防火墙没有放行端口。

## 分钟线下载与导库

当前版本的 QMT Bridge 已新增历史分钟线任务接口，Mac 后端可以复用虚拟仓同一条 bridge 连接发起 Windows 侧 `xtdata` 下载。

新增接口：

- `POST /history/minute/sync`：创建历史 1 分钟 K 线同步任务
- `GET /history/minute/jobs/{job_id}`：查询同步任务进度

> 注意：如果要让 Windows Bridge 直接导入 Mac/PostgreSQL，`QMT_MINUTE_DATABASE_URL` 必须是 Windows 可以访问的数据库地址，不能写 `localhost`。例如 `postgresql://user:password@192.168.10.x:5432/trading_agents`。
> 安全：Mac 后端会按 `QMT_HISTORY_ACCOUNT_KEY=paper_sim` 查找模拟仓 bridge，不会自动回退到实盘 bridge。
> 如果希望跳过项目层 `parquet/csv` 中间文件、直接写 PostgreSQL，可设置 `QMT_MINUTE_SKIP_EXPORT=1`，或在脚本参数中显式加 `--skip-export`。QMT 自身的本地缓存目录仍会由 `xtdata` 使用，这一层不能完全绕过。

### 下载全市场分钟线

执行：

```powershell
cd D:\QMT
powershell -ExecutionPolicy Bypass -File .\run_qmt_minute_history_sync.ps1
```

默认落地：

```text
D:\QMT\data\minute_history
```

### 只导入已有分钟线到 PostgreSQL

先设置数据库连接串：

```powershell
setx QMT_MINUTE_DATABASE_URL "postgresql://user:password@host:5432/dbname"
```

重新打开 PowerShell 后执行：

```powershell
cd D:\QMT
powershell -ExecutionPolicy Bypass -File .\install_qmt_history_import_deps.ps1
```

然后执行：

```powershell
cd D:\QMT
powershell -ExecutionPolicy Bypass -File .\run_qmt_minute_history_import.ps1
```

### 推荐试跑顺序

1. 先执行 `--dry-run` 或只跑少量股票，验证板块别名和下载权限
2. 再下载近一年样本
3. 再跑全市场多年历史
4. 最后再执行导库
