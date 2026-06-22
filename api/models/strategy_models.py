"""
策略管理数据库模型

定义策略相关的数据库表结构。
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import Column, String, Integer, Boolean, Float, Text, DateTime, Date, JSON, ForeignKey, Enum, Index, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship
import logging

logger = logging.getLogger(__name__)
import enum
import uuid

Base = declarative_base()


class StrategyType(str, enum.Enum):
    """策略类型枚举"""
    SELECTION = "selection"  # 选股策略
    TRADING = "trading"      # 交易策略
    RISK = "risk"            # 风控策略
    PORTFOLIO = "portfolio"  # 组合策略


class StrategyStatus(str, enum.Enum):
    """策略状态枚举"""
    DRAFT = "draft"          # 草稿
    ACTIVE = "active"        # 运行中
    PAUSED = "paused"        # 已暂停
    ARCHIVED = "archived"    # 已归档


class StrategyDB(Base):
    """策略表"""
    __tablename__ = "strategies"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String(200), nullable=False, comment="策略名称")
    strategy_type = Column(Enum(StrategyType), nullable=False, comment="策略类型")
    parent_id = Column(String(36), ForeignKey("strategies.id"), nullable=True, comment="父策略ID")
    description = Column(Text, comment="策略描述")

    # 策略配置
    indicators = Column(JSON, comment="指标配置")
    entry_rules = Column(JSON, comment="入场规则")
    exit_rules = Column(JSON, comment="出场规则")
    position_rules = Column(JSON, comment="仓位规则")
    risk_rules = Column(JSON, comment="风控规则")
    parameters = Column(JSON, comment="策略参数")

    # 状态管理
    status = Column(Enum(StrategyStatus), default=StrategyStatus.DRAFT, comment="策略状态")
    version = Column(Integer, default=1, comment="版本号")
    is_active = Column(Boolean, default=False, comment="是否激活")

    # 运行统计
    run_count = Column(Integer, default=0, comment="运行次数")
    last_run_time = Column(DateTime, comment="最后运行时间")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")

    # 绩效快照
    total_return = Column(Float, comment="总收益率")
    sharpe_ratio = Column(Float, comment="夏普比率")
    max_drawdown = Column(Float, comment="最大回撤")
    win_rate = Column(Float, comment="胜率")

    # 关系
    children = relationship("StrategyDB", backref="parent", remote_side=[id])

    def to_dict(self):
        """转换为字典"""
        return {
            "id": self.id,
            "name": self.name,
            "strategy_type": self.strategy_type.value if self.strategy_type else None,
            "parent_id": self.parent_id,
            "description": self.description,
            "indicators": self.indicators,
            "entry_rules": self.entry_rules,
            "exit_rules": self.exit_rules,
            "position_rules": self.position_rules,
            "risk_rules": self.risk_rules,
            "parameters": self.parameters,
            "status": self.status.value if self.status else None,
            "version": self.version,
            "is_active": self.is_active,
            "run_count": self.run_count,
            "last_run_time": self.last_run_time.isoformat() if self.last_run_time else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "performance": {
                "total_return": self.total_return,
                "sharpe_ratio": self.sharpe_ratio,
                "max_drawdown": self.max_drawdown,
                "win_rate": self.win_rate,
            } if self.total_return is not None else None,
        }


class BacktestJobDB(Base):
    """回测任务表"""
    __tablename__ = "backtest_jobs"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    strategy_id = Column(String(36), ForeignKey("strategies.id"), nullable=False, comment="策略ID")

    # 回测配置
    backtest_mode = Column(String(50), comment="回测模式: fixed_period, indicator_driven, walk_forward")
    start_date = Column(DateTime, nullable=False, comment="开始日期")
    end_date = Column(DateTime, nullable=False, comment="结束日期")
    initial_capital = Column(Float, default=1000000.0, comment="初始资金")
    benchmark = Column(String(50), default="hs300", comment="基准指数")

    # 成本配置
    commission_rate = Column(Float, default=0.0003, comment="手续费率")
    slippage_rate = Column(Float, default=0.001, comment="滑点率")
    stamp_duty = Column(Float, default=0.001, comment="印花税")

    # 任务状态
    status = Column(String(50), default="pending", comment="任务状态: pending, running, completed, failed")
    progress = Column(Float, default=0.0, comment="进度 0-1")
    error_message = Column(Text, comment="错误信息")

    # 回测结果
    result = Column(JSON, comment="回测结果")

    # 时间戳
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    started_at = Column(DateTime, comment="开始时间")
    completed_at = Column(DateTime, comment="完成时间")

    # 关系
    strategy = relationship("StrategyDB")

    def to_dict(self):
        """转换为字典"""
        return {
            "id": self.id,
            "strategy_id": self.strategy_id,
            "backtest_mode": self.backtest_mode,
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "end_date": self.end_date.isoformat() if self.end_date else None,
            "initial_capital": self.initial_capital,
            "benchmark": self.benchmark,
            "commission_rate": self.commission_rate,
            "slippage_rate": self.slippage_rate,
            "stamp_duty": self.stamp_duty,
            "status": self.status,
            "progress": self.progress,
            "error_message": self.error_message,
            "result": self.result,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


class BacktestResultDB(Base):
    """回测结果表"""
    __tablename__ = "backtest_results"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    job_id = Column(String(36), ForeignKey("backtest_jobs.id"), nullable=False, comment="任务ID")

    # 收益指标
    total_return = Column(Float, comment="总收益率")
    annual_return = Column(Float, comment="年化收益率")
    excess_return = Column(Float, comment="超额收益")

    # 风险指标
    max_drawdown = Column(Float, comment="最大回撤")
    max_drawdown_duration = Column(Integer, comment="最大回撤持续天数")
    volatility = Column(Float, comment="年化波动率")
    var_95 = Column(Float, comment="VaR 95%")

    # 风险调整收益
    sharpe_ratio = Column(Float, comment="夏普比率")
    sortino_ratio = Column(Float, comment="索提诺比率")
    calmar_ratio = Column(Float, comment="卡玛比率")
    information_ratio = Column(Float, comment="信息比率")

    # 交易统计
    total_trades = Column(Integer, comment="总交易次数")
    winning_trades = Column(Integer, comment="盈利交易次数")
    losing_trades = Column(Integer, comment="亏损交易次数")
    win_rate = Column(Float, comment="胜率")
    profit_factor = Column(Float, comment="盈亏比")
    avg_win = Column(Float, comment="平均盈利")
    avg_loss = Column(Float, comment="平均亏损")
    avg_holding_period = Column(Float, comment="平均持仓天数")

    # 成本分析
    total_commission = Column(Float, comment="总手续费")
    total_slippage = Column(Float, comment="总滑点成本")
    cost_ratio = Column(Float, comment="成本占比")

    # 基准对比
    alpha = Column(Float, comment="Alpha")
    beta = Column(Float, comment="Beta")
    tracking_error = Column(Float, comment="跟踪误差")
    upside_capture = Column(Float, comment="上行捕获比率")
    downside_capture = Column(Float, comment="下行捕获比率")

    # 详细数据
    equity_curve = Column(JSON, comment="资金曲线")
    drawdown_curve = Column(JSON, comment="回撤曲线")
    position_history = Column(JSON, comment="持仓历史")
    trade_list = Column(JSON, comment="交易列表")
    monthly_returns = Column(JSON, comment="月度收益")

    # 时间戳
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")

    def to_dict(self):
        """转换为字典"""
        return {
            "id": self.id,
            "job_id": self.job_id,
            "returns": {
                "total_return": self.total_return,
                "annual_return": self.annual_return,
                "excess_return": self.excess_return,
            },
            "risk": {
                "max_drawdown": self.max_drawdown,
                "max_drawdown_duration": self.max_drawdown_duration,
                "volatility": self.volatility,
                "var_95": self.var_95,
            },
            "risk_adjusted": {
                "sharpe_ratio": self.sharpe_ratio,
                "sortino_ratio": self.sortino_ratio,
                "calmar_ratio": self.calmar_ratio,
                "information_ratio": self.information_ratio,
            },
            "trading": {
                "total_trades": self.total_trades,
                "winning_trades": self.winning_trades,
                "losing_trades": self.losing_trades,
                "win_rate": self.win_rate,
                "profit_factor": self.profit_factor,
                "avg_win": self.avg_win,
                "avg_loss": self.avg_loss,
                "avg_holding_period": self.avg_holding_period,
            },
            "costs": {
                "total_commission": self.total_commission,
                "total_slippage": self.total_slippage,
                "cost_ratio": self.cost_ratio,
            },
            "benchmark": {
                "alpha": self.alpha,
                "beta": self.beta,
                "tracking_error": self.tracking_error,
                "upside_capture": self.upside_capture,
                "downside_capture": self.downside_capture,
            },
            "details": {
                "equity_curve": self.equity_curve,
                "drawdown_curve": self.drawdown_curve,
                "position_history": self.position_history,
                "trade_list": self.trade_list,
                "monthly_returns": self.monthly_returns,
            },
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class TradeRecordDB(Base):
    """交易记录表"""
    __tablename__ = "trade_records"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    strategy_id = Column(String(36), ForeignKey("strategies.id"), comment="策略ID")

    # 交易信息
    symbol = Column(String(20), nullable=False, comment="股票代码")
    side = Column(String(10), nullable=False, comment="买卖方向: buy, sell")
    quantity = Column(Float, nullable=False, comment="交易数量")
    price = Column(Float, nullable=False, comment="交易价格")

    # 交易成本
    commission = Column(Float, default=0.0, comment="手续费")
    slippage = Column(Float, default=0.0, comment="滑点成本")
    stamp_duty = Column(Float, default=0.0, comment="印花税")

    # 交易模式
    mode = Column(String(20), default="paper", comment="交易模式: paper, live")
    order_type = Column(String(20), default="market", comment="订单类型: market, limit")

    # 交易时间
    executed_at = Column(DateTime, default=datetime.now, comment="成交时间")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")

    # 关系
    strategy = relationship("StrategyDB")

    def to_dict(self):
        """转换为字典"""
        return {
            "id": self.id,
            "strategy_id": self.strategy_id,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": self.quantity,
            "price": self.price,
            "commission": self.commission,
            "slippage": self.slippage,
            "stamp_duty": self.stamp_duty,
            "mode": self.mode,
            "order_type": self.order_type,
            "executed_at": self.executed_at.isoformat() if self.executed_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class PaperAccountDB(Base):
    """纸交易账户表"""
    __tablename__ = "paper_accounts"

    id = Column(String(64), primary_key=True)
    name = Column(String(200), nullable=False, comment="账户名称")
    initial_capital = Column(Float, default=1_000_000.0, comment="初始资金")
    cash = Column(Float, default=1_000_000.0, comment="当前现金")
    status = Column(String(20), default="active", comment="账户状态")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "initial_capital": self.initial_capital,
            "cash": self.cash,
            "status": self.status,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class PaperOrderDB(Base):
    """纸交易订单表"""
    __tablename__ = "paper_orders"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    account_id = Column(String(64), ForeignKey("paper_accounts.id"), nullable=False, comment="纸交易账户ID")
    strategy_id = Column(String(36), ForeignKey("strategies.id"), nullable=False, comment="策略ID")
    symbol = Column(String(20), nullable=False, comment="股票代码")
    side = Column(String(10), nullable=False, comment="买卖方向")
    quantity = Column(Float, nullable=False, comment="数量")
    price = Column(Float, nullable=False, comment="价格")
    commission = Column(Float, default=0.0, comment="手续费")
    slippage = Column(Float, default=0.0, comment="滑点")
    stamp_duty = Column(Float, default=0.0, comment="印花税")
    order_type = Column(String(20), default="strategy_signal", comment="订单类型")
    status = Column(String(20), default="filled", comment="订单状态")
    reason = Column(Text, comment="触发原因")
    executed_at = Column(DateTime, default=datetime.now, comment="成交时间")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")

    account = relationship("PaperAccountDB")
    strategy = relationship("StrategyDB")

    def to_dict(self):
        return {
            "id": self.id,
            "account_id": self.account_id,
            "strategy_id": self.strategy_id,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": self.quantity,
            "price": self.price,
            "commission": self.commission,
            "slippage": self.slippage,
            "stamp_duty": self.stamp_duty,
            "order_type": self.order_type,
            "status": self.status,
            "reason": self.reason,
            "executed_at": self.executed_at.isoformat() if self.executed_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class RealtimeMonitorDB(Base):
    """实时监控实例表。"""

    __tablename__ = "realtime_monitors"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(64), index=True, nullable=False, comment="用户ID")
    name = Column(String(200), nullable=False, comment="监控实例名称")
    account_key = Column(String(64), index=True, nullable=False, comment="QMT账户Key")
    account_role = Column(String(20), default="paper", comment="账户角色: paper/live")
    strategy_id = Column(String(36), ForeignKey("strategies.id"), nullable=False, comment="策略ID")
    strategy_version_id = Column(String(64), nullable=True, comment="策略版本ID")
    status = Column(String(20), default="ready", index=True, comment="draft/ready/running/paused/halted/fused/error")
    execution_mode = Column(String(20), default="auto", comment="auto/monitor_only")
    auto_trade_enabled = Column(Boolean, default=True, comment="是否允许自动交易")
    live_trading_enabled = Column(Boolean, default=False, comment="是否允许实盘自动交易")
    quote_source = Column(String(32), default="qmt", comment="行情来源")
    monitor_pool_json = Column(JSON, nullable=True, comment="监控池配置与最近解析结果")
    config_json = Column(JSON, nullable=True, comment="运行配置")
    risk_config_json = Column(JSON, nullable=True, comment="风控配置")
    state_json = Column(JSON, nullable=True, comment="运行状态快照")
    last_heartbeat_at = Column(DateTime, nullable=True, comment="最近心跳")
    fused_reason = Column(Text, nullable=True, comment="熔断原因")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")

    strategy = relationship("StrategyDB")

    def to_dict(self):
        return {
            "id": self.id,
            "user_id": self.user_id,
            "name": self.name,
            "account_key": self.account_key,
            "account_role": self.account_role,
            "strategy_id": self.strategy_id,
            "strategy_version_id": self.strategy_version_id,
            "status": self.status,
            "execution_mode": self.execution_mode,
            "auto_trade_enabled": bool(self.auto_trade_enabled),
            "live_trading_enabled": bool(self.live_trading_enabled),
            "quote_source": self.quote_source,
            "monitor_pool": self.monitor_pool_json or {},
            "config": self.config_json or {},
            "risk_config": self.risk_config_json or {},
            "state": self.state_json or {},
            "last_heartbeat_at": self.last_heartbeat_at.isoformat() if self.last_heartbeat_at else None,
            "fused_reason": self.fused_reason,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class RealtimeEventDB(Base):
    """实时监控事件表，保留完整事件回放。"""

    __tablename__ = "realtime_events"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    monitor_id = Column(String(36), ForeignKey("realtime_monitors.id"), index=True, nullable=False, comment="监控实例ID")
    user_id = Column(String(64), index=True, nullable=False, comment="用户ID")
    event_type = Column(String(64), index=True, nullable=False, comment="事件类型")
    account_key = Column(String(64), index=True, nullable=True, comment="账户Key")
    strategy_id = Column(String(36), index=True, nullable=True, comment="策略ID")
    strategy_version_id = Column(String(64), nullable=True, comment="策略版本ID")
    symbol = Column(String(20), index=True, nullable=True, comment="股票代码")
    trade_time = Column(DateTime, nullable=True, comment="交易时间")
    payload = Column(JSON, nullable=True, comment="通用事件载荷")
    signal_payload = Column(JSON, nullable=True, comment="信号载荷")
    risk_payload = Column(JSON, nullable=True, comment="风控载荷")
    order_payload = Column(JSON, nullable=True, comment="委托载荷")
    broker_result = Column(JSON, nullable=True, comment="券商/QMT返回")
    error_payload = Column(JSON, nullable=True, comment="错误载荷")
    request_id = Column(String(64), index=True, nullable=True, comment="请求ID")
    correlation_id = Column(String(64), index=True, nullable=True, comment="关联ID")
    created_at = Column(DateTime, default=datetime.now, index=True, comment="创建时间")

    monitor = relationship("RealtimeMonitorDB")

    def to_dict(self):
        return {
            "id": self.id,
            "monitor_id": self.monitor_id,
            "user_id": self.user_id,
            "event_type": self.event_type,
            "account_key": self.account_key,
            "strategy_id": self.strategy_id,
            "strategy_version_id": self.strategy_version_id,
            "symbol": self.symbol,
            "trade_time": self.trade_time.isoformat() if self.trade_time else None,
            "payload": self.payload or {},
            "signal_payload": self.signal_payload or {},
            "risk_payload": self.risk_payload or {},
            "order_payload": self.order_payload or {},
            "broker_result": self.broker_result or {},
            "error_payload": self.error_payload or {},
            "request_id": self.request_id,
            "correlation_id": self.correlation_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class RealtimeSignalExecutionDB(Base):
    """实时监控信号执行账本，保证同一信号只进入一次交易链路。"""

    __tablename__ = "realtime_signal_executions"
    __table_args__ = (
        UniqueConstraint("monitor_id", "signal_key", name="uq_realtime_signal_execution_monitor_key"),
    )

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    monitor_id = Column(String(36), ForeignKey("realtime_monitors.id"), index=True, nullable=False, comment="监控实例ID")
    user_id = Column(String(64), index=True, nullable=False, comment="用户ID")
    account_key = Column(String(64), index=True, nullable=False, comment="账户Key")
    strategy_id = Column(String(36), index=True, nullable=True, comment="策略ID")
    strategy_version_id = Column(String(64), nullable=True, comment="策略版本ID")
    symbol = Column(String(20), index=True, nullable=False, comment="股票代码")
    side = Column(String(10), nullable=False, comment="方向")
    timeframe = Column(String(20), index=True, nullable=True, comment="触发周期")
    bar_end = Column(String(40), index=True, nullable=True, comment="触发K线结束时间")
    signal_key = Column(String(80), index=True, nullable=False, comment="信号幂等键")
    status = Column(String(32), default="reserved", index=True, comment="reserved/generated/submitted/rejected")
    signal_identity_json = Column(JSON, nullable=True, comment="信号身份")
    signal_payload_json = Column(JSON, nullable=True, comment="信号载荷")
    order_intent_json = Column(JSON, nullable=True, comment="委托意图")
    broker_result_json = Column(JSON, nullable=True, comment="券商返回")
    error_message = Column(Text, nullable=True, comment="错误信息")
    first_seen_at = Column(DateTime, default=datetime.now, index=True, comment="首次发现时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")

    monitor = relationship("RealtimeMonitorDB")

    def to_dict(self):
        return {
            "id": self.id,
            "monitor_id": self.monitor_id,
            "user_id": self.user_id,
            "account_key": self.account_key,
            "strategy_id": self.strategy_id,
            "strategy_version_id": self.strategy_version_id,
            "symbol": self.symbol,
            "side": self.side,
            "timeframe": self.timeframe,
            "bar_end": self.bar_end,
            "signal_key": self.signal_key,
            "status": self.status,
            "signal_identity": self.signal_identity_json or {},
            "signal_payload": self.signal_payload_json or {},
            "order_intent": self.order_intent_json or {},
            "broker_result": self.broker_result_json or {},
            "error_message": self.error_message,
            "first_seen_at": self.first_seen_at.isoformat() if self.first_seen_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class RealtimeApprovalDB(Base):
    """实时监控人工确认任务表。"""

    __tablename__ = "realtime_approvals"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    monitor_id = Column(String(36), ForeignKey("realtime_monitors.id"), index=True, nullable=False, comment="监控实例ID")
    user_id = Column(String(64), index=True, nullable=False, comment="用户ID")
    account_key = Column(String(64), index=True, nullable=False, comment="账户Key")
    strategy_id = Column(String(36), index=True, nullable=True, comment="策略ID")
    symbol = Column(String(20), index=True, nullable=True, comment="股票代码")
    side = Column(String(10), nullable=True, comment="方向")
    status = Column(String(20), default="pending", index=True, comment="pending/approved/rejected/executed")
    reason = Column(Text, nullable=True, comment="确认原因")
    order_intent_json = Column(JSON, nullable=True, comment="委托意图")
    decision_json = Column(JSON, nullable=True, comment="人工决策")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")
    decided_at = Column(DateTime, nullable=True, comment="决策时间")

    monitor = relationship("RealtimeMonitorDB")

    def to_dict(self):
        return {
            "id": self.id,
            "monitor_id": self.monitor_id,
            "user_id": self.user_id,
            "account_key": self.account_key,
            "strategy_id": self.strategy_id,
            "symbol": self.symbol,
            "side": self.side,
            "status": self.status,
            "reason": self.reason,
            "order_intent": self.order_intent_json or {},
            "decision": self.decision_json or {},
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "decided_at": self.decided_at.isoformat() if self.decided_at else None,
        }


class SelectionCenterTaskDB(Base):
    """选股中心执行记录表，保存每次执行的固定结果快照。"""

    __tablename__ = "selection_center_tasks"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(64), index=True, nullable=False, comment="用户ID")
    name = Column(String(200), nullable=False, comment="任务名称")
    mode = Column(String(20), index=True, nullable=False, comment="strategy/catalyst/hybrid")
    status = Column(String(20), default="running", index=True, comment="running/completed/failed")
    progress = Column(Float, default=0.0, comment="进度 0-100")
    universe = Column(String(200), nullable=True, comment="股票池描述")
    rule = Column(Text, nullable=True, comment="选股规则描述")
    filters_json = Column(JSON, nullable=True, comment="过滤条件标签")
    config_json = Column(JSON, nullable=True, comment="任务创建配置")
    candidates_json = Column(JSON, nullable=True, comment="选股结果快照")
    error_message = Column(Text, nullable=True, comment="失败原因")
    created_at = Column(DateTime, default=datetime.now, index=True, comment="创建时间")
    started_at = Column(DateTime, nullable=True, comment="开始时间")
    completed_at = Column(DateTime, nullable=True, comment="完成时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")

    def to_dict(self):
        return {
            "id": self.id,
            "user_id": self.user_id,
            "name": self.name,
            "mode": self.mode,
            "status": self.status,
            "progress": float(self.progress or 0.0),
            "universe": self.universe or "",
            "rule": self.rule or "",
            "filters": list(self.filters_json or []),
            "config": self.config_json or {},
            "candidates": list(self.candidates_json or []),
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


Index("idx_realtime_events_monitor_created", RealtimeEventDB.monitor_id, RealtimeEventDB.created_at)
Index("idx_realtime_events_user_monitor_created_id", RealtimeEventDB.user_id, RealtimeEventDB.monitor_id, RealtimeEventDB.created_at, RealtimeEventDB.id)
Index("idx_realtime_signal_exec_monitor_bar", RealtimeSignalExecutionDB.monitor_id, RealtimeSignalExecutionDB.timeframe, RealtimeSignalExecutionDB.bar_end)
Index("idx_realtime_signal_exec_user_symbol", RealtimeSignalExecutionDB.user_id, RealtimeSignalExecutionDB.symbol, RealtimeSignalExecutionDB.first_seen_at)
Index("idx_realtime_approvals_user_status", RealtimeApprovalDB.user_id, RealtimeApprovalDB.status)
Index("idx_selection_center_tasks_user_created", SelectionCenterTaskDB.user_id, SelectionCenterTaskDB.created_at)
Index("idx_selection_center_tasks_user_mode_created", SelectionCenterTaskDB.user_id, SelectionCenterTaskDB.mode, SelectionCenterTaskDB.created_at)


# ============================================================
# 股票日K线数据表
# ============================================================

class StockDailyKlineDB(Base):
    """股票日K线数据表 - 对应PostgreSQL中的stock_daily_kline表"""
    __tablename__ = "stock_daily_kline"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False, index=True, comment="股票代码")
    trade_date = Column(Date, nullable=False, index=True, comment="交易日期")
    
    # OHLCV - 可空字段
    open = Column(Float, comment="开盘价")
    high = Column(Float, comment="最高价")
    low = Column(Float, comment="最低价")
    close = Column(Float, comment="收盘价")
    volume = Column(Float, comment="成交量")
    amount = Column(Float, comment="成交额")
    
    # 额外字段
    turnover_rate = Column(Float, comment="换手率")
    pre_close = Column(Float, comment="前收盘价")
    
    # 市值字段
    float_market_cap = Column(Float, comment="流通市值")
    total_market_cap = Column(Float, comment="总市值")
    
    # 财务字段
    net_profit_ttm = Column(Float, comment="净利润TTM")
    cash_flow_ttm = Column(Float, comment="现金流TTM")
    net_assets = Column(Float, comment="净资产")
    total_assets = Column(Float, comment="总资产")
    total_liabilities = Column(Float, comment="总负债")
    
    # 时间字段
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")
    
    # 联合唯一索引
    __table_args__ = (
        Index('idx_symbol_date', 'symbol', 'trade_date', unique=True),
    )

    def to_dict(self):
        """转换为字典"""
        return {
            "symbol": self.symbol,
            "trade_date": self.trade_date.isoformat() if self.trade_date else None,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "amount": self.amount,
            "turnover_rate": self.turnover_rate,
            "pre_close": self.pre_close,
        }


class IndexDailyKlineDB(Base):
    """指数日K线数据表"""
    __tablename__ = "index_daily_kline"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False, index=True, comment="指数代码")
    trade_date = Column(Date, nullable=False, index=True, comment="交易日期")
    
    # OHLCV
    open = Column(Float, nullable=False, comment="开盘价")
    high = Column(Float, nullable=False, comment="最高价")
    low = Column(Float, nullable=False, comment="最低价")
    close = Column(Float, nullable=False, comment="收盘价")
    volume = Column(Float, comment="成交量")
    amount = Column(Float, comment="成交额")
    
    # 元数据
    source = Column(String(32), default='akshare', comment="数据来源")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    
    # 联合唯一索引
    __table_args__ = (
        Index('idx_index_symbol_date', 'symbol', 'trade_date', unique=True),
    )

    def to_dict(self):
        """转换为字典"""
        return {
            "symbol": self.symbol,
            "trade_date": self.trade_date.isoformat() if self.trade_date else None,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "amount": self.amount,
        }


class FactorDB(Base):
    """因子表"""
    __tablename__ = "factors"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String(100), nullable=False, unique=True, comment="因子名称")
    category = Column(String(50), comment="因子类别: value, growth, quality, momentum, etc.")
    formula = Column(Text, comment="因子公式")
    parameters = Column(JSON, comment="因子参数")

    # 因子统计
    ic_history = Column(JSON, comment="IC历史")
    current_weight = Column(Float, default=0.0, comment="当前权重")

    # 状态
    is_active = Column(Boolean, default=True, comment="是否激活")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")

    def to_dict(self):
        """转换为字典"""
        return {
            "id": self.id,
            "name": self.name,
            "category": self.category,
            "formula": self.formula,
            "parameters": self.parameters,
            "ic_history": self.ic_history,
            "current_weight": self.current_weight,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class EvolutionExperimentDB(Base):
    """策略进化实验表"""
    __tablename__ = "strategy_evolution_experiments"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    strategy_id = Column(String(36), ForeignKey("strategies.id"), nullable=False, index=True, comment="基础策略ID")
    objective = Column(String(100), nullable=False, comment="优化目标")
    status = Column(String(30), default="completed", comment="实验状态")
    progress = Column(Float, default=1.0, comment="实验进度")
    search_space = Column(JSON, comment="搜索空间")
    base_backtest_run_id = Column(String(36), nullable=True, comment="基准回测ID")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")

    strategy = relationship("StrategyDB")
    candidates = relationship(
        "EvolutionCandidateDB",
        back_populates="experiment",
        cascade="all, delete-orphan",
        order_by="EvolutionCandidateDB.score.desc()",
    )


class EvolutionCandidateDB(Base):
    """策略进化候选表"""
    __tablename__ = "strategy_evolution_candidates"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    experiment_id = Column(
        String(36),
        ForeignKey("strategy_evolution_experiments.id"),
        nullable=False,
        index=True,
        comment="实验ID",
    )
    name = Column(String(200), nullable=False, comment="候选名称")
    score = Column(Float, default=0.0, comment="候选评分")
    status = Column(String(30), default="candidate", comment="候选状态")
    improvement_summary = Column(Text, comment="改进摘要")
    risk_flags = Column(JSON, comment="风险标记")
    metrics = Column(JSON, comment="候选绩效指标")
    dsl_patch = Column(JSON, comment="DSL 补丁")
    accepted_at = Column(DateTime, comment="接受时间")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")

    experiment = relationship("EvolutionExperimentDB", back_populates="candidates")


# 数据库初始化函数
def init_database(engine):
    """初始化数据库表"""
    Base.metadata.create_all(engine)
    logger.info("数据库表创建完成")


if __name__ == "__main__":
    # 测试数据库创建
    from api.core.strategy_db import strategy_engine

    init_database(strategy_engine)
