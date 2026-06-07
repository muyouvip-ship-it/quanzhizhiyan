"""
Agent回测验证系统 - P2优化
评估Agent决策的历史准确率
"""

import asyncio
import json
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import pandas as pd
import numpy as np
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


@dataclass
class BacktestResult:
    """回测结果"""
    date: str
    symbol: str
    decision: str  # BUY, SELL, HOLD
    confidence: float
    actual_return: float  # 实际收益率
    holding_days: int
    correct: bool  # 决策是否正确
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BacktestStats:
    """回测统计"""
    total_trades: int = 0
    correct_trades: int = 0
    accuracy: float = 0.0
    avg_return: float = 0.0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_trades": self.total_trades,
            "correct_trades": self.correct_trades,
            "accuracy": f"{self.accuracy:.2%}",
            "avg_return": f"{self.avg_return:.2%}",
            "win_rate": f"{self.win_rate:.2%}",
            "profit_factor": f"{self.profit_factor:.2f}",
            "sharpe_ratio": f"{self.sharpe_ratio:.2f}",
            "max_drawdown": f"{self.max_drawdown:.2%}",
        }


class AgentBacktester:
    """
    Agent回测器
    
    使用示例:
        backtester = AgentBacktester(graph)
        results = await backtester.run("600519.SH", "2025-01-01", "2025-03-31")
        print(results["stats"])
    """
    
    def __init__(
        self,
        trading_graph,
        holding_period: int = 5,  # 持仓周期（天）
        initial_capital: float = 100000.0,
    ):
        """
        Args:
            trading_graph: TradingAgentsGraph实例
            holding_period: 持仓周期
            initial_capital: 初始资金
        """
        self.graph = trading_graph
        self.holding_period = holding_period
        self.initial_capital = initial_capital
        self.results: List[BacktestResult] = []
    
    async def run_single(
        self,
        symbol: str,
        trade_date: str,
    ) -> BacktestResult:
        """
        单次回测
        
        Args:
            symbol: 股票代码
            trade_date: 交易日期
        
        Returns:
            BacktestResult
        """
        logger.info(f"回测 {symbol} @ {trade_date}")
        
        # 1. 运行Agent分析
        try:
            result = await self.graph.arun(
                company_name=symbol,
                trade_date=trade_date,
            )
            
            # 提取决策
            final_decision = result.get("final_decision", "HOLD")
            confidence = result.get("confidence", 0.5)
            
        except Exception as e:
            logger.error(f"Agent运行失败: {e}")
            return BacktestResult(
                date=trade_date,
                symbol=symbol,
                decision="ERROR",
                confidence=0.0,
                actual_return=0.0,
                holding_days=0,
                correct=False,
                details={"error": str(e)},
            )
        
        # 2. 获取实际收益
        try:
            actual_return = await self._get_actual_return(
                symbol,
                trade_date,
                self.holding_period,
            )
        except Exception as e:
            logger.error(f"获取实际收益失败: {e}")
            actual_return = 0.0
        
        # 3. 判断决策是否正确
        correct = self._evaluate_decision(final_decision, actual_return)
        
        return BacktestResult(
            date=trade_date,
            symbol=symbol,
            decision=final_decision,
            confidence=confidence,
            actual_return=actual_return,
            holding_days=self.holding_period,
            correct=correct,
            details=result,
        )
    
    async def run(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        sample_interval: int = 5,  # 采样间隔（天）
    ) -> Dict[str, Any]:
        """
        批量回测
        
        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            sample_interval: 采样间隔
        
        Returns:
            {
                "results": List[BacktestResult],
                "stats": BacktestStats,
            }
        """
        logger.info(
            f"开始回测 {symbol} "
            f"({start_date} ~ {end_date}, "
            f"间隔={sample_interval}天)"
        )
        
        # 获取交易日列表
        trading_days = await self._get_trading_days(
            start_date,
            end_date,
            sample_interval,
        )
        
        # 运行回测
        results: List[BacktestResult] = []
        for trade_date in trading_days:
            result = await self.run_single(symbol, trade_date)
            results.append(result)
            
            # 避免API限流
            await asyncio.sleep(1)
        
        self.results = results
        
        # 计算统计
        stats = self._calculate_stats(results)
        
        return {
            "results": results,
            "stats": stats,
        }
    
    async def _get_actual_return(
        self,
        symbol: str,
        trade_date: str,
        holding_days: int,
    ) -> float:
        """获取实际收益率，基于 stock_daily_kline 表计算。"""
        try:
            from api.database import SessionLocal
            from api.services.market_data_pipeline_service import preferred_daily_kline_table
            from sqlalchemy import text
            from datetime import timedelta

            table = preferred_daily_kline_table()
            start_dt = datetime.strptime(trade_date, "%Y-%m-%d")
            end_dt = start_dt + timedelta(days=holding_days + 5)

            db = SessionLocal()
            try:
                rows = db.execute(
                    text(
                        f"SELECT trade_date, close FROM {table} "
                        "WHERE symbol = :symbol AND trade_date BETWEEN :start AND :end "
                        "ORDER BY trade_date ASC"
                    ),
                    {"symbol": symbol, "start": trade_date, "end": end_dt.strftime("%Y-%m-%d")},
                ).fetchall()

                if len(rows) < 2:
                    return 0.0

                entry_price = float(rows[0][1])
                exit_price = float(rows[-1][1])
                if entry_price and entry_price != 0:
                    return (exit_price - entry_price) / entry_price
                return 0.0
            finally:
                db.close()
        except Exception:
            logger.warning("Failed to fetch actual return for %s on %s, returning 0.0", symbol, trade_date)
            return 0.0
    
    async def _get_trading_days(
        self,
        start_date: str,
        end_date: str,
        interval: int,
    ) -> List[str]:
        """获取交易日列表，基于真实交易日历并按 interval 采样。"""
        try:
            from tradingagents.dataflows.trade_calendar import _load_cn_trade_dates

            all_dates, _ = _load_cn_trade_dates()
            if not all_dates:
                # Fallback: 使用周末规则
                start = datetime.strptime(start_date, "%Y-%m-%d")
                end = datetime.strptime(end_date, "%Y-%m-%d")
                days = []
                current = start
                while current <= end:
                    if current.weekday() < 5:
                        days.append(current.strftime("%Y-%m-%d"))
                    current += timedelta(days=1)
                return days[::interval]

            start_d = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_d = datetime.strptime(end_date, "%Y-%m-%d").date()
            filtered = [d.isoformat() for d in all_dates if start_d <= d <= end_d]
            return filtered[::interval]
        except Exception:
            logger.warning("Failed to load trade calendar, using weekday fallback")
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            days = []
            current = start
            while current <= end:
                if current.weekday() < 5:
                    days.append(current.strftime("%Y-%m-%d"))
                current += timedelta(days=1)
            return days[::interval]
    
    def _evaluate_decision(
        self,
        decision: str,
        actual_return: float,
    ) -> bool:
        """
        评估决策是否正确
        
        规则：
        - BUY + return > 0 → 正确
        - SELL + return < 0 → 正确
        - HOLD + abs(return) < 2% → 正确
        """
        if decision == "BUY":
            return actual_return > 0
        elif decision == "SELL":
            return actual_return < 0
        elif decision == "HOLD":
            return abs(actual_return) < 0.02
        else:
            return False
    
    def _calculate_stats(
        self,
        results: List[BacktestResult],
    ) -> BacktestStats:
        """
        计算回测统计
        """
        if not results:
            return BacktestStats()
        
        total = len(results)
        correct = sum(1 for r in results if r.correct)
        returns = [r.actual_return for r in results]
        
        # 计算各种指标
        accuracy = correct / total if total > 0 else 0
        avg_return = np.mean(returns) if returns else 0
        
        # 盈利交易 vs 亏损交易
        profits = [r for r in returns if r > 0]
        losses = [r for r in returns if r < 0]
        
        win_rate = len(profits) / total if total > 0 else 0
        
        gross_profit = sum(profits) if profits else 0
        gross_loss = abs(sum(losses)) if losses else 1e-6
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0
        
        # Sharpe Ratio（简化版）
        if len(returns) > 1:
            sharpe = np.mean(returns) / (np.std(returns) + 1e-6) * np.sqrt(252)
        else:
            sharpe = 0
        
        # Max Drawdown
        cumulative = np.cumprod([1 + r for r in returns])
        running_max = np.maximum.accumulate(cumulative)
        drawdowns = (cumulative - running_max) / running_max
        max_drawdown = np.min(drawdowns) if len(drawdowns) > 0 else 0
        
        return BacktestStats(
            total_trades=total,
            correct_trades=correct,
            accuracy=accuracy,
            avg_return=avg_return,
            win_rate=win_rate,
            profit_factor=profit_factor,
            sharpe_ratio=sharpe,
            max_drawdown=max_drawdown,
        )
    
    def save_results(
        self,
        output_path: str,
    ):
        """
        保存回测结果
        """
        output = {
            "summary": self._calculate_stats(self.results).to_dict(),
            "details": [
                {
                    "date": r.date,
                    "symbol": r.symbol,
                    "decision": r.decision,
                    "confidence": r.confidence,
                    "actual_return": f"{r.actual_return:.2%}",
                    "correct": r.correct,
                }
                for r in self.results
            ],
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        logger.info(f"回测结果已保存到 {output_path}")


# ━━━━ 便捷命令 ━━━━

async def run_backtest(
    graph,
    symbol: str,
    start_date: str,
    end_date: str,
    output_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    便捷函数：运行回测
    
    使用示例:
        results = await run_backtest(
            graph,
            "600519.SH",
            "2025-01-01",
            "2025-03-31",
            "backtest_results.json",
        )
    """
    backtester = AgentBacktester(graph)
    results = await backtester.run(symbol, start_date, end_date)
    
    if output_path:
        backtester.save_results(output_path)
    
    return results
