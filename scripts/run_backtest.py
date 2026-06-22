"""
策略回测运行脚本
直接运行回测，无需前端
"""

import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import asyncio
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# 导入模块
from api.core.strategy_db import StrategySessionLocal
from api.models.strategy_models import StrategyDB, BacktestJobDB
from tradingagents.backtest.engine_v2 import BacktestEngine


def generate_mock_data(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """生成模拟股票数据"""
    dates = pd.date_range(start_date, end_date, freq='B')
    
    # 生成多只股票的数据
    stocks = ['000001.SZ', '000002.SZ', '000333.SZ', '000651.SZ', '000858.SZ']
    
    data = []
    for stock in stocks:
        base_price = np.random.uniform(10, 100)
        
        for i, date in enumerate(dates):
            # 模拟价格波动
            change = np.random.uniform(-0.03, 0.03)
            if i == 0:
                close = base_price
            else:
                close = data[-len(stocks)]['close'] * (1 + change)
            
            open_price = close * (1 + np.random.uniform(-0.01, 0.01))
            high = max(open_price, close) * (1 + np.random.uniform(0, 0.02))
            low = min(open_price, close) * (1 - np.random.uniform(0, 0.02))
            volume = np.random.randint(1000000, 10000000)
            
            data.append({
                'date': date,
                'symbol': stock,
                'open': open_price,
                'high': high,
                'low': low,
                'close': close,
                'volume': volume,
            })
    
    return pd.DataFrame(data)


async def run_backtest(job_id: str):
    """执行回测任务"""
    db = StrategySessionLocal()
    
    try:
        # 获取任务信息
        job = db.query(BacktestJobDB).filter(BacktestJobDB.id == job_id).first()
        if not job:
            print(f"❌ 任务不存在: {job_id}")
            return
        
        # 更新状态
        job.status = "running"
        job.started_at = datetime.now()
        db.commit()
        
        print(f"🚀 开始回测: {job.strategy.name}")
        print(f"   回测模式: {job.backtest_mode}")
        print(f"   时间范围: {job.start_date.date()} 至 {job.end_date.date()}")
        print(f"   初始资金: ¥{job.initial_capital:,.2f}")
        
        # 生成模拟数据
        print("\n📊 生成模拟数据...")
        data = generate_mock_data(job.start_date, job.end_date)
        print(f"   数据量: {len(data)} 条")
        print(f"   股票数: {data['symbol'].nunique()} 只")
        
        # 创建回测引擎
        backtest_engine = BacktestEngine(
            initial_capital=job.initial_capital,
            commission_rate=job.commission_rate,
            slippage_rate=job.slippage_rate,
            stamp_duty=job.stamp_duty,
        )
        
        # 运行回测
        print("\n⚡ 执行回测...")
        result = backtest_engine.run_backtest(
            strategy=job.strategy.to_dict(),
            data=data,
            start_date=job.start_date,
            end_date=job.end_date,
            backtest_mode=job.backtest_mode,
        )
        
        # 更新结果
        job.status = "completed"
        job.progress = 1.0
        job.completed_at = datetime.now()
        job.result = {
            'metrics': {
                'total_return': result.total_return,
                'annual_return': result.annual_return,
                'sharpe_ratio': result.sharpe_ratio,
                'max_drawdown': result.max_drawdown,
                'win_rate': result.win_rate,
                'total_trades': result.total_trades,
                'profit_factor': result.profit_factor,
            },
            'equity_curve': result.equity_curve[:100],  # 只保存前100条
            'trade_list': result.trade_list[:50],  # 只保存前50条
        }
        db.commit()
        
        # 打印结果
        print("\n✅ 回测完成!")
        print("=" * 60)
        print(f"策略名称: {result.strategy_name}")
        print(f"回测期间: {result.start_date.date()} 至 {result.end_date.date()}")
        print(f"初始资金: ¥{result.initial_capital:,.2f}")
        print(f"最终资金: ¥{result.final_capital:,.2f}")
        print("=" * 60)
        print("\n📊 绩效指标:")
        print(f"  总收益率:     {result.total_return:.2%}")
        print(f"  年化收益率:   {result.annual_return:.2%}")
        print(f"  夏普比率:     {result.sharpe_ratio:.2f}")
        print(f"  最大回撤:     {result.max_drawdown:.2%}")
        print("=" * 60)
        print("\n📈 交易统计:")
        print(f"  总交易次数:   {result.total_trades}")
        print(f"  盈利次数:     {result.winning_trades}")
        print(f"  亏损次数:     {result.losing_trades}")
        print(f"  胜率:         {result.win_rate:.2%}")
        print(f"  盈亏比:       {result.profit_factor:.2f}")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ 回测失败: {e}")
        import traceback
        traceback.print_exc()
        
        # 更新错误状态
        job = db.query(BacktestJobDB).filter(BacktestJobDB.id == job_id).first()
        if job:
            job.status = "failed"
            job.error_message = str(e)
            db.commit()
    
    finally:
        db.close()


def main():
    """主函数"""
    print("=" * 60)
    print("策略回测测试")
    print("=" * 60)
    
    db = StrategySessionLocal()
    
    try:
        # 获取所有策略
        strategies = db.query(StrategyDB).all()
        
        if not strategies:
            print("\n❌ 没有策略，请先创建策略")
            print("   可先通过前端策略管理创建，或运行: python scripts/verify_strategy_workflows.py")
            return
        
        print(f"\n📋 找到 {len(strategies)} 个策略:")
        for i, s in enumerate(strategies, 1):
            print(f"   {i}. {s.name} ({s.strategy_type.value})")
        
        # 选择第一个策略
        strategy = strategies[0]
        
        # 创建回测任务
        job = BacktestJobDB(
            strategy_id=strategy.id,
            backtest_mode="indicator_driven",
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2026, 4, 18),
            initial_capital=1000000.0,
            benchmark="hs300",
            commission_rate=0.0003,
            slippage_rate=0.001,
            stamp_duty=0.001,
            status="pending",
        )
        
        db.add(job)
        db.commit()
        db.refresh(job)
        
        print(f"\n✅ 创建回测任务: {job.id}")
        
        # 运行回测
        asyncio.run(run_backtest(job.id))
        
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        db.close()


if __name__ == "__main__":
    main()
