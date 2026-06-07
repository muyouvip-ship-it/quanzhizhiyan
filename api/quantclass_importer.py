"""
量化课堂数据导入到数据库
"""
import pandas as pd
import logging
import os

from api.services.market_data_pipeline_service import ingest_raw_daily_rows, reconcile_daily_trade_dates

logger = logging.getLogger(__name__)


def import_stock_daily_from_quantclass(db_session, csv_file_path: str) -> dict:
    """
    从量化课堂CSV文件导入股票日线数据到数据库（Pro版本，包含所有字段）
    
    Args:
        db_session: 数据库会话
        csv_file_path: CSV文件路径
    
    Returns:
        {
            'success': bool,
            'records_imported': int,
            'stocks_count': int,
            'errors': list
        }
    """
    try:
        # 读取CSV文件（量化课堂使用GBK编码，第一行是注释）
        logger.info(f"开始读取CSV文件: {csv_file_path}")
        df = pd.read_csv(csv_file_path, encoding='gbk', skiprows=1)
        trade_dates = pd.to_datetime(df['交易日期'], errors='coerce').dt.date
        min_trade_date = trade_dates.min() if not trade_dates.empty else None
        max_trade_date = trade_dates.max() if not trade_dates.empty else None
        
        logger.info(f"读取到 {len(df)} 行数据，{df['股票代码'].nunique()} 只股票")
        
        # 数据清洗后写入 raw/norm/pub 增量治理链路；不再回写旧的 stock_daily_kline。
        records = []
        errors = []
        
        for idx, row in df.iterrows():
            try:
                symbol = _normalize_quantclass_symbol(row['股票代码'])
                
                # 处理日期
                trade_date = pd.to_datetime(row['交易日期']).date()
                
                records.append({
                    "symbol": symbol,
                    "trade_date": trade_date,
                    "open": _safe_float(row.get('开盘价')),
                    "high": _safe_float(row.get('最高价')),
                    "low": _safe_float(row.get('最低价')),
                    "close": _safe_float(row.get('收盘价')),
                    "volume": _safe_float(row.get('成交量')),
                    "amount": _safe_float(row.get('成交额')),
                    "pre_close": _safe_float(row.get('前收盘价')),
                    "float_market_cap": _safe_float(row.get('流通市值')),
                    "total_market_cap": _safe_float(row.get('总市值')),
                    "net_profit_ttm": _safe_float(row.get('净利润TTM')),
                    "sw_industry_l1": _safe_text(row.get('新版申万一级行业名称')),
                    "sw_industry_l2": _safe_text(row.get('新版申万二级行业名称')),
                    "sw_industry_l3": _safe_text(row.get('新版申万三级行业名称')),
                })
                    
            except Exception as e:
                errors.append(f"行 {idx}: {str(e)}")
                continue
        
        ingest_result = ingest_raw_daily_rows(source="quantclass", rows=records)
        if not ingest_result.get("success"):
            return {
                'success': False,
                'error': ingest_result.get("error", "量化课堂 raw 导入失败"),
                'records_imported': 0,
                'errors': errors[:10],
            }

        reconcile_result = reconcile_daily_trade_dates(trade_dates=ingest_result.get("trade_dates") or [])
        if not reconcile_result.get("success"):
            return {
                'success': False,
                'error': reconcile_result.get("error", "量化课堂发布层对账失败"),
                'records_imported': int(ingest_result.get("rows") or 0),
                'errors': errors[:10],
            }

        records_imported = int(ingest_result.get("rows") or 0)
        
        logger.info(f"导入完成: {records_imported} 条记录")
        
        return {
            'success': True,
            'records_imported': records_imported,
            'stocks_count': df['股票代码'].nunique(),
            'min_trade_date': min_trade_date,
            'max_trade_date': max_trade_date,
            'errors': errors[:10]  # 只返回前10个错误
        }
        
    except Exception as e:
        logger.error(f"导入失败: {e}")
        return {
            'success': False,
            'error': str(e),
            'records_imported': 0
        }


def _normalize_quantclass_symbol(value) -> str:
    symbol = str(value or "").strip().upper()
    for prefix in ("SH", "SZ", "BJ"):
        if symbol.startswith(prefix) and symbol[2:].isdigit():
            return f"{symbol[2:]}.{prefix}"
    return symbol


def _safe_float(value):
    if pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_text(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


# 测试脚本
if __name__ == '__main__':
    import sys
    sys.path.insert(0, '/Users/wolf/Documents/DaiMa/TradingAgents-AShare-main')
    
    from api.database import get_db_ctx
    from api.quantclass_downloader import QuantClassDownloader
    
    # 配置
    API_KEY = os.getenv('QUANTCLASS_API_KEY', '')
    HID = os.getenv('QUANTCLASS_HID', '')
    
    # 创建下载器
    downloader = QuantClassDownloader(API_KEY, HID)
    
    # 下载股票日线数据
    print("开始下载量化课堂数据...")
    result = downloader.download_product('stock-trading-data', save_path='/tmp/quantclass_import')
    
    if not result['success']:
        print(f"下载失败: {result.get('error')}")
        sys.exit(1)
    
    csv_file = result['data_path']
    print(f"下载成功: {csv_file}")
    
    # 导入数据库
    print("\n开始导入数据库...")
    with get_db_ctx() as db:
        import_result = import_stock_daily_from_quantclass(db, csv_file)
    
    if import_result['success']:
        print(f"\n✅ 导入成功!")
        print(f"导入记录: {import_result['records_imported']}")
        print(f"股票数量: {import_result['stocks_count']}")
        if import_result.get('errors'):
            print(f"错误数量: {len(import_result['errors'])}")
    else:
        print(f"\n❌ 导入失败: {import_result.get('error')}")
