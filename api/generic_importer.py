"""
通用数据导入器 - 支持多种数据类型
"""
import pandas as pd
from sqlalchemy import text
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


def _normalize_index_symbol(symbol: str) -> str:
    text = str(symbol or "").strip().upper()
    index_symbol_by_code = {
        "000001": "000001.SH",
        "399001": "399001.SZ",
        "399006": "399006.SZ",
        "000300": "000300.SH",
        "000905": "000905.SH",
        "000852": "000852.SH",
        "000688": "000688.SH",
        "899050": "899050.BJ",
    }
    if text in index_symbol_by_code.values():
        return text
    if text.startswith(("SH", "SZ", "BJ")) and len(text) >= 8:
        code = text[2:]
        candidate = index_symbol_by_code.get(code)
        if candidate and candidate.endswith(f".{text[:2]}"):
            return candidate
        return f"{code}.{text[:2]}"
    if "." in text:
        return text
    return index_symbol_by_code.get(text, text)


def import_generic_data(db_session, csv_file_path: str, data_type: str) -> dict:
    """
    通用数据导入函数
    
    Args:
        db_session: 数据库会话
        csv_file_path: CSV文件路径
        data_type: 数据类型
    
    Returns:
        {
            'success': bool,
            'records_imported': int,
            'message': str
        }
    """
    try:
        # 读取CSV文件
        logger.info(f"开始读取CSV文件: {csv_file_path}")
        df = pd.read_csv(csv_file_path, encoding='gbk', skiprows=1)
        
        logger.info(f"读取到 {len(df)} 行数据")
        
        # 根据数据类型选择导入方法
        if data_type == 'daily_kline':
            return _import_stock_daily(db_session, df)
        elif data_type == 'index_daily':
            return _import_index_daily(db_session, df)
        elif data_type == 'chip_data':
            return _import_chip_data(db_session, df)
        elif data_type == 'money_flow':
            return _import_money_flow(db_session, df)
        else:
            return {
                'success': False,
                'error': f'不支持的数据类型: {data_type}',
                'records_imported': 0
            }
            
    except Exception as e:
        logger.error(f"导入失败: {e}")
        return {
            'success': False,
            'error': str(e),
            'records_imported': 0
        }


def _import_stock_daily(db_session, df: pd.DataFrame) -> dict:
    """导入股票日线数据"""
    try:
        records_imported = 0
        
        for idx, row in df.iterrows():
            try:
                # 处理股票代码
                symbol = row['股票代码']
                if symbol.startswith('bj'):
                    pass  # 北交所股票保留原代码
                elif symbol.startswith(('sh', 'sz')):
                    symbol = symbol[2:]  # 去掉沪深前缀
                
                # 插入数据库
                insert_query = text("""
                    INSERT INTO stock_daily_kline 
                    (symbol, trade_date, open, high, low, close, volume, amount)
                    VALUES (:symbol, :trade_date, :open, :high, :low, :close, :volume, :amount)
                    ON CONFLICT (symbol, trade_date) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        amount = EXCLUDED.amount,
                        updated_at = NOW()
                """)
                
                db_session.execute(insert_query, {
                    "symbol": symbol,
                    "trade_date": row['交易日期'],
                    "open": float(row['开盘价']) if pd.notna(row['开盘价']) else None,
                    "high": float(row['最高价']) if pd.notna(row['最高价']) else None,
                    "low": float(row['最低价']) if pd.notna(row['最低价']) else None,
                    "close": float(row['收盘价']) if pd.notna(row['收盘价']) else None,
                    "volume": int(row['成交量']) if pd.notna(row['成交量']) else None,
                    "amount": float(row['成交额']) if pd.notna(row['成交额']) else None
                })
                
                records_imported += 1
                
                # 每100条提交一次
                if records_imported % 100 == 0:
                    db_session.commit()
                    
            except Exception as e:
                continue
        
        db_session.commit()
        
        return {
            'success': True,
            'records_imported': records_imported,
            'message': f'成功导入 {records_imported} 条记录'
        }
        
    except Exception as e:
        logger.error(f"导入股票日线数据失败: {e}")
        return {
            'success': False,
            'error': str(e),
            'records_imported': 0
        }


def _import_index_daily(db_session, df: pd.DataFrame) -> dict:
    """导入指数日线数据"""
    try:
        records_imported = 0
        
        for idx, row in df.iterrows():
            try:
                # 处理指数代码
                symbol = row.get('指数代码', row.get('股票代码', ''))
                if symbol.startswith('sh'):
                    symbol = symbol[2:]
                elif symbol.startswith('sz'):
                    symbol = symbol[2:]
                
                symbol = _normalize_index_symbol(symbol)

                # 插入数据库
                insert_query = text("""
                    INSERT INTO index_daily_kline
                    (symbol, trade_date, open, high, low, close, volume, amount, source)
                    VALUES (:symbol, :trade_date, :open, :high, :low, :close, :volume, :amount, :source)
                    ON CONFLICT (symbol, trade_date) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        amount = EXCLUDED.amount,
                        source = EXCLUDED.source,
                        updated_at = NOW()
                """)
                
                db_session.execute(insert_query, {
                    "symbol": symbol,
                    "trade_date": row.get('交易日期', row.get('日期')),
                    "open": float(row.get('开盘价', row.get('开盘'))) if pd.notna(row.get('开盘价', row.get('开盘'))) else None,
                    "high": float(row.get('最高价', row.get('最高'))) if pd.notna(row.get('最高价', row.get('最高'))) else None,
                    "low": float(row.get('最低价', row.get('最低'))) if pd.notna(row.get('最低价', row.get('最低'))) else None,
                    "close": float(row.get('收盘价', row.get('收盘'))) if pd.notna(row.get('收盘价', row.get('收盘'))) else None,
                    "volume": int(row.get('成交量', 0)) if pd.notna(row.get('成交量')) else None,
                    "amount": float(row.get('成交额', 0)) if pd.notna(row.get('成交额')) else None,
                    "source": "quantclass",
                })
                
                records_imported += 1
                
                if records_imported % 100 == 0:
                    db_session.commit()
                    
            except Exception as e:
                continue
        
        db_session.commit()
        
        return {
            'success': True,
            'records_imported': records_imported,
            'message': f'成功导入 {records_imported} 条指数数据'
        }
        
    except Exception as e:
        logger.error(f"导入指数数据失败: {e}")
        return {
            'success': False,
            'error': str(e),
            'records_imported': 0
        }


def _import_chip_data(db_session, df: pd.DataFrame) -> dict:
    """导入筹码数据（暂未实现完整表结构，返回未支持状态）。"""
    logger.warning(
        "筹码数据导入功能尚未实现完整表结构，跳过 %d 条记录",
        len(df) if df is not None else 0,
    )
    return {
        "success": False,
        "error": "筹码数据导入功能尚未实现，需要先创建筹码数据表结构",
        "records_imported": 0,
    }


def _import_money_flow(db_session, df: pd.DataFrame) -> dict:
    """导入资金流数据（暂未实现完整表结构，返回未支持状态）。"""
    logger.warning(
        "资金流数据导入功能尚未实现完整表结构，跳过 %d 条记录",
        len(df) if df is not None else 0,
    )
    return {
        "success": False,
        "error": "资金流数据导入功能尚未实现，需要先创建资金流数据表结构",
        "records_imported": 0,
    }
