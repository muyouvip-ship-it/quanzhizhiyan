"""通用数据导入器 - 支持多种数据类型。"""

import json
import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Any

import pandas as pd
from sqlalchemy import text

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
        logger.info(f"开始读取CSV文件: {csv_file_path}")
        frames = _read_csv_frames(csv_file_path)
        if not frames:
            return {
                'success': False,
                'error': f'未找到可导入的CSV文件: {csv_file_path}',
                'records_imported': 0
            }
        df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
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
        elif data_type == 'financial_data':
            return _import_financial_data(db_session, df)
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


def _read_csv_frames(csv_file_path: str) -> list[pd.DataFrame]:
    target = Path(csv_file_path)
    files = [target] if target.is_file() else sorted(target.rglob("*.csv")) if target.exists() else []
    frames: list[pd.DataFrame] = []
    for file_path in files:
        frame = _read_csv_with_fallback(file_path)
        if frame is not None and not frame.empty:
            frames.append(frame)
    return frames


def _read_csv_with_fallback(file_path: Path) -> pd.DataFrame | None:
    for encoding in ("gbk", "utf-8-sig", "utf-8"):
        for skiprows in (1, 0):
            try:
                frame = pd.read_csv(file_path, encoding=encoding, skiprows=skiprows)
            except Exception:
                continue
            if frame.empty:
                continue
            columns = {str(column).strip() for column in frame.columns}
            if columns & {"股票代码", "证券代码", "symbol", "代码"}:
                return frame
    return None


def _ensure_extended_market_tables(db_session) -> None:
    db_session.execute(text("""
        CREATE TABLE IF NOT EXISTS stock_chip_distribution (
            id BIGSERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            trade_date DATE NOT NULL,
            name VARCHAR(128),
            profit_ratio DOUBLE PRECISION,
            average_cost DOUBLE PRECISION,
            cost_concentration_90 DOUBLE PRECISION,
            cost_concentration_70 DOUBLE PRECISION,
            source VARCHAR(32) DEFAULT 'quantclass',
            raw_json TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """))
    db_session.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_stock_chip_distribution_symbol_date_source
        ON stock_chip_distribution(symbol, trade_date, source)
    """))
    db_session.execute(text("""
        CREATE TABLE IF NOT EXISTS stock_money_flow (
            id BIGSERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            trade_date DATE NOT NULL,
            name VARCHAR(128),
            medium_buy DOUBLE PRECISION,
            medium_sell DOUBLE PRECISION,
            large_buy DOUBLE PRECISION,
            large_sell DOUBLE PRECISION,
            retail_buy DOUBLE PRECISION,
            retail_sell DOUBLE PRECISION,
            institution_buy DOUBLE PRECISION,
            institution_sell DOUBLE PRECISION,
            main_net_inflow DOUBLE PRECISION,
            source VARCHAR(32) DEFAULT 'quantclass',
            raw_json TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """))
    db_session.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_stock_money_flow_symbol_date_source
        ON stock_money_flow(symbol, trade_date, source)
    """))
    db_session.execute(text("""
        CREATE TABLE IF NOT EXISTS stock_financial_snapshots (
            id BIGSERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            report_date DATE NOT NULL,
            name VARCHAR(128),
            net_profit_ttm DOUBLE PRECISION,
            cash_flow_ttm DOUBLE PRECISION,
            net_assets DOUBLE PRECISION,
            total_assets DOUBLE PRECISION,
            total_liabilities DOUBLE PRECISION,
            net_profit_quarter DOUBLE PRECISION,
            source VARCHAR(32) DEFAULT 'quantclass',
            raw_json TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """))
    db_session.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_stock_financial_snapshots_symbol_date_source
        ON stock_financial_snapshots(symbol, report_date, source)
    """))


def _first_value(row: pd.Series, aliases: tuple[str, ...]) -> Any:
    for alias in aliases:
        if alias in row.index:
            value = row.get(alias)
            if pd.notna(value) and str(value).strip() != "":
                return value
    return None


def _to_float_value(value: Any) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text_value = str(value).strip().replace(",", "").replace("%", "")
    if not text_value or text_value.lower() in {"nan", "none", "null", "--"}:
        return None
    try:
        parsed = float(text_value)
    except Exception:
        return None
    if "%" in str(value):
        parsed = parsed / 100.0
    return parsed


def _parse_date_value(value: Any) -> date | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text_value = str(value).strip()
    if not text_value:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(text_value[:10] if fmt != "%Y%m%d" else text_value[:8], fmt).date()
        except Exception:
            continue
    try:
        return pd.to_datetime(text_value).date()
    except Exception:
        return None


def _normalize_stock_symbol(value: Any) -> str:
    text_value = str(value or "").strip().upper()
    if not text_value:
        return ""
    if "." in text_value:
        code, suffix = text_value.split(".", 1)
        if len(code) == 6 and suffix in {"SH", "SZ", "BJ"}:
            return f"{code}.{suffix}"
    if text_value.startswith(("SH", "SZ", "BJ")) and len(text_value) >= 8:
        return f"{text_value[2:8]}.{text_value[:2]}"
    code = re.sub(r"\D", "", text_value)
    if len(code) != 6:
        return text_value
    if code.startswith(("60", "68", "90", "51", "52", "56", "58")):
        return f"{code}.SH"
    if code.startswith(("00", "30", "20", "15", "16", "18")):
        return f"{code}.SZ"
    if code.startswith(("43", "83", "87", "88", "92")):
        return f"{code}.BJ"
    return code


def _row_raw_json(row: pd.Series) -> str:
    payload = {}
    for key, value in row.to_dict().items():
        if pd.isna(value):
            continue
        if isinstance(value, (datetime, date)):
            payload[str(key)] = value.isoformat()
        else:
            payload[str(key)] = value
    return json.dumps(payload, ensure_ascii=False, default=str)


def _import_extended_rows(
    db_session,
    df: pd.DataFrame,
    *,
    table_name: str,
    date_column: str,
    insert_sql: str,
    mapper,
) -> dict:
    _ensure_extended_market_tables(db_session)
    records_imported = 0
    skipped = 0
    for _, row in df.iterrows():
        try:
            payload = mapper(row)
            if not payload:
                skipped += 1
                continue
            db_session.execute(text(insert_sql), payload)
            records_imported += 1
            if records_imported % 500 == 0:
                db_session.commit()
        except Exception as exc:
            skipped += 1
            logger.warning("导入%s行失败: %s", table_name, exc)
            continue
    db_session.commit()
    return {
        "success": records_imported > 0,
        "records_imported": records_imported,
        "skipped_records": skipped,
        "message": f"成功导入 {records_imported} 条记录，跳过 {skipped} 条",
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
    """导入筹码分布数据。"""

    def mapper(row: pd.Series) -> dict[str, Any] | None:
        symbol = _normalize_stock_symbol(_first_value(row, ("股票代码", "证券代码", "代码", "symbol")))
        trade_date = _parse_date_value(_first_value(row, ("交易日期", "日期", "date", "trade_date")))
        if not symbol or trade_date is None:
            return None
        return {
            "symbol": symbol,
            "trade_date": trade_date,
            "name": _first_value(row, ("股票名称", "证券简称", "名称", "name")),
            "profit_ratio": _to_float_value(_first_value(row, ("获利比例", "获利盘比例", "profit_ratio"))),
            "average_cost": _to_float_value(_first_value(row, ("平均成本", "平均持仓成本", "average_cost"))),
            "cost_concentration_90": _to_float_value(_first_value(row, ("90%成本集中度", "90成本集中度", "cost_concentration_90"))),
            "cost_concentration_70": _to_float_value(_first_value(row, ("70%成本集中度", "70成本集中度", "cost_concentration_70"))),
            "source": "quantclass",
            "raw_json": _row_raw_json(row),
        }

    return _import_extended_rows(
        db_session,
        df,
        table_name="stock_chip_distribution",
        date_column="trade_date",
        mapper=mapper,
        insert_sql="""
            INSERT INTO stock_chip_distribution
            (symbol, trade_date, name, profit_ratio, average_cost, cost_concentration_90, cost_concentration_70, source, raw_json)
            VALUES (:symbol, :trade_date, :name, :profit_ratio, :average_cost, :cost_concentration_90, :cost_concentration_70, :source, :raw_json)
            ON CONFLICT (symbol, trade_date, source) DO UPDATE SET
                name = EXCLUDED.name,
                profit_ratio = EXCLUDED.profit_ratio,
                average_cost = EXCLUDED.average_cost,
                cost_concentration_90 = EXCLUDED.cost_concentration_90,
                cost_concentration_70 = EXCLUDED.cost_concentration_70,
                raw_json = EXCLUDED.raw_json,
                updated_at = NOW()
        """,
    )


def _import_money_flow(db_session, df: pd.DataFrame) -> dict:
    """导入资金流数据。"""

    def mapper(row: pd.Series) -> dict[str, Any] | None:
        symbol = _normalize_stock_symbol(_first_value(row, ("股票代码", "证券代码", "代码", "symbol")))
        trade_date = _parse_date_value(_first_value(row, ("交易日期", "日期", "date", "trade_date")))
        if not symbol or trade_date is None:
            return None
        large_buy = _to_float_value(_first_value(row, ("大户资金买入额", "大资金买入额", "large_buy")))
        large_sell = _to_float_value(_first_value(row, ("大户资金卖出额", "大资金卖出额", "large_sell")))
        institution_buy = _to_float_value(_first_value(row, ("机构资金买入额", "institution_buy")))
        institution_sell = _to_float_value(_first_value(row, ("机构资金卖出额", "institution_sell")))
        main_net_inflow = None
        if large_buy is not None or large_sell is not None or institution_buy is not None or institution_sell is not None:
            main_net_inflow = (large_buy or 0.0) - (large_sell or 0.0) + (institution_buy or 0.0) - (institution_sell or 0.0)
        return {
            "symbol": symbol,
            "trade_date": trade_date,
            "name": _first_value(row, ("股票名称", "证券简称", "名称", "name")),
            "medium_buy": _to_float_value(_first_value(row, ("中户资金买入额", "中户资金流入额", "medium_buy"))),
            "medium_sell": _to_float_value(_first_value(row, ("中户资金卖出额", "中户资金流出额", "medium_sell"))),
            "large_buy": large_buy,
            "large_sell": large_sell,
            "retail_buy": _to_float_value(_first_value(row, ("散户资金买入额", "retail_buy"))),
            "retail_sell": _to_float_value(_first_value(row, ("散户资金卖出额", "retail_sell"))),
            "institution_buy": institution_buy,
            "institution_sell": institution_sell,
            "main_net_inflow": main_net_inflow,
            "source": "quantclass",
            "raw_json": _row_raw_json(row),
        }

    result = _import_extended_rows(
        db_session,
        df,
        table_name="stock_money_flow",
        date_column="trade_date",
        mapper=mapper,
        insert_sql="""
            INSERT INTO stock_money_flow
            (symbol, trade_date, name, medium_buy, medium_sell, large_buy, large_sell, retail_buy, retail_sell,
             institution_buy, institution_sell, main_net_inflow, source, raw_json)
            VALUES (:symbol, :trade_date, :name, :medium_buy, :medium_sell, :large_buy, :large_sell, :retail_buy, :retail_sell,
                    :institution_buy, :institution_sell, :main_net_inflow, :source, :raw_json)
            ON CONFLICT (symbol, trade_date, source) DO UPDATE SET
                name = EXCLUDED.name,
                medium_buy = EXCLUDED.medium_buy,
                medium_sell = EXCLUDED.medium_sell,
                large_buy = EXCLUDED.large_buy,
                large_sell = EXCLUDED.large_sell,
                retail_buy = EXCLUDED.retail_buy,
                retail_sell = EXCLUDED.retail_sell,
                institution_buy = EXCLUDED.institution_buy,
                institution_sell = EXCLUDED.institution_sell,
                main_net_inflow = EXCLUDED.main_net_inflow,
                raw_json = EXCLUDED.raw_json,
                updated_at = NOW()
        """,
    )
    if result.get("records_imported"):
        _merge_money_flow_into_daily(db_session)
    return result


def _import_financial_data(db_session, df: pd.DataFrame) -> dict:
    """导入财务快照数据。"""

    def mapper(row: pd.Series) -> dict[str, Any] | None:
        symbol = _normalize_stock_symbol(_first_value(row, ("股票代码", "证券代码", "代码", "symbol")))
        report_date = _parse_date_value(_first_value(row, ("报告期", "公告日期", "交易日期", "日期", "report_date", "date")))
        if not symbol or report_date is None:
            return None
        return {
            "symbol": symbol,
            "report_date": report_date,
            "name": _first_value(row, ("股票名称", "证券简称", "名称", "name")),
            "net_profit_ttm": _to_float_value(_first_value(row, ("净利润TTM", "归母净利润TTM", "net_profit_ttm"))),
            "cash_flow_ttm": _to_float_value(_first_value(row, ("现金流TTM", "经营现金流TTM", "cash_flow_ttm"))),
            "net_assets": _to_float_value(_first_value(row, ("净资产", "net_assets"))),
            "total_assets": _to_float_value(_first_value(row, ("总资产", "total_assets"))),
            "total_liabilities": _to_float_value(_first_value(row, ("总负债", "total_liabilities"))),
            "net_profit_quarter": _to_float_value(_first_value(row, ("净利润(单季)", "单季净利润", "net_profit_quarter"))),
            "source": "quantclass",
            "raw_json": _row_raw_json(row),
        }

    result = _import_extended_rows(
        db_session,
        df,
        table_name="stock_financial_snapshots",
        date_column="report_date",
        mapper=mapper,
        insert_sql="""
            INSERT INTO stock_financial_snapshots
            (symbol, report_date, name, net_profit_ttm, cash_flow_ttm, net_assets, total_assets,
             total_liabilities, net_profit_quarter, source, raw_json)
            VALUES (:symbol, :report_date, :name, :net_profit_ttm, :cash_flow_ttm, :net_assets, :total_assets,
                    :total_liabilities, :net_profit_quarter, :source, :raw_json)
            ON CONFLICT (symbol, report_date, source) DO UPDATE SET
                name = EXCLUDED.name,
                net_profit_ttm = EXCLUDED.net_profit_ttm,
                cash_flow_ttm = EXCLUDED.cash_flow_ttm,
                net_assets = EXCLUDED.net_assets,
                total_assets = EXCLUDED.total_assets,
                total_liabilities = EXCLUDED.total_liabilities,
                net_profit_quarter = EXCLUDED.net_profit_quarter,
                raw_json = EXCLUDED.raw_json,
                updated_at = NOW()
        """,
    )
    if result.get("records_imported"):
        _merge_financial_into_daily(db_session)
    return result


def _merge_money_flow_into_daily(db_session) -> None:
    db_session.execute(text("""
        UPDATE stock_daily_kline AS daily
        SET medium_buy = COALESCE(flow.medium_buy, daily.medium_buy),
            medium_sell = COALESCE(flow.medium_sell, daily.medium_sell),
            large_buy = COALESCE(flow.large_buy, daily.large_buy),
            large_sell = COALESCE(flow.large_sell, daily.large_sell),
            retail_buy = COALESCE(flow.retail_buy, daily.retail_buy),
            retail_sell = COALESCE(flow.retail_sell, daily.retail_sell),
            institution_buy = COALESCE(flow.institution_buy, daily.institution_buy),
            institution_sell = COALESCE(flow.institution_sell, daily.institution_sell),
            updated_at = NOW()
        FROM stock_money_flow AS flow
        WHERE daily.symbol = flow.symbol
          AND daily.trade_date = flow.trade_date
    """))
    db_session.commit()


def _merge_financial_into_daily(db_session) -> None:
    db_session.execute(text("""
        UPDATE stock_daily_kline AS daily
        SET net_profit_ttm = COALESCE(fin.net_profit_ttm, daily.net_profit_ttm),
            cash_flow_ttm = COALESCE(fin.cash_flow_ttm, daily.cash_flow_ttm),
            net_assets = COALESCE(fin.net_assets, daily.net_assets),
            total_assets = COALESCE(fin.total_assets, daily.total_assets),
            total_liabilities = COALESCE(fin.total_liabilities, daily.total_liabilities),
            net_profit_quarter = COALESCE(fin.net_profit_quarter, daily.net_profit_quarter),
            updated_at = NOW()
        FROM stock_financial_snapshots AS fin
        WHERE daily.symbol = fin.symbol
          AND daily.trade_date = fin.report_date
    """))
    db_session.commit()
