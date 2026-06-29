from pathlib import Path
from uuid import uuid4

from sqlalchemy import text

from api.database import get_db_ctx, init_db
from api.generic_importer import import_generic_data


def _write_quantclass_csv(path: Path, header: str, row: str) -> Path:
    path.write_text(f"量化课堂测试文件\n{header}\n{row}\n", encoding="gbk")
    return path


def test_import_chip_money_flow_and_financial_csv(tmp_path):
    init_db(force=True)
    suffix = f"{uuid4().int % 10000:04d}"
    chip_symbol = f"sh60{suffix}"
    money_symbol = f"sz00{suffix}"
    fin_symbol = f"sh68{suffix}"

    chip_csv = _write_quantclass_csv(
        tmp_path / "chip.csv",
        "股票代码,股票名称,交易日期,获利比例,平均成本,90%成本集中度,70%成本集中度",
        f"{chip_symbol},测试筹码,2026-06-22,0.62,12.34,0.18,0.11",
    )
    money_csv = _write_quantclass_csv(
        tmp_path / "money.csv",
        "股票代码,股票名称,交易日期,中户资金买入额,中户资金卖出额,大户资金买入额,大户资金卖出额,散户资金买入额,散户资金卖出额,机构资金买入额,机构资金卖出额",
        f"{money_symbol},测试资金,2026-06-22,100,50,200,80,300,90,400,120",
    )
    financial_csv = _write_quantclass_csv(
        tmp_path / "financial.csv",
        "股票代码,股票名称,交易日期,净利润TTM,现金流TTM,净资产,总资产,总负债,净利润(单季)",
        f"{fin_symbol},测试财务,2026-06-22,1000,800,5000,9000,4000,120",
    )

    with get_db_ctx() as db:
        chip_result = import_generic_data(db, str(chip_csv), "chip_data")
        money_result = import_generic_data(db, str(money_csv), "money_flow")
        financial_result = import_generic_data(db, str(financial_csv), "financial_data")

        assert chip_result["success"] is True
        assert money_result["success"] is True
        assert financial_result["success"] is True

        assert chip_result["records_imported"] == 1
        assert money_result["records_imported"] == 1
        assert financial_result["records_imported"] == 1

        assert db.execute(
            text("SELECT COUNT(*) FROM stock_chip_distribution WHERE symbol = :symbol"),
            {"symbol": f"{chip_symbol[2:]}.SH"},
        ).scalar() == 1
        assert db.execute(
            text("SELECT COUNT(*) FROM stock_money_flow WHERE symbol = :symbol"),
            {"symbol": f"{money_symbol[2:]}.SZ"},
        ).scalar() == 1
        assert db.execute(
            text("SELECT COUNT(*) FROM stock_financial_snapshots WHERE symbol = :symbol"),
            {"symbol": f"{fin_symbol[2:]}.SH"},
        ).scalar() == 1
