def test_core_utils_math():
    assert sum([1, 2, 3]) == 6


from api.core.stock_utils import normalize_symbol


def test_normalize_symbol_supports_beijing_exchange_suffix():
    assert normalize_symbol("899050.BJ") == "899050.BJ"


def test_normalize_symbol_infers_beijing_exchange_for_8_prefix():
    assert normalize_symbol("830000") == "830000.BJ"


from api.core.stock_utils import resolve_watchlist_identifier, search_cn_stock_by_name


def test_resolve_watchlist_identifier_recovers_wrong_suffix_when_code_is_unique():
    symbol, name, error = resolve_watchlist_identifier(
        "000001.SH",
        {"平安银行": "000001.SZ"},
        {"000001.SZ": "平安银行"},
    )
    assert error is None
    assert symbol == "000001.SZ"
    assert name == "平安银行"


def test_search_cn_stock_by_name_recovers_wrong_suffix_when_code_is_unique():
    assert search_cn_stock_by_name("000001.SH") == "000001.SZ"


def test_search_cn_stock_by_name_rejects_unknown_plain_text():
    assert search_cn_stock_by_name("海力士") is None
