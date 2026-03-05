import pandas as pd
import pytest

import lib.hk_quant_strategy_platform.fetchers as fetchers_mod


def test_company_profile_normalizes_ticker(monkeypatch):
    called = {}

    def fake(symbol):
        called["symbol"] = symbol
        return pd.DataFrame({"ok": [1]})

    monkeypatch.setattr(fetchers_mod.ak, "stock_hk_company_profile_em", fake)
    out = fetchers_mod.company_profile("1")

    assert called["symbol"] == "00001"
    assert not out.empty


def test_dividend_history_uses_ths_code(monkeypatch):
    called = {}

    def fake(symbol):
        called["symbol"] = symbol
        return pd.DataFrame({"ok": [1]})

    monkeypatch.setattr(fetchers_mod.ak, "stock_hk_fhpx_detail_ths", fake)
    fetchers_mod.dividend_history("00700")
    assert called["symbol"] == "0700"


def test_hk_spot_all_raw_zfills_code(monkeypatch):
    monkeypatch.setattr(fetchers_mod.ak, "stock_hk_spot_em", lambda: pd.DataFrame({"代码": ["1", "700"]}))
    out = fetchers_mod._hk_spot_all_raw()
    assert out["代码"].tolist() == ["00001", "00700"]


def test_hk_spot_all_with_is_equity_error_modes(monkeypatch):
    monkeypatch.setattr(fetchers_mod, "_hk_spot_all_raw", lambda: pd.DataFrame({"代码": ["00001", "00002"]}))

    def classifier(ticker):
        if ticker == "00002":
            raise RuntimeError("probe failed")
        return type("R", (), {"is_equity": True})()

    out_null = fetchers_mod.hk_spot_all_with_is_equity(classifier, errors="null")
    assert bool(out_null["is_equity"].iloc[0]) is True
    assert pd.isna(out_null["is_equity"].iloc[1])

    out_false = fetchers_mod.hk_spot_all_with_is_equity(classifier, errors="false")
    assert out_false["is_equity"].tolist() == [True, False]

    with pytest.raises(RuntimeError):
        fetchers_mod.hk_spot_all_with_is_equity(classifier, errors="raise")


def test_spot_snapshot_from_global_filters_one_ticker():
    df = pd.DataFrame({"代码": ["00001", "00002"], "名称": ["A", "B"]})
    out = fetchers_mod.spot_snapshot_from_global("1", df)
    assert out.shape[0] == 1
    assert out.iloc[0]["代码"] == "00001"


def test_build_ticker_fetchers_passes_indicator(monkeypatch):
    calls = []

    def fake_financial_report(ticker, report_name, *, indicator):
        calls.append((ticker, report_name, indicator))
        return pd.DataFrame({"x": [1]})

    monkeypatch.setattr(fetchers_mod, "financial_report", fake_financial_report)
    fetchers = fetchers_mod.build_ticker_fetchers(indicator="报告期")
    fetchers["financial_bs_yearly"]("00001")

    assert calls[0] == ("00001", "资产负债表", "报告期")
