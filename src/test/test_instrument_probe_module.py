import pandas as pd

from lib.hk_quant_strategy_platform.instrument_probe import (
    _df_has_business_cols,
    _probe_fetch,
    is_equity_like_ticker,
)


def test_df_has_business_cols():
    assert _df_has_business_cols(pd.DataFrame({"ticker": ["00001"], "asof": ["x"], "dataset": ["d"]})) is False
    assert _df_has_business_cols(pd.DataFrame({"ticker": ["00001"], "业务列": [1]})) is True


def test_probe_fetch_retries_then_success(monkeypatch):
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("temp")
        return pd.DataFrame({"k": [1]})

    monkeypatch.setattr("lib.hk_quant_strategy_platform.instrument_probe.time.sleep", lambda _: None)
    monkeypatch.setattr("lib.hk_quant_strategy_platform.instrument_probe.random.uniform", lambda _a, _b: 0.0)

    ok, err = _probe_fetch(fn, tries=2, sleep_s=0.01)
    assert ok is True
    assert err is None


def test_is_equity_like_ticker_require_both_and_any():
    fetchers = {
        "security_profile": lambda _: pd.DataFrame({"name": ["x"]}),
        "financial_indicator_snapshot": lambda _: pd.DataFrame(),
    }

    strict = is_equity_like_ticker("00001", fetchers, require_both=True)
    loose = is_equity_like_ticker("00001", fetchers, require_both=False)

    assert strict.is_equity is False
    assert loose.is_equity is True
