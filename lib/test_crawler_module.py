from datetime import datetime, timezone

import pandas as pd
import pytest

from lib.hk_quant_strategy_platform.crawler import _standardize_df, crawl_ticker


def test_standardize_df_adds_trace_and_handles_empty():
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)

    out = _standardize_df(pd.DataFrame({"x": [1]}), ticker="00001", asof=ts, dataset="d")
    assert out.loc[0, "ticker"] == "00001"
    assert out.loc[0, "dataset"] == "d"

    out_empty = _standardize_df(pd.DataFrame(), ticker="00001", asof=ts, dataset="d")
    assert list(out_empty.columns) == ["ticker", "asof", "dataset"]


def test_crawl_ticker_collects_errors_without_stopping():
    fetchers = {
        "ok": lambda _: pd.DataFrame({"v": [1]}),
        "bad": lambda _: (_ for _ in ()).throw(RuntimeError("boom")),
    }

    bundle, report = crawl_ticker("1", fetchers, add_trace=True, stop_on_error=False)

    assert bundle.ticker == "00001"
    assert set(bundle.data.keys()) == {"ok", "bad"}
    assert report.ok["ok"] is True
    assert report.ok["bad"] is False
    assert "bad" in bundle.errors


def test_crawl_ticker_stop_on_error_with_require_raises():
    fetchers = {
        "bad": lambda _: (_ for _ in ()).throw(RuntimeError("boom")),
        "ok": lambda _: pd.DataFrame({"v": [1]}),
    }

    with pytest.raises(KeyError):
        crawl_ticker("1", fetchers, stop_on_error=True, require=["bad", "ok"])
