from datetime import datetime, timezone

import pandas as pd

from lib.hk_quant_strategy_platform.bundle import Bundle
from lib.hk_quant_strategy_platform.refresh import (
    extract_tickers_from_spot,
    refresh_financial_indicator_snapshot_only,
    refresh_market,
    refresh_tickers,
    select_fetchers,
    write_bundle,
)
from lib.hk_quant_strategy_platform.storage import ParquetStore


def _bundle(ticker: str, key: str = "k", value: int = 1) -> Bundle:
    return Bundle(
        ticker=ticker,
        asof=datetime(2026, 1, 1, tzinfo=timezone.utc),
        data={key: pd.DataFrame({"v": [value]})},
        meta={},
        errors={},
    )


def test_extract_tickers_from_spot_filters_and_dedups():
    df = pd.DataFrame({"代码": ["1", "00001", "2"], "is_equity": [True, True, False]})
    assert extract_tickers_from_spot(df, only_equity=True) == ["00001"]


def test_select_fetchers_subset_and_error():
    fetchers = {"a": lambda _: pd.DataFrame(), "b": lambda _: pd.DataFrame()}
    sub, keys = select_fetchers(fetchers, ["b"])
    assert list(sub.keys()) == ["b"]
    assert keys == ["b"]


def test_write_bundle_update_datasets_bootstraps_new_ticker(tmp_path):
    store = ParquetStore(tmp_path)
    b = _bundle("00003", key="financial_indicator_snapshot", value=3)

    write_bundle(store, b, strategy="update_datasets", update_asof=True)

    assert store.exists_ticker("00003") is True
    assert store.list_datasets("00003") == ["financial_indicator_snapshot"]


def test_refresh_tickers_incremental_skip_and_new_ticker(tmp_path):
    store = ParquetStore(tmp_path)
    store.save_bundle(_bundle("00001", key="financial_indicator_snapshot", value=1), overwrite=True, prune=True)

    fetchers = {"financial_indicator_snapshot": lambda t: pd.DataFrame({"v": [int(t)]})}
    res = refresh_tickers(
        store,
        tickers=["00001", "00002"],
        fetchers=fetchers,
        datasets=["financial_indicator_snapshot"],
        incremental=True,
        progress=False,
    )

    assert res.total == 2
    assert res.skipped == 1
    assert res.succeeded == 1
    assert store.exists_ticker("00002") is True


def test_refresh_market_filters_user_tickers_by_universe(tmp_path):
    store = ParquetStore(tmp_path)
    store.save_global_df("hk_spot_all", pd.DataFrame({"代码": ["00001", "00002"], "is_equity": [True, False]}), overwrite=True)

    fetchers = {"financial_indicator_snapshot": lambda _: pd.DataFrame({"v": [1]})}
    _, res = refresh_market(
        store,
        fetchers,
        mode="tickers_only",
        tickers=["00001", "00002", "12345"],
        datasets=["financial_indicator_snapshot"],
        incremental=False,
        progress=False,
    )

    assert res is not None
    assert res.total == 1
    assert res.succeeded == 1


def test_refresh_financial_indicator_snapshot_only_wrapper(tmp_path):
    store = ParquetStore(tmp_path)
    store.save_global_df("hk_spot_all", pd.DataFrame({"代码": ["00005"], "is_equity": [True]}), overwrite=True)
    fetchers = {"financial_indicator_snapshot": lambda _: pd.DataFrame({"snap": [1]})}

    _, res = refresh_financial_indicator_snapshot_only(
        store,
        fetchers,
        tickers=["00005"],
        incremental=False,
        progress=False,
    )

    assert res.succeeded == 1
    assert store.list_datasets("00005") == ["financial_indicator_snapshot"]
