from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd

from lib.hk_quant_strategy_platform.analyzer_function import (
    analyzer_basic_info_balance_sheet,
    compute_weighted_from_bundle,
    extract_basic_info,
    get_report_df,
    parse_bool,
    parse_float,
    run_analyzer,
    weighted_dot_on_df,
)
from lib.hk_quant_strategy_platform.bundle import Bundle


@dataclass
class _FakeStorage:
    bundles: dict

    def list_tickers(self):
        return list(self.bundles.keys()) + ["00002"]

    def load_bundle(self, ticker, load=None):
        if ticker == "00002":
            raise RuntimeError("missing")
        return self.bundles[ticker]


def _make_bundle() -> Bundle:
    bs = pd.DataFrame(
        {
            "STD_ITEM_CODE": ["100", "200", "300", "100", "300"],
            "AMOUNT": [10, 20, 8, 5, 3],
            "STD_REPORT_DATE": ["2024-12-31", "2024-12-31", "2024-12-31", "2024-12-31", "2023-12-31"],
            "SECURITY_NAME_ABBR": ["测试公司"] * 5,
        }
    )
    cp = pd.DataFrame({"公司名称": ["测试公司"], "所属行业": ["工业"], "员工人数": [100]})
    sp = pd.DataFrame({"是否沪港通标的": ["是"], "是否深港通标的": ["否"]})
    fi = pd.DataFrame({"总市值(港元)": [100]})
    return Bundle(
        ticker="00001",
        asof=datetime(2026, 1, 1, tzinfo=timezone.utc),
        data={
            "financial_bs_yearly": bs,
            "company_profile": cp,
            "security_profile": sp,
            "financial_indicator_snapshot": fi,
        },
    )


def test_parse_helpers_and_get_report_df():
    assert parse_float("HK$1,234") == 1234.0
    assert parse_float("nan") is None
    assert parse_bool("是") is True
    assert parse_bool("0") is False

    df = pd.DataFrame({"STD_REPORT_DATE": ["2024-12-31", "2023-12-31"], "x": [1, 2]})
    latest = get_report_df(df, offset=0)
    prev = get_report_df(df, offset=1)
    assert latest.iloc[0]["x"] == 1
    assert prev.iloc[0]["x"] == 2


def test_weighted_dot_and_compute_from_bundle():
    b = _make_bundle()
    weights = {
        "现金": {"codes": ["100"], "CashOnlyAsset": 1, "ConservativeAsset": 1, "NormalAsset": 1},
        "应收": {"codes": ["200"], "CashOnlyAsset": 0, "ConservativeAsset": 0.5, "NormalAsset": 1},
    }

    latest = get_report_df(b.data["financial_bs_yearly"], offset=0)
    totals = weighted_dot_on_df(latest, weights)
    assert totals["CashOnlyAsset"] == 15.0
    assert totals["ConservativeAsset"] == 25.0

    totals2 = compute_weighted_from_bundle(b, "financial_bs_yearly", offset=0, weights=weights)
    assert totals2["NormalAsset"] == 35.0


def test_extract_basic_info_and_full_analyzer():
    b = _make_bundle()
    asset_weights = {
        "现金": {"codes": ["100"], "CashOnlyAsset": 1, "ConservativeAsset": 1, "NormalAsset": 1},
        "应收": {"codes": ["200"], "CashOnlyAsset": 0, "ConservativeAsset": 0.5, "NormalAsset": 1},
    }
    liab_weights = {"负债": {"codes": ["300"], "PriorityLiab": 0.5, "TotalLiab": 1}}

    base = extract_basic_info(b)
    assert base["名称"] == "测试公司"
    assert base["港股通"] is True
    assert base["市值(港元)"] == 100.0

    out = analyzer_basic_info_balance_sheet(
        b,
        asset_weights=asset_weights,
        liab_weights=liab_weights,
    )
    assert out["CashOnlyAsset"] == 15.0
    assert out["Liab_Total"] == 8.0
    assert out["MOS_NormalAsset_minus_TotalLiab"] == (35.0 - 8.0) / 100.0


def test_run_analyzer_handles_skip_and_errors():
    b1 = _make_bundle()
    b3 = _make_bundle()
    b3.ticker = "00003"
    storage = _FakeStorage({"00001": b1, "00003": b3})

    def analyzer(bundle):
        if bundle.ticker == "00001":
            return None
        raise RuntimeError("bad bundle")

    df = run_analyzer(storage, analyzer, keep_skipped=True)
    assert set(df["ticker"].tolist()) == {"00001", "00002", "00003"}
    assert bool(df.loc[df["ticker"] == "00001", "skipped"].iloc[0]) is True
    assert bool(df.loc[df["ticker"] == "00002", "ok"].iloc[0]) is False
    assert bool(df.loc[df["ticker"] == "00003", "ok"].iloc[0]) is False
