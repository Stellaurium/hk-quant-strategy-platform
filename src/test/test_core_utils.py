from datetime import datetime, timezone

import pandas as pd
import pytest

from lib.hk_quant_strategy_platform.utils import (
    DatasetError,
    add_trace_cols,
    df_brief,
    ensure_dir,
    hk_to_ths_code,
    normalize_hk_ticker,
    require_columns,
    safe_empty_df,
)


def test_normalize_hk_ticker_and_hk_to_ths_code():
    assert normalize_hk_ticker("HK00001") == "00001"
    assert normalize_hk_ticker("700") == "00700"
    assert hk_to_ths_code("00700") == "0700"


@pytest.mark.parametrize("bad", ["", "abc", "100000"])
def test_normalize_hk_ticker_invalid(bad):
    with pytest.raises(Exception):
        normalize_hk_ticker(bad)


def test_add_trace_cols_copy_and_inplace():
    src = pd.DataFrame({"v": [1]})
    asof = datetime(2026, 1, 1, tzinfo=timezone.utc)

    out = add_trace_cols(src, ticker="00001", asof=asof, inplace=False)
    assert "ticker" in out.columns and "asof" in out.columns
    assert "ticker" not in src.columns

    out2 = add_trace_cols(src, ticker="00002", asof="2026-01-02", inplace=True)
    assert out2 is src
    assert src.loc[0, "ticker"] == "00002"


def test_require_columns_df_brief_and_helpers(tmp_path):
    p = ensure_dir(tmp_path / "nested")
    assert p.exists()

    df = safe_empty_df(["a", "b"])
    assert list(df.columns) == ["a", "b"]

    with pytest.raises(DatasetError):
        require_columns(pd.DataFrame({"a": [1]}), ["a", "b"], where="unit")

    summary = df_brief(pd.DataFrame({"a": [1], "b": [2]}), n=1)
    assert summary["shape"] == (1, 2)
    assert summary["columns"] == ["a", "b"]
    assert isinstance(summary["head"], list)
