from datetime import datetime, timezone

import pandas as pd

from lib.hk_quant_strategy_platform.bundle import Bundle
from lib.hk_quant_strategy_platform.diagnostic_utils import (
    bundle_key_report,
    compare_bundles,
    df_fingerprint,
    df_schema,
    preview_df,
)


def _bundle(tag: str, x: int) -> Bundle:
    return Bundle(
        ticker="00001",
        asof=datetime(2026, 1, 1, tzinfo=timezone.utc),
        data={tag: pd.DataFrame({"v": [x, x + 1]})},
        meta={},
        errors={},
    )


def test_preview_schema_and_fingerprint():
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    assert preview_df(df, n=1).shape == (1, 2)
    schema = df_schema(df)
    assert "a" in schema and "b" in schema
    assert df_fingerprint(df) == df_fingerprint(df.copy())


def test_compare_bundles_and_key_report():
    b1 = _bundle("k", 1)
    b2 = _bundle("k", 99)

    diffs = compare_bundles(b1, b2, verbose=False)
    assert len(diffs) >= 1
    assert any(d.path.endswith(".values") for d in diffs)
    assert bundle_key_report(b1) == ["k"]
