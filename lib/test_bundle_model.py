from datetime import datetime, timezone

import pandas as pd
import pytest

from lib.hk_quant_strategy_platform.bundle import Bundle, DatasetSpec


def test_bundle_validate_and_roundtrip_dict():
    b = Bundle(
        ticker="00001",
        asof=datetime(2026, 1, 1, tzinfo=timezone.utc),
        data={"a": pd.DataFrame({"x": [1, 2]})},
        meta={"k": "v"},
        errors={},
    )
    b.validate(require=["a"])

    b2 = Bundle.from_dict(b.to_dict())
    assert b2.ticker == "00001"
    assert b2.asof == b.asof
    assert list(b2.data.keys()) == ["a"]


def test_bundle_validate_raises_on_invalid_payloads():
    with pytest.raises(ValueError):
        Bundle(ticker="", asof=datetime.now(timezone.utc), data={}).validate()

    with pytest.raises(TypeError):
        Bundle(ticker="00001", asof="2026-01-01", data={}).validate()  # type: ignore[arg-type]

    with pytest.raises(TypeError):
        Bundle(ticker="00001", asof=datetime.now(timezone.utc), data={"a": 1}).validate()  # type: ignore[arg-type]

    with pytest.raises(KeyError):
        Bundle(ticker="00001", asof=datetime.now(timezone.utc), data={}).validate(require=["missing"])


def test_dataset_spec_defaults():
    spec = DatasetSpec(name="company_profile")
    assert spec.name == "company_profile"
    assert spec.description == ""
    assert spec.source == ""
    assert spec.is_snapshot is False
