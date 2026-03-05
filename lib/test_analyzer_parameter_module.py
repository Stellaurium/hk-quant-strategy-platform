from lib.hk_quant_strategy_platform.analyzer_parameter import (
    DEFAULT_ASSET_WEIGHTS,
    DEFAULT_FCF_WEIGHTS,
    DEFAULT_LIAB_WEIGHTS,
)


def _assert_weights_shape(weights: dict):
    assert isinstance(weights, dict)
    assert len(weights) > 0
    for name, spec in list(weights.items())[:30]:
        assert isinstance(name, str)
        assert isinstance(spec, dict)
        assert "codes" in spec
        assert isinstance(spec["codes"], list)


def test_default_weights_have_expected_structure():
    _assert_weights_shape(DEFAULT_ASSET_WEIGHTS)
    _assert_weights_shape(DEFAULT_LIAB_WEIGHTS)
    _assert_weights_shape(DEFAULT_FCF_WEIGHTS)


def test_liab_and_asset_regime_keys_exist_in_samples():
    asset_regimes = set()
    for spec in DEFAULT_ASSET_WEIGHTS.values():
        asset_regimes.update(k for k in spec.keys() if k != "codes")

    liab_regimes = set()
    for spec in DEFAULT_LIAB_WEIGHTS.values():
        liab_regimes.update(k for k in spec.keys() if k != "codes")

    assert {"CashOnlyAsset", "ConservativeAsset", "NormalAsset"}.issubset(asset_regimes)
    assert {"PriorityLiab", "TotalLiab"}.issubset(liab_regimes)
