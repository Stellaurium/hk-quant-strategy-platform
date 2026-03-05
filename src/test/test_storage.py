# 单元测试文件, 测试了storage文件的各个函数, 基本上可用
# 使用pytest, 只有test开头or结尾的文件的 test开头的函数才是测试函数

import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from lib.hk_quant_strategy_platform.storage import ParquetStore, StorageError
from lib.hk_quant_strategy_platform.bundle import Bundle
from lib.hk_quant_strategy_platform.diagnostic_utils import compare_bundles


# ----------------------------
# helpers: deterministic bundle / dfs
# ----------------------------

def _dt(i: int = 0) -> datetime:
    # deterministic "asof"
    return datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc).replace(microsecond=i)


def make_df(tag: str, n: int = 3) -> pd.DataFrame:
    maybe_pattern = [None, "a", None]
    maybe = [maybe_pattern[i % len(maybe_pattern)] for i in range(n)]

    return pd.DataFrame(
        {
            "tag": [tag] * n,
            "i": list(range(n)),
            "x": [0.1 * k for k in range(n)],
            "ts": [pd.Timestamp("2020-01-01") + pd.Timedelta(days=k) for k in range(n)],
            "maybe": maybe,
        }
    )



def make_bundle(ticker: str, keys=("a", "b", "c")) -> Bundle:
    data = {k: make_df(f"{ticker}-{k}", n=5) for k in keys}
    return Bundle(
        ticker=str(ticker).zfill(5),
        asof=_dt(1),
        data=data,
        meta={"indicator": "年度", "fetcher_count": len(keys)},
        errors={},
    )


def make_store(tmp_path: Path) -> ParquetStore:
    return ParquetStore(tmp_path)


# ----------------------------
# pytest fixtures
# ----------------------------

@pytest.fixture()
def store(tmp_path: Path) -> ParquetStore:
    return make_store(tmp_path)


# ============================================================
# Tests
# ============================================================

def test_roundtrip_save_load_bundle(store: ParquetStore):
    b1 = make_bundle("00001", keys=("company_profile", "financial_bs_yearly"))
    store.save_bundle(b1, overwrite=True, prune=True)

    b2 = store.load_bundle("00001")
    diffs = compare_bundles(b1, b2, ignore_cols=[], check_values=True, verbose=False)
    assert diffs == []


def test_load_partial_keys(store: ParquetStore):
    b1 = make_bundle("00001", keys=("a", "b", "c"))
    store.save_bundle(b1)

    b2 = store.load_bundle("00001", load=["a"])
    assert set(b2.data.keys()) == {"a"}
    # spot-check content
    assert b2.data["a"].shape == b1.data["a"].shape


def test_update_dataset_replace_existing(store: ParquetStore):
    b1 = make_bundle("00001", keys=("a", "b"))
    store.save_bundle(b1)

    # replace "b"
    df_new = make_df("00001-b-NEW", n=4)
    store.update_dataset("00001", "b", df_new, must_exist=True, update_asof=False)

    b2 = store.load_bundle("00001")
    # "a" unchanged
    diffs_a = compare_bundles(
        Bundle(ticker=b1.ticker, asof=b1.asof, data={"a": b1.data["a"]}, meta=b1.meta, errors=b1.errors),
        Bundle(ticker=b2.ticker, asof=b2.asof, data={"a": b2.data["a"]}, meta=b2.meta, errors=b2.errors),
        check_values=True,
        verbose=False,
    )
    assert diffs_a == []

    # "b" updated
    pd.testing.assert_frame_equal(
        b2.data["b"].reset_index(drop=True),
        df_new.reset_index(drop=True),
        check_dtype=False,
        check_like=True,
    )

    # asof should remain unchanged by default
    assert b2.asof == b1.asof


def test_update_dataset_update_asof_true(store: ParquetStore):
    b1 = make_bundle("00001", keys=("a",))
    store.save_bundle(b1)
    df_new = make_df("00001-a-NEW", n=2)

    store.update_dataset("00001", "a", df_new, must_exist=True, update_asof=True)

    b2 = store.load_bundle("00001")
    assert b2.asof != b1.asof  # bumped


def test_update_dataset_must_exist_raises(store: ParquetStore):
    b1 = make_bundle("00001", keys=("a",))
    store.save_bundle(b1)

    with pytest.raises(KeyError):
        store.update_dataset("00001", "not_exist_key", make_df("x"), must_exist=True)


def test_update_dataset_must_exist_false_allows_new_key(store: ParquetStore):
    b1 = make_bundle("00001", keys=("a",))
    store.save_bundle(b1)

    df_new = make_df("NEW")  # 默认 n=3
    store.update_dataset("00001", "new_key", df_new, must_exist=False)

    b2 = store.load_bundle("00001")
    assert "new_key" in b2.data

    pd.testing.assert_frame_equal(
        b2.data["new_key"].reset_index(drop=True),
        df_new.reset_index(drop=True),
        check_dtype=False,
        check_like=True,
    )


def test_delete_dataset(store: ParquetStore):
    b1 = make_bundle("00001", keys=("a", "b"))
    store.save_bundle(b1)

    store.delete_dataset("00001", "b", must_exist=True)
    b2 = store.load_bundle("00001")
    assert "b" not in b2.data
    assert "a" in b2.data

    # deleting again should be no-op if must_exist=False
    store.delete_dataset("00001", "b", must_exist=False)


def test_delete_ticker(store: ParquetStore):
    b1 = make_bundle("00001", keys=("a",))
    store.save_bundle(b1)

    assert store.exists_ticker("00001") is True
    store.delete_ticker("00001", must_exist=True)
    assert store.exists_ticker("00001") is False

    with pytest.raises(StorageError):
        store.load_bundle("00001")


def test_prune_removes_stale_datasets(store: ParquetStore):
    b1 = make_bundle("00001", keys=("a", "b", "c"))
    store.save_bundle(b1, prune=True)

    # overwrite with only a,b
    b2 = make_bundle("00001", keys=("a", "b"))
    store.save_bundle(b2, prune=True)

    # file-level check: c.parquet should be gone
    assert store.exists_dataset("00001", "c") is False

    # manifest-level check: no c
    m = store.get_manifest("00001")
    assert "c" not in m.get("datasets", {})


def test_globals_roundtrip(store: ParquetStore):
    df = pd.DataFrame({"代码": ["00001", "00882"], "最新价": [10.0, 2.0]})
    store.save_global_df("hk_spot_all", df, overwrite=True)

    df2 = store.load_global_df("hk_spot_all")
    pd.testing.assert_frame_equal(df2.reset_index(drop=True), df.reset_index(drop=True), check_dtype=False)

    keys = store.list_globals()
    assert "hk_spot_all" in keys

    store.delete_global("hk_spot_all", must_exist=True)
    assert "hk_spot_all" not in store.list_globals()


def test_cross_session_readability(tmp_path: Path):
    # save in one store instance
    store1 = ParquetStore(tmp_path)
    b1 = make_bundle("00001", keys=("a", "b"))
    store1.save_bundle(b1)

    # new instance simulates "restart"
    store2 = ParquetStore(tmp_path)
    b2 = store2.load_bundle("00001")

    diffs = compare_bundles(b1, b2, check_values=True, verbose=False)
    assert diffs == []


def test_errors_semantics(store: ParquetStore):
    # non-existent ticker
    with pytest.raises(StorageError):
        store.load_bundle("99999")

    # delete non-existent ticker
    store.delete_ticker("99999", must_exist=False)  # no-op
    with pytest.raises(StorageError):
        store.delete_ticker("99999", must_exist=True)

    # overwrite=False behavior (dataset exists)
    b1 = make_bundle("00001", keys=("a",))
    store.save_bundle(b1, overwrite=True)
    with pytest.raises(Exception):
        store.save_bundle(b1, overwrite=False)
