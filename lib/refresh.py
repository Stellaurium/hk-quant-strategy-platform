# lib/stock_data_analysis/refresh.py
"""
Batch refresh pipeline for HK stock datasets:
- update global hk_spot_all once
- build ticker universe from stored hk_spot_all
- crawl per-ticker datasets via registry fetchers
- persist via ParquetStore (save_bundle or update_dataset strategy)

Design goals:
1) Most operations are "read" and "full overwrite write". No partial in-place edits of parquet rows.
2) Persist Bundle structure: ticker + asof + {dataset_name -> DataFrame} (+ meta/errors in manifest).
3) Support updating only one dataset (e.g. financial_indicator_snapshot) safely.
4) Support resume: skip tickers that are already "complete" for the requested dataset keys.
5) Allow dataset subset update without deleting other existing datasets (avoid prune mistakes).
"""

from __future__ import annotations

import time
import random
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Literal

import pandas as pd

from .storage import ParquetStore, StorageError
from .crawler import crawl_ticker
from .bundle import Bundle
from .utils import normalize_hk_ticker



# Fetchers registry type:
Fetcher = Callable[[str], pd.DataFrame]
Fetchers = Dict[str, Fetcher]

WriteStrategy = Literal["save_bundle", "update_datasets"]
Mode = Literal["global_only", "tickers_only", "global_then_tickers"]


# ----------------------------
# Results / reporting
# ----------------------------

@dataclass(slots=True)
class RefreshResult:
    """Summary of a batch run."""
    total: int
    succeeded: int
    failed: int
    skipped: int
    # per ticker error
    errors: Dict[str, str]
    # optional per ticker processed keys (for debugging)
    processed: Dict[str, List[str]]

    def ok(self) -> bool:
        return self.failed == 0


# ----------------------------
# Global: hk_spot_all
# ----------------------------

def update_global_hk_spot_all(
    store: ParquetStore,
    *,
    hk_spot_all_fetcher: Callable[[], pd.DataFrame],
    overwrite: bool = True,
    global_key: str = "hk_spot_all",
) -> pd.DataFrame:
    """
    Update and persist the global all-market spot table once.

    Parameters
    ----------
    store:
        ParquetStore instance.
    hk_spot_all_fetcher:
        Callable that returns the full market spot DataFrame. Typically wraps ak.stock_hk_spot_em().
    overwrite:
        Whether to overwrite existing global parquet.
    global_key:
        Key under store globals namespace.

    Returns
    -------
    DataFrame
        The fetched spot table.
    """
    df = hk_spot_all_fetcher()
    store.save_global_df(global_key, df, overwrite=overwrite)
    return df


def extract_tickers_from_spot(
    df: pd.DataFrame,
    *,
    only_equity: bool = True,
    equity_col: str = "is_equity",
) -> List[str]:
    """
    Extract normalized 5-digit tickers from hk_spot_all dataframe.

    If only_equity=True and equity_col exists, filter to equity tickers.
    """
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return []

    col_candidates = ["代码", "SECURITY_CODE", "stock", "ticker"]
    col = next((c for c in col_candidates if c in df.columns), None)
    if col is None:
        raise ValueError(f"Cannot find ticker column in hk_spot_all df. Tried: {col_candidates}")

    # optional filter
    if only_equity and equity_col in df.columns:
        df = df[df[equity_col].fillna(False).astype(bool)]

    tickers = [normalize_hk_ticker(x) for x in df[col].astype(str).tolist()]

    # unique while keeping order
    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def get_universe_tickers(
    store: ParquetStore,
    *,
    refresh_spot: bool = False,
    hk_spot_all_fetcher: Optional[Callable[[], pd.DataFrame]] = None,
    global_key: str = "hk_spot_all",
) -> List[str]:
    """
    Get the universe tickers list.

    Behavior:
    - If refresh_spot=True, will fetch and overwrite global hk_spot_all first (requires hk_spot_all_fetcher).
    - Else tries to load existing global hk_spot_all from store.

    Returns
    -------
    List[str] normalized 5-digit tickers
    """
    if refresh_spot:
        if hk_spot_all_fetcher is None:
            raise ValueError("refresh_spot=True requires hk_spot_all_fetcher.")
        df = update_global_hk_spot_all(
            store, hk_spot_all_fetcher=hk_spot_all_fetcher, overwrite=True, global_key=global_key
        )
    else:
        df = store.load_global_df(global_key)

    return extract_tickers_from_spot(df)


# ----------------------------
# Completeness & selection
# ----------------------------

def select_fetchers(fetchers: Fetchers, datasets: Optional[Sequence[str]]) -> Tuple[Fetchers, List[str]]:
    """
    Select a subset of fetchers by dataset keys.

    datasets=None -> full fetchers
    datasets=[...] -> subset
    """
    if datasets is None:
        keys = list(fetchers.keys())
        return fetchers, keys

    keys = list(datasets)
    missing = [k for k in keys if k not in fetchers]
    if missing:
        raise KeyError(f"Unknown dataset keys in selection: {missing}. Available: {sorted(fetchers.keys())}")

    return {k: fetchers[k] for k in keys}, keys


def is_ticker_complete(store: ParquetStore, ticker: str, required_keys: Sequence[str]) -> bool:
    """
    Check whether a ticker already contains all required dataset keys in storage.

    Used for resume / incremental mode.

    Notes:
    - This is "schema completeness", not "data freshness".
    - If a run crashes mid-write, storage will often be incomplete; rerun will re-crawl.
    """
    t = normalize_hk_ticker(ticker)
    if not store.exists_ticker(t):
        return False

    have = set(store.list_datasets(t))
    return all(k in have for k in required_keys)


# ----------------------------
# Writing strategies
# ----------------------------

def write_bundle(
    store: ParquetStore,
    bundle: Bundle,
    *,
    strategy: WriteStrategy,
    prune: bool = False,
    update_asof: bool = False,
) -> None:
    """
    Persist a Bundle to storage using a selected strategy.

    strategy="save_bundle":
        - store.save_bundle(bundle, overwrite=True, prune=prune)
        - Suitable for full refresh / full overwrite.
        - Danger: prune=True will delete datasets not present in current bundle.

    strategy="update_datasets":
        - For each dataset in bundle.data: store.update_dataset(ticker, name, df, must_exist=False, update_asof=update_asof)
        - Suitable for partial updates / snapshot refresh (safe, does not delete other datasets).
        - Recommended default when datasets subset is used.
    """
    if strategy == "save_bundle":
        store.save_bundle(bundle, overwrite=True, prune=prune)
        return

    if strategy == "update_datasets":
        for name, df in bundle.data.items():
            store.update_dataset(bundle.ticker, name, df, must_exist=False, update_asof=update_asof)
        return

    raise ValueError(f"Unknown write strategy: {strategy}")


# ----------------------------
# Main per-ticker refresh
# ----------------------------

def refresh_tickers(
    store: ParquetStore,
    tickers: Sequence[str],
    fetchers: Fetchers,
    *,
    # dataset selection
    datasets: Optional[Sequence[str]] = None,
    # resume / "incremental": if complete -> skip; if incomplete -> FULL crawl (simpler)
    incremental: bool = True,
    # writing options
    strategy: Optional[WriteStrategy] = None,
    prune: bool = False,
    update_asof_on_update_datasets: bool = False,
    # runtime options
    skip_errors: bool = True,
    sleep_s: float = 0.0,
    max_tickers: Optional[int] = None,
    progress: bool = True,
) -> RefreshResult:
    """
    Crawl and persist per-ticker datasets.

    This function is the core worker that supports:
    - Full refresh: datasets=None, incremental=False, strategy="save_bundle", prune=True/False
    - Resume: incremental=True (skip tickers already complete)
    - Subset update: datasets=[...], strategy defaults to "update_datasets" (safe), prune forced to False

    Parameters
    ----------
    store:
        ParquetStore instance.
    tickers:
        Iterable of tickers (string or digits); will be normalized to 5 digits.
    fetchers:
        Full registry: name -> fetcher(ticker)->DataFrame
    datasets:
        None for all. Or a list of dataset keys to refresh only these.
    incremental:
        If True: skip tickers already complete for required_keys.
        If False: always crawl/write regardless of existing data.
    strategy:
        If None:
          - datasets is None -> "save_bundle"
          - datasets is not None -> "update_datasets"
    prune:
        Only applies to strategy="save_bundle".
        IMPORTANT: subset update should NOT prune; this function will force prune=False if datasets is not None.
    update_asof_on_update_datasets:
        When strategy="update_datasets", whether to bump asof for each update_dataset call.
        Recommended True when refreshing snapshot-like datasets (e.g. financial_indicator_snapshot).
    skip_errors:
        If True, continue on per-ticker exceptions; errors captured in result.
        If False, re-raise immediately.
    sleep_s:
        Optional delay between tickers to be polite to upstream sources.
    max_tickers:
        Debug limiter.
    progress:
        Whether to print a light progress (no tqdm dependency).
    """
    fetchers_use, required_keys = select_fetchers(fetchers, datasets)

    # Decide default strategy
    if strategy is None:
        strategy = "save_bundle" if datasets is None else "update_datasets"

    # Enforce safety: subset update should not prune
    if datasets is not None and prune:
        # force to False to prevent accidental deletion
        prune = False

    # Normalize tickers, keep order unique
    seen = set()
    normalized: List[str] = []
    for x in tickers:
        t = normalize_hk_ticker(x)
        if t not in seen:
            seen.add(t)
            normalized.append(t)

    if max_tickers is not None:
        normalized = normalized[:max_tickers]

    errors: Dict[str, str] = {}
    processed: Dict[str, List[str]] = {}
    succeeded = failed = skipped = 0

    total = len(normalized)
    start = time.time()

    for idx, t in enumerate(normalized, start=1):
        if incremental and is_ticker_complete(store, t, required_keys):
            skipped += 1
            if progress:
                print(f"[{idx}/{total}] {t} skip (complete)")
            continue

        if progress:
            print(f"[{idx}/{total}] {t} crawl keys={required_keys}")

        try:
            # As per your preference: if incomplete, just FULL crawl the requested keys (simple).
            bundle, report = crawl_ticker(t, fetchers_use)
            processed[t] = list(bundle.data.keys())

            write_bundle(
                store,
                bundle,
                strategy=strategy,
                prune=prune if strategy == "save_bundle" else False,
                update_asof=update_asof_on_update_datasets if strategy == "update_datasets" else False,
            )
            succeeded += 1

        except Exception as e:
            failed += 1
            errors[t] = repr(e)
            if progress:
                print(f"  !! error: {errors[t]}")
            if not skip_errors:
                raise

        if sleep_s > 0:
            jitter = random.uniform(0, sleep_s * 0.3)  # 0~30% 抖动
            time.sleep(sleep_s + jitter)

    if progress:
        elapsed = time.time() - start
        print(f"Done. total={total} ok={succeeded} failed={failed} skipped={skipped} elapsed={elapsed:.2f}s")

    return RefreshResult(
        total=total,
        succeeded=succeeded,
        failed=failed,
        skipped=skipped,
        errors=errors,
        processed=processed,
    )


# ----------------------------
# High-level orchestrator
# ----------------------------

def refresh_market(
    store: ParquetStore,
    fetchers: Fetchers,
    *,
    mode: Mode = "global_then_tickers",
    # global
    refresh_spot: bool = False,
    hk_spot_all_fetcher: Optional[Callable[[], pd.DataFrame]] = None,
    global_key: str = "hk_spot_all",
    # tickers
    tickers: Optional[Sequence[str]] = None,
    # selection / update
    datasets: Optional[Sequence[str]] = None,
    incremental: bool = True,
    strategy: Optional[WriteStrategy] = None,
    prune: bool = False,
    update_asof_on_update_datasets: bool = False,
    # runtime
    skip_errors: bool = True,
    sleep_s: float = 0.5,
    max_tickers: Optional[int] = None,
    progress: bool = True,
) -> Tuple[Optional[pd.DataFrame], Optional[RefreshResult]]:
    """
    One entry point that covers your modes:

    1) Only update global hk_spot_all:
        mode="global_only"
        典型用法:
        spot_df, result = refresh_market(
            store, fetchers,
            mode="global_only",
            hk_spot_all_fetcher=hk_spot_all,
        )
        # result == None

    2) Only update tickers using stored hk_spot_all:
        mode="tickers_only", tickers=None
        (if global missing and ticker=None and refresh_spot=False, raise ERROR,
        to fix set refresh_spot=True and provide hk_spot_all_fetcher)
        ticker = None 使用global的hk_spot_all中的list
        ticker not None 使用指定的 ticker, 可以用作测试

    3) Update global then update tickers:
        mode="global_then_tickers"

    Parameters
    ----------
    store:
        ParquetStore instance.
    fetchers:
        Full ticker fetchers registry: name -> fetcher(ticker)->DataFrame
    mode:
        "global_only" | "tickers_only" | "global_then_tickers"
    refresh_spot:
        If True, refresh global hk_spot_all (requires hk_spot_all_fetcher) before deriving universe.
        For mode="global_then_tickers", global update is always performed (overwrite=True).
    hk_spot_all_fetcher:
        Callable returning full-market spot df. Required when refreshing global.
        Typically: from .fetchers import hk_spot_all (your wrapper around ak.stock_hk_spot_em)
    tickers:
        If provided, use this list; else derive from store's global hk_spot_all.
    datasets:
        None for all, or subset list for partial updates.
        可以部分更新data的key, 实现一部分时效性数据的更新, 例如 financial_indicator_snapshot
    incremental:
        Resume logic: complete tickers are skipped.
    strategy:
        None -> defaults: full => save_bundle, subset => update_datasets
        不用手动设置, 他会自动选择的
    prune:
        Only used for save_bundle (subset update will force prune=False for safety).
        删去多余的, dict中没有的df
    update_asof_on_update_datasets:
        If strategy=update_datasets, whether to bump asof on each dataset update.
        For snapshot updates, set True.

    常见用法:
    # 只更新 global 例如每天一次
    refresh_market(
        store, fetchers,
        mode="global_only",
        hk_spot_all_fetcher=hk_spot_all,
    )

    # 全市场断点续传（不更新 global）
    refresh_market(
        store, fetchers,
        mode="tickers_only",
        tickers=None,
        incremental=True,
        sleep_s=0.7,
    )

    # 全量重建（先更新 global，再全部重抓）
    refresh_market(
        store, fetchers,
        mode="global_then_tickers",
        hk_spot_all_fetcher=hk_spot_all,
        incremental=False,
        prune=False,     # 先别 prune，稳定后再考虑
        sleep_s=0.7,
    )

    # 只更新快照数据（financial_indicator_snapshot）
    refresh_market(
        store, fetchers,
        mode="tickers_only",
        tickers=None,
        datasets=["financial_indicator_snapshot"],
        incremental=True,
        update_asof_on_update_datasets=True,
        sleep_s=0.7,
    )
    """
    spot_df: Optional[pd.DataFrame] = None
    result: Optional[RefreshResult] = None

    if mode not in ("global_only", "tickers_only", "global_then_tickers"):
        raise ValueError(f"Unknown mode: {mode}")

    # global step
    if mode in ("global_only", "global_then_tickers") or refresh_spot:
        if hk_spot_all_fetcher is None:
            raise ValueError("Global refresh requested but hk_spot_all_fetcher is None.")
        spot_df = update_global_hk_spot_all(
            store,
            hk_spot_all_fetcher=hk_spot_all_fetcher,
            overwrite=True,
            global_key=global_key,
        )

    if mode == "global_only":
        return spot_df, None

    # ----------------------------
    # Fixed whitelist filtering for user-provided tickers:
    # If caller passes tickers explicitly, we first intersect them with the
    # current stored universe (derived from hk_spot_all in store).
    # This prevents random/invalid tickers (e.g. "12341") from triggering crawl errors.
    # The universe extraction uses extract_tickers_from_spot(df) default behavior:
    #   - only_equity=True (so it respects df["is_equity"] if present).
    # ----------------------------
    if tickers is not None:
        # Build whitelist from the latest available hk_spot_all:
        # - if we refreshed global in this call, spot_df is the freshest
        # - else load from store
        df_universe = spot_df if spot_df is not None else store.load_global_df(global_key)

        # NOTE: extract_tickers_from_spot defaults only_equity=True, so it will only
        # include tickers with is_equity=True (if column exists).
        whitelist = set(extract_tickers_from_spot(df_universe))

        # Intersect while keeping user order and removing duplicates
        orig = [normalize_hk_ticker(x) for x in tickers]
        out: List[str] = []
        seen = set()
        for t in orig:
            if t in whitelist and t not in seen:
                seen.add(t)
                out.append(t)

        if progress:
            dropped = [t for t in orig if t not in whitelist]
            print(f"[refresh_market] tickers filtered by store universe: in={len(orig)} out={len(out)} dropped={len(dropped)}")

        tickers = out

    # universe selection
    if tickers is None:
        # derive from store global
        tickers = get_universe_tickers(
            store,
            refresh_spot=False,  # already refreshed above if needed
            hk_spot_all_fetcher=hk_spot_all_fetcher,
            global_key=global_key,
        )

    result = refresh_tickers(
        store,
        tickers,
        fetchers,
        datasets=datasets,
        incremental=incremental,
        strategy=strategy,
        prune=prune,
        update_asof_on_update_datasets=update_asof_on_update_datasets,
        skip_errors=skip_errors,
        sleep_s=sleep_s,
        max_tickers=max_tickers,
        progress=progress,
    )

    return spot_df, result


# ----------------------------
# Convenience wrappers
# ----------------------------
# 连带检查ticker是否是股票 用了30分钟
def refresh_global_only(
    store: ParquetStore,
    *,
    hk_spot_all_fetcher: Callable[[], pd.DataFrame],
    global_key: str = "hk_spot_all",
) -> pd.DataFrame:
    """Convenience: only refresh hk_spot_all global dataset."""
    return update_global_hk_spot_all(store, hk_spot_all_fetcher=hk_spot_all_fetcher, overwrite=True, global_key=global_key)


# 就是封装了一层, 没有这个函数也行, 减少认知的负担
def refresh_financial_indicator_snapshot_only(
    store: ParquetStore,
    fetchers: Fetchers,
    *,
    tickers: Optional[Sequence[str]] = None,
    refresh_spot: bool = False,
    hk_spot_all_fetcher: Optional[Callable[[], pd.DataFrame]] = None,
    global_key: str = "hk_spot_all",
    incremental: bool = True,
    update_asof: bool = True,
    skip_errors: bool = True,
    sleep_s: float = 0.0,
    max_tickers: Optional[int] = None,
    progress: bool = True,
) -> Tuple[Optional[pd.DataFrame], RefreshResult]:
    """
    Convenience: update only 'financial_indicator_snapshot' for many tickers.

    This reuses the SAME pipeline:
      - select fetchers subset
      - crawl_ticker(ticker, subset)
      - write strategy defaults to update_datasets (safe)
    """
    spot_df, res = refresh_market(
        store,
        fetchers,
        mode="tickers_only",
        refresh_spot=refresh_spot,
        hk_spot_all_fetcher=hk_spot_all_fetcher,
        global_key=global_key,
        tickers=tickers,
        datasets=["financial_indicator_snapshot"],
        incremental=incremental,
        strategy="update_datasets",
        prune=False,
        update_asof_on_update_datasets=update_asof,
        skip_errors=skip_errors,
        sleep_s=sleep_s,
        max_tickers=max_tickers,
        progress=progress,
    )
    assert res is not None
    return spot_df, res



# ----------------------------
# Usage examples (copy to ipynb)
# ----------------------------

USAGE = r"""
# 0) Prepare store & fetchers
from lib.stock_data_analysis.storage import ParquetStore
from lib.stock_data_analysis.fetchers import build_ticker_fetchers, hk_spot_all
from lib.stock_data_analysis.refresh import (
    refresh_market,
    refresh_global_only,
    refresh_financial_indicator_snapshot_only,
)

store = ParquetStore(r"E:\data\hk_value_store")
fetchers = build_ticker_fetchers(indicator="年度")

# 1) Only update global hk_spot_all
df_spot = refresh_global_only(store, hk_spot_all_fetcher=hk_spot_all)

# 2) Update tickers using stored hk_spot_all (resume enabled)
spot_df, result = refresh_market(
    store,
    fetchers,
    mode="tickers_only",
    tickers=None,           # derive from stored hk_spot_all
    incremental=True,       # skip complete tickers
    prune=False,            # safe default
    progress=True,
)

# 3) Update hk_spot_all then update tickers (full refresh, overwrite everything)
spot_df, result = refresh_market(
    store,
    fetchers,
    mode="global_then_tickers",
    hk_spot_all_fetcher=hk_spot_all,
    incremental=False,      # force refresh
    prune=True,             # clean stale datasets (ONLY when doing full refresh!)
)

# 4) Partial update: only refresh financial_indicator_snapshot for all tickers (safe)
spot_df, res = refresh_financial_indicator_snapshot_only(
    store,
    fetchers,
    tickers=None,           # derive from stored hk_spot_all
    refresh_spot=False,
    incremental=True,
    update_asof=True,
)

# 5) Partial update: update a custom subset of dataset keys
subset = ["company_profile", "security_profile"]
spot_df, result = refresh_market(
    store,
    fetchers,
    mode="tickers_only",
    tickers=["00001", "00882"],
    datasets=subset,
    incremental=True,
    # subset defaults to strategy=update_datasets (safe), prune forced False
)
"""

