# stock_data_analysis/lib/test_utils.py
"""
存放测试/诊断函数，检验程序是否正常运行，并提供直观理解。

约定：
- 这里的函数尽量“无副作用”（不写盘、不改全局状态），除非函数名明确表示会写盘。
- 输出尽量直观：表格/摘要 + 少量 print。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd


# ============================================================
# Helpers (DataFrame diagnostics)
# ============================================================

def preview_df(df: pd.DataFrame, n: int = 5) -> pd.DataFrame:
    """返回 df 的头部 n 行，用于在 notebook 里直接 display."""
    if df is None or not isinstance(df, pd.DataFrame):
        return pd.DataFrame()
    return df.head(n)


def df_schema(df: pd.DataFrame) -> Dict[str, str]:
    """返回 {col: dtype_str} 的 schema，用于比较."""
    if df is None or not isinstance(df, pd.DataFrame):
        return {}
    return df.dtypes.astype(str).to_dict()


def df_fingerprint(
    df: pd.DataFrame,
    *,
    sample_rows: int = 200,
    sort_cols: bool = True,
    ignore_cols: Optional[Sequence[str]] = None,
) -> str:
    """
    给 DataFrame 一个“近似指纹”，用于快速判断内容是否一致。
    - 会采样前 sample_rows 行（默认 200），避免大表 hash 太慢
    - 默认按列名排序 + 排序列（尽量稳定）
    - ignore_cols: 忽略某些列（例如 asof/ticker 追溯列）
    """
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return "EMPTY"

    x = df.copy()

    if ignore_cols:
        cols = [c for c in x.columns if c not in set(ignore_cols)]
        x = x[cols]

    if sort_cols:
        x = x.reindex(sorted(x.columns), axis=1)

    # 只取一部分行，避免过慢
    if sample_rows is not None and sample_rows > 0 and len(x) > sample_rows:
        x = x.iloc[:sample_rows].copy()

    # 尽量排序行（并不保证总能排序成功）
    try:
        x = x.sort_values(by=list(x.columns)).reset_index(drop=True)
    except Exception:
        x = x.reset_index(drop=True)

    # 用 pandas 内置 hash（快速，适合诊断；不是密码学 hash）
    h = pd.util.hash_pandas_object(x, index=False).sum()
    return str(int(h))


def summarize_bundle(bundle: Any, *, max_keys: int = 50) -> pd.DataFrame:
    """
    把 Bundle 摘要成一个表格：每个 dataset 一行：
    - key / rows / cols / columns(截断) / fingerprint
    """
    rows = []
    data = getattr(bundle, "data", {}) or {}
    keys = list(data.keys())[:max_keys]

    for k in keys:
        df = data.get(k)
        if isinstance(df, pd.DataFrame):
            cols = list(df.columns)
            col_preview = cols[:10]
            rows.append(
                {
                    "dataset": k,
                    "rows": df.shape[0],
                    "cols": df.shape[1],
                    "columns_head": col_preview,
                    "fingerprint": df_fingerprint(df, sample_rows=200, ignore_cols=None),
                }
            )
        else:
            rows.append({"dataset": k, "rows": None, "cols": None, "columns_head": None, "fingerprint": "N/A"})
    return pd.DataFrame(rows).sort_values("dataset").reset_index(drop=True)


# ============================================================
# Bundle compare
# ============================================================

def _normalize_nulls_for_compare(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()

    # 1) 先把 None 统一成 pandas 的缺失标记（对 object/string/int 都更稳）
    x = x.where(pd.notna(x), pd.NA)

    # 2) 对 datetime 列，再把 NA 转成 NaT（保持 dtype 语义）
    dt_cols = x.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns
    if len(dt_cols) > 0:
        for c in dt_cols:
            x[c] = pd.to_datetime(x[c], errors="coerce")

    return x


@dataclass(frozen=True)
class Diff:
    """结构化 diff，方便后续你做更复杂的报告/可视化."""
    path: str
    left: Any
    right: Any


def compare_bundles(
    b1: Any,
    b2: Any,
    *,
    check_values: bool = True,
    verbose: bool = True,
    ignore_cols: Optional[Sequence[str]] = None,
    max_value_rows: int = 2000,
    sample_value_rows: Optional[int] = None,
    check_dtype: bool = False,
) -> List[Diff]:
    """
    对比两个 bundle（逐 dataset 检测一致性）。

    参数：
    - check_values: 是否做严格 values 对比（可能慢）
    - ignore_cols: values 对比时忽略列名（例如 ["asof"] 或追溯列）
    - max_value_rows: 超过该行数时默认不做全量排序对比（避免卡死）
    - sample_value_rows: 如果设置，则只抽样对比前 N 行（更快；用于大表）
    - check_dtype: 是否严格比较 dtype（Parquet 往返 dtype 可能略变，默认 False）

    返回：
    - diffs: List[Diff]
    """
    diffs: List[Diff] = []

    # ---- basic fields ----
    if getattr(b1, "ticker", None) != getattr(b2, "ticker", None):
        diffs.append(Diff("ticker", getattr(b1, "ticker", None), getattr(b2, "ticker", None)))
    if getattr(b1, "asof", None) != getattr(b2, "asof", None):
        diffs.append(Diff("asof", getattr(b1, "asof", None), getattr(b2, "asof", None)))
    if (getattr(b1, "meta", {}) or {}) != (getattr(b2, "meta", {}) or {}):
        diffs.append(Diff("meta", getattr(b1, "meta", {}), getattr(b2, "meta", {})))
    if (getattr(b1, "errors", {}) or {}) != (getattr(b2, "errors", {}) or {}):
        diffs.append(Diff("errors", getattr(b1, "errors", {}), getattr(b2, "errors", {})))

    d1: Dict[str, pd.DataFrame] = getattr(b1, "data", {}) or {}
    d2: Dict[str, pd.DataFrame] = getattr(b2, "data", {}) or {}

    # ---- datasets keys ----
    k1 = set(d1.keys())
    k2 = set(d2.keys())
    only1 = sorted(k1 - k2)
    only2 = sorted(k2 - k1)
    if only1:
        diffs.append(Diff("datasets_only_in_left", only1, None))
    if only2:
        diffs.append(Diff("datasets_only_in_right", only2, None))

    common = sorted(k1 & k2)

    # ---- per dataset compare ----
    for k in common:
        left = d1[k]
        right = d2[k]

        if not isinstance(left, pd.DataFrame) or not isinstance(right, pd.DataFrame):
            diffs.append(Diff(f"{k}.type", type(left), type(right)))
            continue

        # shape
        if left.shape != right.shape:
            diffs.append(Diff(f"{k}.shape", left.shape, right.shape))
            continue

        # columns
        c1 = list(left.columns)
        c2 = list(right.columns)
        if c1 != c2:
            diffs.append(Diff(f"{k}.columns", c1, c2))

        # dtypes
        if check_dtype:
            s1 = df_schema(left)
            s2 = df_schema(right)
            if s1 != s2:
                diffs.append(Diff(f"{k}.dtypes", s1, s2))

        # values（最严格）
        if check_values:
            ldf = left.copy()
            rdf = right.copy()
            ldf = _normalize_nulls_for_compare(ldf)
            rdf = _normalize_nulls_for_compare(rdf)

            if ignore_cols:
                drop = set(ignore_cols)
                ldf = ldf[[c for c in ldf.columns if c not in drop]]
                rdf = rdf[[c for c in rdf.columns if c not in drop]]

            common_cols = [c for c in ldf.columns if c in rdf.columns]
            ldf = ldf[common_cols]
            rdf = rdf[common_cols]

            # 对大表的策略：默认不做全量排序对比
            if sample_value_rows is not None:
                ldf = ldf.iloc[:sample_value_rows].reset_index(drop=True)
                rdf = rdf.iloc[:sample_value_rows].reset_index(drop=True)
            elif len(ldf) > max_value_rows:
                # 只做指纹对比（更快）
                f1 = df_fingerprint(ldf, sample_rows=200, ignore_cols=None)
                f2 = df_fingerprint(rdf, sample_rows=200, ignore_cols=None)
                if f1 != f2:
                    diffs.append(Diff(f"{k}.values_fingerprint", f1, f2))
                continue
            else:
                # 小表：尽量排序后严格对比
                try:
                    ldf = ldf.sort_values(by=common_cols).reset_index(drop=True)
                    rdf = rdf.sort_values(by=common_cols).reset_index(drop=True)
                except Exception:
                    ldf = ldf.reset_index(drop=True)
                    rdf = rdf.reset_index(drop=True)

            try:
                pd.testing.assert_frame_equal(
                    ldf,
                    rdf,
                    check_dtype=check_dtype,
                    check_like=True,
                )
            except AssertionError as e:
                diffs.append(Diff(f"{k}.values", "DIFF", str(e)[:800]))

    if verbose:
        if not diffs:
            print("✅ Bundles are consistent (within chosen checks).")
        else:
            print("❌ Found diffs:")
            for d in diffs:
                print("-", d.path)
                if d.path.endswith(".values") or d.path.endswith(".values_fingerprint"):
                    print("  left :", d.left)
                    print("  right:", d.right)
                else:
                    print("  left :", d.left)
                    print("  right:", d.right)

    return diffs


# ============================================================
# Convenience: quick roundtrip checks (optional; no IO by default)
# ============================================================

def bundle_key_report(b: Any) -> List[str]:
    """返回 bundle.data 的 keys，方便快速打印."""
    data = getattr(b, "data", {}) or {}
    return sorted(list(data.keys()))
