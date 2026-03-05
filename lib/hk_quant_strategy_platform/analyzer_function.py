# 保存评判股票好坏的算法, 可以有多套
# 例如 针对烟蒂股可以写一套, 针对大盘股,港股通的股可以写一套 等等
# 然后使用一个标准的框架, 将这个评判股票好坏的算法作用在其上面, 产生一个带有详细评判的表格, 实现初步的筛选
# 评判算法的参数是固定的 Bundle -> Dict, 其中dict中的每一个key对应一个df的列的名称, value是其内容
# 函数也可以返回空结果, 表示忽略分析这个ticker (例如 当使用特定的算法, 只分析银行的时候, 可以让非银行的直接返回None)


import pandas as pd
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Union, Tuple

# 导入外部函数的参数
from .analyzer_parameter import DEFAULT_ASSET_WEIGHTS, DEFAULT_LIAB_WEIGHTS

AnalyzerFn = Callable[[Any], Optional[Dict[str, Any]]]


def run_analyzer(
        storage,
        analyzer_fn: AnalyzerFn,
        *,
        load_keys: Optional[Sequence[str]] = None,
        tickers: Optional[Sequence[str]] = None,
        keep_skipped: bool = False,  # True: analyzer 返回 None/{} 也保留一行（只有 ticker/asof + skipped 标记）
        stop_on_error: bool = False,  # True: 任意异常直接抛出
) -> pd.DataFrame:
    """
    单算法 runner：把 analyzer_fn 应用到一组 tickers，汇总成 DataFrame。

    analyzer_fn 签名：
      - 输入：bundle（storage.load_bundle 返回）
      - 输出：dict（列名->值）或 None（跳过该 ticker）
        - 若返回 {} 也按跳过处理（可通过 keep_skipped=True 保留一行）

    输出至少包含：
      - ticker
      - asof
      - ok (bool)
      - error (str|None)
      - skipped (bool) 仅当 keep_skipped=True 时有意义
      - 再加 analyzer 输出的各字段
    """
    if tickers is None:
        tickers = storage.list_tickers()

    # load_keys: None 表示全量 load（通常不建议）；建议你显式传最小 keys
    rows: List[Dict[str, Any]] = []

    for t in tickers:
        try:
            b = storage.load_bundle(t, load=list(load_keys) if load_keys is not None else None)
        except Exception as e:
            if stop_on_error:
                raise
            rows.append({
                "ticker": t,
                "asof": None,
                "ok": False,
                "error": f"load_bundle failed: {type(e).__name__}: {e}",
                "skipped": False,
            })
            continue

        base = {
            "ticker": getattr(b, "ticker", t),
            "asof": getattr(b, "asof", None),
            "ok": True,
            "error": None,
            "skipped": False,
        }

        try:
            out = analyzer_fn(b)
            # None / {} => 跳过
            if out is None or (isinstance(out, dict) and len(out) == 0):
                if keep_skipped:
                    base["skipped"] = True
                    rows.append(base)
                continue

            if not isinstance(out, dict):
                raise TypeError(f"analyzer_fn must return dict|None, got {type(out)}")

            base.update(out)
            rows.append(base)

        except Exception as e:
            if stop_on_error:
                raise
            base["ok"] = False
            base["error"] = f"{type(e).__name__}: {e}"
            rows.append(base)

    df = pd.DataFrame(rows)

    # 如果你不想要 skipped 列，也可以在这里 drop；我保留它便于调试。
    return df


# ------------------------
# 这后面写具体每一个子分析的函数 (计算某个参数, 返回一个或几个数值, 例如计算手中现金, 有息负债, 自由现金流)

# -------------------------------------
# 辅助函数
from typing import Any, Dict, Iterable, Optional, Callable, TypeVar
import pandas as pd

T = TypeVar("T")


# ----------------------------
# Core primitives
# ----------------------------

def _safe_div(num: float, denom: Optional[float]) -> Optional[float]:
    if denom is None or denom == 0:
        return None
    return num / denom

def _isna_bool(v: Any) -> bool:
    """pd.isna for scalars; if it returns array-like, treat as not-na."""
    try:
        na = pd.isna(v)
    except Exception:
        return False
    return bool(na) if isinstance(na, (bool,)) else False


def _is_blank(v: Any) -> bool:
    """Blank = None / NaN / empty string."""
    if v is None:
        return True
    if _isna_bool(v):
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    return False


def _pick_first(items: Iterable[T], pred: Callable[[T], bool]) -> Optional[T]:
    for x in items:
        if pred(x):
            return x
    return None


def _clean_str(v: Any) -> Optional[str]:
    """Return stripped string, or None if blank."""
    if _is_blank(v):
        return None
    s = str(v).strip().strip('"').strip("'")
    return s if s != "" else None


# ----------------------------
# Your original utilities (reduced)
# ----------------------------

def pick_first_df(data: Dict[str, Any], keys: Iterable[str]) -> Optional[pd.DataFrame]:
    """Pick first non-empty DataFrame among data[key] for keys."""
    k = _pick_first(keys, lambda kk: isinstance(data.get(kk), pd.DataFrame) and not data.get(kk).empty)
    return data.get(k) if k is not None else None


def pick_first_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    """Pick first existing column name in df."""
    return _pick_first(candidates, lambda c: c in df.columns)


def first_value(df: Optional[pd.DataFrame], candidates: Iterable[str], *, row: int = 0) -> Any:
    """Pick first non-blank value in row across candidate columns."""
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return None
    rowx = df.iloc[row]
    col = _pick_first(candidates, lambda c: c in df.columns and not _is_blank(rowx[c]))
    return None if col is None else rowx[col]


def parse_float(v: Any) -> Optional[float]:
    if _is_blank(v):
        return None
    if isinstance(v, (int, float)) and not _isna_bool(v):
        return float(v)

    s = _clean_str(v)
    if s is None:
        return None
    s2 = s.strip().lower()
    if s2 in {"nan", "none", "null"}:
        return None

    s = s.replace(",", "").replace("HK$", "").replace("HKD", "").replace("港元", "").strip()
    try:
        return float(s)
    except Exception:
        return None


def parse_bool(v: Any) -> Optional[bool]:
    if _is_blank(v):
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)) and not _isna_bool(v):
        if v == 1:
            return True
        if v == 0:
            return False

    s = _clean_str(v)
    if s is None:
        return None
    s = s.lower()
    if s in {"是", "y", "yes", "true", "1", "t"}:
        return True
    if s in {"否", "n", "no", "false", "0", "f"}:
        return False
    return None


def extract_bundle_data(bundle: Any) -> Dict[str, pd.DataFrame]:
    """Support Bundle-like objects (.data) and plain dicts."""
    if isinstance(bundle, dict):
        if "data" in bundle and isinstance(bundle["data"], dict):
            return bundle["data"]
        return bundle
    if hasattr(bundle, "data"):
        return getattr(bundle, "data")
    raise TypeError("bundle must be a Bundle-like object with .data, or a dict.")


def normalize_code(x: Any, *, zfill_to: int = 9) -> Optional[str]:
    s = _clean_str(x)
    if s is None:
        return None
    if s.endswith(".0"):
        s = s[:-2]
    s = s.replace(" ", "")
    if s.isdigit() and len(s) < zfill_to:
        s = s.zfill(zfill_to)
    return s


# ----------------------------
# select nth (latest) report slice
# ----------------------------
def get_report_df(
        df: pd.DataFrame,
        *,
        offset: int = 0,
        report_date_col: Optional[str] = None,
) -> pd.DataFrame:
    """
    Get the latest-nth report slice of a statement DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Statement DF containing multiple reporting periods.
    offset : int
        0 = latest, 1 = previous, ...
    report_date_col : Optional[str]
        Force which column to use as report date. If None, auto-detect.

    Returns
    -------
    pd.DataFrame
        Rows for the selected reporting date.
    """
    if df is None or df.empty:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()

    if offset < 0:
        raise ValueError("offset must be >= 0")

    date_col = report_date_col or pick_first_col(
        df,
        candidates=[
            "REPORT_DATE",
            "STD_REPORT_DATE",
            "report_date",
            "period_end",
            "END_DATE",
        ],
    )
    if date_col is None:
        raise KeyError(
            "Cannot find report date column. Tried REPORT_DATE/STD_REPORT_DATE/... "
            "Pass report_date_col explicitly."
        )

    d = df.copy()
    d[date_col] = pd.to_datetime(d[date_col], errors="coerce")
    valid_dates = d[date_col].dropna().drop_duplicates().sort_values(ascending=False).tolist()
    if not valid_dates:
        raise ValueError(f"No valid dates found in column: {date_col}")

    if offset >= len(valid_dates):
        raise IndexError(f"offset={offset} out of range: only {len(valid_dates)} distinct report dates")

    target = valid_dates[offset]
    out = d.loc[d[date_col] == target].copy()
    return out


# ----------------------------
# helper: infer regimes by blacklist
# ----------------------------
def _infer_regimes_blacklist(
        weights: Dict[str, Dict[str, Any]],
        *,
        blacklist: Iterable[str] = ("codes",),
        fallback: Tuple[str, ...] = ("CashOnlyAsset", "ConservativeAsset", "NormalAsset"),
) -> Tuple[str, ...]:
    """
    Infer regime names from weights dict keys, excluding blacklist.
    Keep only numeric-like keys (value can be cast to float or is None).

    Ordering rule:
      - Prefer keys in fallback order if present
      - Then append other inferred regimes in sorted order

    If no regimes inferred, return fallback.
    """
    bl = set(blacklist)
    inferred: set[str] = set()

    for _, spec in (weights or {}).items():
        if not isinstance(spec, dict):
            continue
        for k, v in spec.items():
            if k in bl:
                continue
            if v is None:
                inferred.add(k)
                continue
            if isinstance(v, (int, float)):
                inferred.add(k)
                continue
            # allow numeric strings
            try:
                float(v)
                inferred.add(k)
            except Exception:
                pass

    if not inferred:
        return fallback

    ordered = [r for r in fallback if r in inferred] + sorted([r for r in inferred if r not in fallback])
    return tuple(ordered)


# ----------------------------
# weighted dot product on a given year DF (blacklist regimes)
# ----------------------------
def weighted_dot_on_df(
        df_year: pd.DataFrame,
        weights: Dict[str, Dict[str, Any]],
        *,
        regimes: Optional[Tuple[str, ...]] = None,
        meta_blacklist: Iterable[str] = ("codes",),
        code_col: Optional[str] = None,
        amount_col: Optional[str] = None,
        return_detail: bool = False,
) -> Union[Dict[str, float], Tuple[Dict[str, float], Dict[str, Any]]]:
    """
    Dot product: sum(amount(code) * weight(code, regime)) across matched codes.

    weights format:
      {
        "中文科目": {
            "codes": ["004002003", "003002003", ...],
            "CashOnlyAsset": 0.0,
            "ConservativeAsset": 0.25,
            "NormalAsset": 0.55,
            ... (other regimes allowed)
        },
        ...
      }

    regimes:
      - If provided, use exactly these columns as regimes (missing => treated as 0).
      - If None, infer regimes from weights using blacklist meta_blacklist.

    Returns
    -------
    totals : Dict[str, float]
      {regime: total}
    detail : Dict[str, Any] (optional)
      breakdown / unmatched / duplicates info
    """
    if regimes is None:
        regimes = _infer_regimes_blacklist(weights, blacklist=meta_blacklist)

    # empty input handling
    if df_year is None or df_year.empty:
        totals = {r: 0.0 for r in regimes}
        if not return_detail:
            return totals
        return totals, {
            "by_item": {},
            "unmatched_codes": [],
            "matched_codes": [],
            "duplicate_codes_in_weights": {},
            "matched_rows": 0,
            "regimes": list(regimes),
            "meta_blacklist": list(meta_blacklist),
        }

    _code_col = code_col or pick_first_col(df_year, ["STD_ITEM_CODE", "code", "STD_ITEM_ID", "ITEM_CODE"])
    if _code_col is None:
        raise KeyError("Cannot find code column. Tried STD_ITEM_CODE/code/STD_ITEM_ID/ITEM_CODE.")

    _amount_col = amount_col or pick_first_col(df_year, ["AMOUNT", "amount", "VALUE", "value"])
    if _amount_col is None:
        raise KeyError("Cannot find amount column. Tried AMOUNT/amount/VALUE/value.")

    # normalize df
    d = df_year.copy()
    d["_code"] = d[_code_col].map(normalize_code)
    d["_amount"] = pd.to_numeric(d[_amount_col], errors="coerce").fillna(0.0)

    # aggregate amount by code (handles duplicates in DF)
    amt_by_code: Dict[str, float] = d.groupby("_code", dropna=True)["_amount"].sum().to_dict()

    # build code -> item and code -> weight vector
    code2item: Dict[str, str] = {}
    code2w: Dict[str, Dict[str, float]] = {}
    dup_codes_in_weights: Dict[str, List[str]] = {}

    for item_name, spec in (weights or {}).items():
        if not isinstance(spec, dict):
            continue
        codes = spec.get("codes", []) or []
        for c in codes:
            cc = normalize_code(c)
            if not cc:
                continue

            if cc in code2item and code2item[cc] != item_name:
                # same code mapped to multiple items -> record duplicates
                dup_codes_in_weights.setdefault(cc, [code2item[cc]]).append(item_name)
                # keep the first mapping to avoid instability
                continue

            code2item[cc] = item_name
            # missing regime treated as 0, bad cast treated as 0
            wvec: Dict[str, float] = {}
            for r in regimes:
                v = spec.get(r, 0.0)
                try:
                    wvec[r] = float(v) if v is not None else 0.0
                except Exception:
                    wvec[r] = 0.0
            code2w[cc] = wvec

    totals: Dict[str, float] = {r: 0.0 for r in regimes}
    by_item: Dict[str, Dict[str, float]] = {}  # item -> regime -> contribution

    matched_rows = 0
    matched_codes: List[str] = []
    unmatched_codes: List[str] = []

    for code, amt in amt_by_code.items():
        if not code:
            continue
        w = code2w.get(code)
        if w is None:
            unmatched_codes.append(code)
            continue

        matched_rows += 1
        matched_codes.append(code)

        item = code2item.get(code, "<unknown>")
        if item not in by_item:
            by_item[item] = {r: 0.0 for r in regimes}

        for r in regimes:
            contrib = amt * w[r]
            totals[r] += contrib
            by_item[item][r] += contrib

    if not return_detail:
        return totals

    detail = {
        "by_item": by_item,  # 每个中文科目对各口径的贡献
        "unmatched_codes": unmatched_codes,  # DF里出现但权重没覆盖
        "matched_codes": matched_codes,  # DF里被覆盖到的codes
        "duplicate_codes_in_weights": dup_codes_in_weights,  # weights里重复声明的code
        "matched_rows": matched_rows,  # 被匹配的 code 数量（聚合后）
        "code_col": _code_col,
        "amount_col": _amount_col,
        "regimes": list(regimes),
        "meta_blacklist": list(meta_blacklist),
    }
    return totals, detail


# ----------------------------
# one-shot: bundle + statement_key + offset + weights (blacklist regimes)
# 封装了一下的接口
# ----------------------------
def compute_weighted_from_bundle(
        bundle: Any,
        statement_key: str,
        *,
        offset: int = 0,
        weights: Dict[str, Dict[str, Any]],
        regimes: Optional[Tuple[str, ...]] = None,
        meta_blacklist: Iterable[str] = ("codes",),
        report_date_col: Optional[str] = None,
        code_col: Optional[str] = None,
        amount_col: Optional[str] = None,
        return_detail: bool = False,
) -> Union[Dict[str, float], Tuple[Dict[str, float], Dict[str, Any]]]:
    """
    Fetch statement DF from bundle, slice by nth latest year (offset), then compute weighted dot.

    statement_key examples:
      - "financial_bs_yearly"
      - "financial_is_yearly"
      - "financial_cf_yearly"
    """
    data = extract_bundle_data(bundle)
    if statement_key not in data:
        raise KeyError(
            f"statement_key={statement_key!r} not found in bundle.data keys: {list(data.keys())[:20]}..."
        )

    df = data[statement_key]
    df_year = get_report_df(df, offset=offset, report_date_col=report_date_col)

    return weighted_dot_on_df(
        df_year,
        weights,
        regimes=regimes,  # None => infer by blacklist
        meta_blacklist=meta_blacklist,
        code_col=code_col,
        amount_col=amount_col,
        return_detail=return_detail,
    )


def extract_basic_info(
        bundle: Any,
        *,
        bs_key: str = "financial_bs_yearly",
) -> Dict[str, Any]:
    """
    只负责抽“基础信息 + 市值(港元) + 港股通”，不做任何加权计算。
    """
    data = extract_bundle_data(bundle)

    cp = pick_first_df(data, ["company_profile"])
    sp = pick_first_df(data, ["security_profile"])
    fi = pick_first_df(data, ["financial_indicator_snapshot"])
    spot = pick_first_df(data, ["spot_snapshot", "security_spot", "hk_spot_snapshot"])

    # 名称兜底链
    name = (
            first_value(cp, ["公司名称", "名称", "公司简称", "中文名称"])
            or first_value(sp, ["证券简称", "简称", "SECURITY_NAME_ABBR", "SECURITY_NAME", "name"])
            or first_value(fi, ["名称", "证券简称", "SECURITY_NAME_ABBR"])
            or first_value(spot, ["名称", "证券简称", "SECURITY_NAME_ABBR"])
    )
    if not name:
        df_bs = data.get(bs_key)
        if isinstance(df_bs, pd.DataFrame) and not df_bs.empty:
            name = first_value(df_bs, ["SECURITY_NAME_ABBR", "SECURITY_NAME"])

    reg_place = first_value(cp, ["注册地", "注册国家/地区", "注册国家", "REGISTERED_PLACE"])
    founded = first_value(cp, ["公司成立日期", "成立日期", "成立时间", "FOUND_DATE"])
    industry = first_value(cp, ["所属行业", "行业", "INDUSTRY"])
    employees = first_value(cp, ["员工人数", "雇员人数", "员工总数", "EMPLOYEES"])
    intro = first_value(cp, ["公司介绍", "公司简介", "介绍", "简介", "COMPANY_PROFILE", "主营业务"])

    # 港股通：优先 security_profile
    sh = parse_bool(first_value(sp, ["是否沪港通标的", "沪港通标的", "是否沪港通", "is_hk_connect_sh"]))
    sz = parse_bool(first_value(sp, ["是否深港通标的", "深港通标的", "是否深港通", "is_hk_connect_sz"]))
    hk_connect: Optional[bool] = None
    if sh is not None or sz is not None:
        hk_connect = bool((sh is True) or (sz is True))
    else:
        hk_connect = parse_bool(first_value(sp, ["港股通", "HK_CONNECT"])) or parse_bool(first_value(spot, ["港股通"]))

    # 市值(港元)：优先 indicator，其次 spot
    mcap_raw = (
            first_value(fi, ["总市值(港元)", "市值(港元)", "总市值", "市值", "MARKET_CAP", "market_cap"])
            or first_value(spot, ["总市值(港元)", "市值(港元)", "总市值", "市值", "MARKET_CAP", "market_cap"])
    )
    mcap_hkd = parse_float(mcap_raw)

    return {
        "名称": name,
        "注册地": reg_place,
        "成立日期": founded,
        "行业": industry,
        "员工人数": employees,
        "公司介绍": intro,
        "港股通": hk_connect,  # 保留 None/True/False
        "市值(港元)": mcap_hkd,
    }


def compute_assets_from_bundle(
        bundle: Any,
        *,
        bs_key: str,
        bs_offset: int,
        asset_weights: Dict[str, Dict[str, Any]],
        meta_blacklist: Iterable[str] = ("codes",),
) -> Dict[str, float]:
    out = compute_weighted_from_bundle(
        bundle,
        bs_key,
        offset=bs_offset,
        weights=asset_weights,
        meta_blacklist=meta_blacklist,
        return_detail=False,
    )
    return {
        "CashOnlyAsset": float(out.get("CashOnlyAsset", 0.0) or 0.0),
        "ConservativeAsset": float(out.get("ConservativeAsset", 0.0) or 0.0),
        "NormalAsset": float(out.get("NormalAsset", 0.0) or 0.0),
    }


def compute_liabs_from_bundle(
        bundle: Any,
        *,
        bs_key: str,
        bs_offset: int,
        liab_weights: Dict[str, Dict[str, Any]],
        meta_blacklist: Iterable[str] = ("codes",),
) -> Dict[str, float]:
    out = compute_weighted_from_bundle(
        bundle,
        bs_key,
        offset=bs_offset,
        weights=liab_weights,
        meta_blacklist=meta_blacklist,
        return_detail=False,
    )
    return {
        "Liab_Priority": float(out.get("PriorityLiab", 0.0) or 0.0),
        "Liab_Total": float(out.get("TotalLiab", 0.0) or 0.0),
    }


def compute_mos_columns(
    *,
    asset_cash: float,
    asset_cons: float,
    asset_norm: float,
    liab_total: float,
    liab_priority: float,
    mcap_hkd: Optional[float],
) -> Dict[str, Optional[float]]:
    def _net_to_mcap(a: float, l: float) -> Optional[float]:
        if mcap_hkd is None or mcap_hkd == 0:
            return None
        return (a - l) / mcap_hkd

    return {
        "MOS_CashOnlyAsset_minus_TotalLiab": _net_to_mcap(asset_cash, liab_total),
        "MOS_CashOnlyAsset_minus_PriorityLiab": _net_to_mcap(asset_cash, liab_priority),

        "MOS_ConservativeAsset_minus_TotalLiab": _net_to_mcap(asset_cons, liab_total),
        "MOS_ConservativeAsset_minus_PriorityLiab": _net_to_mcap(asset_cons, liab_priority),

        "MOS_NormalAsset_minus_TotalLiab": _net_to_mcap(asset_norm, liab_total),
        "MOS_NormalAsset_minus_PriorityLiab": _net_to_mcap(asset_norm, liab_priority),
    }


# ------------------------
# 这后面写具体每一个分析的函数
# 利用上面的模块化的函数, 调用, 组装成dict


def analyzer_basic_info_balance_sheet(
        bundle: Any,
        *,
        bs_key: str = "financial_bs_yearly",
        bs_offset: int = 0,
        asset_weights: Dict[str, Dict[str, Any]] = None,
        liab_weights: Dict[str, Dict[str, Any]] = None,
        meta_blacklist: Iterable[str] = ("codes",),
) -> Optional[Dict[str, Any]]:
    if asset_weights is None:
        raise ValueError("asset_weights must be provided (e.g., DEFAULT_ASSET_WEIGHTS)")
    if liab_weights is None:
        raise ValueError("liab_weights must be provided (e.g., DEFAULT_LIAB_WEIGHTS)")

    base = extract_basic_info(bundle, bs_key=bs_key)
    assets = compute_assets_from_bundle(
        bundle,
        bs_key=bs_key,
        bs_offset=bs_offset,
        asset_weights=asset_weights,
        meta_blacklist=meta_blacklist,
    )
    liabs = compute_liabs_from_bundle(
        bundle,
        bs_key=bs_key,
        bs_offset=bs_offset,
        liab_weights=liab_weights,
        meta_blacklist=meta_blacklist,
    )

    mos = compute_mos_columns(
        asset_cash=assets["CashOnlyAsset"],
        asset_cons=assets["ConservativeAsset"],
        asset_norm=assets["NormalAsset"],
        liab_total=liabs["Liab_Total"],
        liab_priority=liabs["Liab_Priority"],
        mcap_hkd=base.get("市值(港元)"),
    )

    out: Dict[str, Any] = {}
    out.update(base)
    out.update(assets)
    out.update(liabs)
    out.update(mos)
    out["bs_offset"] = bs_offset

    # ---- Coverage / “负债率”类指标（你当前定义是 Asset / Liab）----
    out["LR_CashOnlyAsset_over_PriorityLiab"] = _safe_div(
        float(assets.get("CashOnlyAsset", 0.0) or 0.0),
        float(liabs.get("Liab_Priority", 0.0) or 0.0),
    )
    out["LR_NormalAsset_over_TotalLiab"] = _safe_div(
        float(assets.get("NormalAsset", 0.0) or 0.0),
        float(liabs.get("Liab_Total", 0.0) or 0.0),
    )


    return out
