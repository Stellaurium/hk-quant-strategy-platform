# stock_data_analysis/lib/crawler.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

import pandas as pd

from .utils import now_utc, normalize_hk_ticker, safe_empty_df
from .bundle import Bundle, DatasetName

Fetcher = Callable[[str], pd.DataFrame]


# ----------------------------
# Standardization (lightweight, non-destructive)
# ----------------------------
def _standardize_df(
    df: pd.DataFrame,
    *,
    ticker: str,
    asof: datetime,
    dataset: str,
    add_trace: bool = True,
) -> pd.DataFrame:
    """
    统一做一些“轻标准化”，避免污染原始含义：
    - 空值返回空DF
    - 可选加入追溯列：ticker/asof/dataset
    - 不做字段重命名、不做数值单位转换（这些应该放 schema 层）
    """
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        # 保持最小一致性：返回空DF而不是 None
        out = safe_empty_df()
    else:
        out = df.copy()

    if add_trace:
        # 追溯列放在末尾，不影响你观察原始列
        out["ticker"] = ticker
        out["asof"] = asof.isoformat()
        out["dataset"] = dataset
    return out


# ----------------------------
# Crawl result (optional)
# ----------------------------
@dataclass(slots=True)
class CrawlReport:
    ticker: str
    asof: datetime
    ok: Dict[str, bool]
    errors: Dict[str, str]


# ----------------------------
# Core crawler
# ----------------------------
def crawl_ticker(
    ticker: str,
    fetchers: Dict[DatasetName, Fetcher],
    *,
    asof: Optional[datetime] = None,
    indicator: str = "年度",
    require: Optional[Iterable[DatasetName]] = None,
    add_trace: bool = True,
    stop_on_error: bool = False,
) -> Tuple[Bundle, CrawlReport]:
    """
    爬取框架（单 ticker）：
      输入：
        - ticker: 例如 "00001"
        - fetchers: name -> fn(ticker)->DataFrame 的注册表（你 build_ticker_fetchers() 的输出）
      输出：
        - Bundle: (ticker, asof, data, meta, errors)
        - CrawlReport: 每个 dataset 成功与否、错误摘要（方便打印）

    标准化策略（统一模式）：
      - 每个 DF 统一 copy
      - 统一补充追溯字段 ticker/asof/dataset（可关闭 add_trace）
      - 失败项写入 bundle.errors，并放入空 DF（不会阻断整体；除非 stop_on_error=True）
    """
    t = normalize_hk_ticker(ticker)
    ts = asof or now_utc()

    data: Dict[DatasetName, pd.DataFrame] = {}
    errors: Dict[DatasetName, str] = {}
    ok: Dict[DatasetName, bool] = {}

    for name, fn in fetchers.items():
        try:
            df = fn(t)  # 统一调用方式：fn(ticker)->df
            data[name] = _standardize_df(df, ticker=t, asof=ts, dataset=name, add_trace=add_trace)
            ok[name] = True
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            errors[name] = msg
            ok[name] = False
            data[name] = _standardize_df(safe_empty_df(), ticker=t, asof=ts, dataset=name, add_trace=add_trace)
            if stop_on_error:
                break

    bundle = Bundle(
        ticker=t,
        asof=ts,
        data=data,
        meta={
            "indicator": indicator,
            "fetcher_count": len(fetchers),
        },
        errors=errors,
    )

    # 可选校验：确保必须的数据集存在（注意：就算失败，我们也会塞空DF，所以“存在性”一般满足）
    if require is not None:
        bundle.validate(require=require)
    else:
        bundle.validate()

    report = CrawlReport(ticker=t, asof=ts, ok=ok, errors=errors)
    return bundle, report

