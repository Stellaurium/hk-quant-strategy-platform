# lib/stock_data_analysis/instrument_probe.py
# 实现检测一个ticker 是否是一个公司的启发性策略
# 大体上应该是有效的, 也可以未来使用其他的方式
# 例如找到一个官方数据源, 直接下载当前所有的list, 然后检查目标的ticker 是否在这个list中

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Optional, Tuple
import time
import random
import pandas as pd


@dataclass(frozen=True)
class EquityProbeResult:
    ticker: str
    is_equity: bool
    reason: str
    ok_security_profile: bool
    ok_financial_indicator_snapshot: bool
    err_security_profile: Optional[str] = None
    err_financial_indicator_snapshot: Optional[str] = None


def _df_has_business_cols(df: pd.DataFrame) -> bool:
    """
    你 crawler 会加 trace 列 ticker/asof/dataset，所以这里要求：
    - df 非空
    - 且至少有一个“非 trace”的业务列
    """
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return False
    trace = {"ticker", "asof", "dataset"}
    business_cols = [c for c in df.columns if c not in trace]
    return len(business_cols) > 0


def _probe_fetch(
    fn: Callable[[], pd.DataFrame],
    *,
    tries: int,
    sleep_s: float,
) -> Tuple[bool, Optional[str]]:
    last_err = None
    for i in range(tries):
        try:
            df = fn()
            if _df_has_business_cols(df):
                return True, None
            return False, "empty_or_no_business_cols"
        except Exception as e:
            last_err = repr(e)
            if i < tries - 1 and sleep_s > 0:
                jitter = random.uniform(0, sleep_s * 0.3)
                time.sleep(sleep_s + jitter)
    return False, last_err


def is_equity_like_ticker(
    ticker: str,
    fetchers: Dict[str, Callable[[str], pd.DataFrame]],
    *,
    tries: int = 2,
    probe_sleep_s: float = 0.1,
    require_both: bool = True,
) -> EquityProbeResult:
    """
    极简策略：
    - probe security_profile
    - probe financial_indicator_snapshot
    - require_both=False: 任意一个成功 => True（更宽松）
    - require_both=True : 两个都成功 => True（更严格，你现在更像想要这个）

    返回 EquityProbeResult，方便你在 refresh 里打印/落盘审计。
    """
    ok_sp, err_sp = (False, "missing_fetcher")
    ok_fi, err_fi = (False, "missing_fetcher")

    if "security_profile" in fetchers:
        ok_sp, err_sp = _probe_fetch(lambda: fetchers["security_profile"](ticker), tries=tries, sleep_s=probe_sleep_s)

    if "financial_indicator_snapshot" in fetchers:
        ok_fi, err_fi = _probe_fetch(lambda: fetchers["financial_indicator_snapshot"](ticker), tries=tries, sleep_s=probe_sleep_s)

    if require_both:
        is_eq = ok_sp and ok_fi
        reason = "both probes ok" if is_eq else "at least one probe failed"
    else:
        is_eq = ok_sp or ok_fi
        reason = "any probe ok" if is_eq else "both probes failed"

    return EquityProbeResult(
        ticker=ticker,
        is_equity=is_eq,
        reason=reason,
        ok_security_profile=ok_sp,
        ok_financial_indicator_snapshot=ok_fi,
        err_security_profile=None if ok_sp else err_sp,
        err_financial_indicator_snapshot=None if ok_fi else err_fi,
    )
