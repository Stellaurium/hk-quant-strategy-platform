# stock_data_analysis/lib/fetchers.py
# 这里面是每一个data中的item 用到一个独特的函数
# 同时将data dict的key的名称 和 一个调用的函数/lambda对应起来, 解耦调用的过程
from __future__ import annotations

import random
import time
from functools import partial
from typing import Callable, Dict, Optional, Any

import pandas as pd
import akshare as ak
from tqdm.auto import tqdm

from .utils import normalize_hk_ticker, hk_to_ths_code, safe_empty_df
from .instrument_probe import is_equity_like_ticker, EquityProbeResult

Fetcher = Callable[[str], pd.DataFrame]


# ---------- single-ticker fetchers ----------
def company_profile(ticker: str) -> pd.DataFrame:
    """
    获取【港股-公司资料/公司画像】（东方财富 EM）：
    - 传入港股 ticker（会被规范化为 5 位，如 "00001"）
    - 返回该公司的基础资料 DataFrame（通常为 1 行或少量行）
    - 常见信息包括：所属行业、公司介绍/主营描述、公司名称等（以实际返回列为准）

    对应 AkShare: stock_hk_company_profile_em(symbol=...)
    """
    t = normalize_hk_ticker(ticker)
    return ak.stock_hk_company_profile_em(symbol=t)


def security_profile(ticker: str) -> pd.DataFrame:
    """
    获取【港股-证券资料/证券画像】（东方财富 EM）：
    - 传入港股 ticker（规范化为 5 位）
    - 返回该证券的基础信息 DataFrame（通常为 1 行或少量行）
    - 常见信息包括：板块/交易信息、上市日期、是否沪港通标的、是否深港通标的等（以实际返回列为准）

    对应 AkShare: stock_hk_security_profile_em(symbol=...)
    """
    t = normalize_hk_ticker(ticker)
    return ak.stock_hk_security_profile_em(symbol=t)


def financial_indicator_snapshot(ticker: str) -> pd.DataFrame:
    """
    获取【港股-财务指标快照/估值股本市值】（东方财富 EM）：
    - 传入港股 ticker（规范化为 5 位）
    - 返回该标的的“当前快照”类指标 DataFrame（通常为 1 行或少量行）
    - 你关心的字段通常包括：总市值(港元)、市盈率/市净率、已发行股本、
      股息率TTM、每股股息TTM 等（以实际返回列为准）

    这是一个实时变动的指标, 可能需要频繁地获取最新值, 而其他的数据可能就不需要这样获取.
    可能需要单独写一个函数, 用于更新database中所有与这个相关的数据.

    对应 AkShare: stock_hk_financial_indicator_em(symbol=...)
    """
    t = normalize_hk_ticker(ticker)
    return ak.stock_hk_financial_indicator_em(symbol=t)


def dividend_history(ticker: str) -> pd.DataFrame:
    """
    获取【港股-分红派息历史明细】（同花顺 THS）：
    - 传入港股 ticker（内部会转换为同花顺常用 4 位代码：如 00700 -> 0700）
    - 返回该标的历史分红/派息事件列表 DataFrame（多行）
    - 常见字段包括：公告日、方案、除净日、派息日、过户日期起止、类型、进度、以股代息等（以实际返回列为准）

    对应 AkShare: stock_hk_fhpx_detail_ths(symbol=...)
    """
    t = normalize_hk_ticker(ticker)
    ths = hk_to_ths_code(t)
    return ak.stock_hk_fhpx_detail_ths(symbol=ths)


def financial_report(ticker: str, report_name: str, *, indicator: str = "年度") -> pd.DataFrame:
    """
    获取【港股-财务报表（三大表之一）】（东方财富 EM）：
    - 传入港股 ticker（规范化为 5 位）
    - report_name：报表类型，通常为：
        - "资产负债表"
        - "利润表"
        - "现金流量表"
    - indicator：期间粒度，常用 "年度"（也可用接口支持的其它值，如 "报告期"）
    - 返回该报表的“科目长表” DataFrame（多行）：
      常见字段包括 STD_ITEM_CODE / STD_ITEM_NAME / AMOUNT / STD_REPORT_DATE 等（以实际返回列为准）

    对应 AkShare: stock_financial_hk_report_em(stock=..., symbol=..., indicator=...)
    """
    t = normalize_hk_ticker(ticker)
    return ak.stock_financial_hk_report_em(stock=t, symbol=report_name, indicator=indicator)


# ---------- global fetchers ----------
# 原始 spot：只抓全市场行情
def _hk_spot_all_raw() -> pd.DataFrame:
    """
    仅抓取 AkShare 全市场行情，不做任何 is_equity 判定。
    """
    df = ak.stock_hk_spot_em()
    if isinstance(df, pd.DataFrame) and not df.empty and "代码" in df.columns:
        df = df.copy()
        df["代码"] = df["代码"].astype(str).str.zfill(5)
    return df


def hk_spot_all_with_is_equity(
        classifier: Callable[[str], EquityProbeResult],
        *,
        ticker_col: str = "代码",
        out_col: str = "is_equity",
        errors: str = "null",  # "raise" | "null" | "false"
) -> pd.DataFrame:
    """
    1) 调用 _hk_spot_all_raw() 获取全市场 spot
    2) 对 df[ticker_col] 的每个 ticker 调用 classifier(ticker)
    3) 增广 out_col 列（默认 is_equity）

    errors:
      - "raise": classifier 任何异常直接抛出
      - "null" : 异常时该 ticker 的 is_equity = pd.NA（推荐，避免静默误判）
      - "false": 异常时该 ticker 的 is_equity = False
    """
    # 参数校验（避免传错导致行为不明确）
    if errors not in ("raise", "null", "false"):
        raise ValueError(f"errors must be one of ('raise','null','false'), got: {errors!r}")

    df = _hk_spot_all_raw()
    if df.empty:
        # 返回空表也保持列一致性
        df[out_col] = pd.Series(dtype="boolean")
        return df

    if ticker_col not in df.columns:
        # ticker 列都没有，就无法分类；给出空的 is_equity 列
        out = df.copy()
        out[out_col] = pd.Series([pd.NA] * len(out), dtype="boolean")
        return out

    # 注意：这里仍然沿用你原本的逻辑（astype(str) + 过滤 nan/空串）
    tickers = df[ticker_col].astype(str)
    uniq = pd.unique(tickers)

    mapping: dict[str, Any] = {}

    # tqdm 进度条：在 notebook 会显示百分比/ETA；在终端也可用
    # desc 可以改成你想展示的中文
    for t in tqdm(uniq, total=len(uniq), desc="Probe HK tickers", unit="ticker"):
        # 过滤掉空 ticker（规范化后可能出现 ""）
        if t is None or t == "" or t.lower() == "nan":
            mapping[t] = pd.NA
            continue

        try:
            mapping[t] = classifier(t).is_equity
        except Exception:
            if errors == "raise":
                raise
            mapping[t] = (False if errors == "false" else pd.NA)

    out = df.copy()
    out[out_col] = tickers.map(mapping).astype("boolean")
    return out


def spot_snapshot_from_global(ticker: str, spot_all: pd.DataFrame) -> pd.DataFrame:
    """Filter one ticker row from an all-market spot table."""
    t = normalize_hk_ticker(ticker)
    if spot_all is None or not isinstance(spot_all, pd.DataFrame) or "代码" not in spot_all.columns:
        return safe_empty_df()
    return spot_all.loc[spot_all["代码"] == t].reset_index(drop=True)


# ---------- registry ----------
# 注册每个ticker的data这个dict的每一个key对应的value 是通过什么函数调用的
def build_ticker_fetchers(*, indicator: str = "年度") -> Dict[str, Fetcher]:
    """
    name -> fetcher(ticker)->DataFrame
    (spot is handled separately as a global dataset; see hk_spot_all / spot_snapshot_from_global)
    """
    return {
        "company_profile": company_profile,
        "security_profile": security_profile,
        "financial_bs_yearly": lambda t: financial_report(t, "资产负债表", indicator="年度"),
        "financial_is_yearly": lambda t: financial_report(t, "利润表", indicator="年度"),
        "financial_cf_yearly": lambda t: financial_report(t, "现金流量表", indicator="年度"),
        "dividend_history": dividend_history,
        "financial_indicator_snapshot": financial_indicator_snapshot,
    }


# 直接绑定, 保留外部稳定的接口
_classifier = partial(is_equity_like_ticker, fetchers=build_ticker_fetchers(),
                      tries=2, probe_sleep_s=0.1, require_both=True)
hk_spot_all = lambda: hk_spot_all_with_is_equity(_classifier, errors="null")
