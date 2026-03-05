# stock_data_analysis/lib/utils.py
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union

import pandas as pd

__all__ = [
    "now_utc",
    "now_utc_iso",
    "normalize_hk_ticker",
    "hk_to_ths_code",
    "ensure_dir",
    "safe_empty_df",
    "add_trace_cols",
    "require_columns",
    "df_brief",
    "DatasetError",
]


# ----------------------------
# Time
# ----------------------------
def now_utc() -> datetime:
    """Return timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def now_utc_iso(timespec: str = "seconds") -> str:
    """
    Return UTC ISO8601 string.
    timespec: 'auto'|'hours'|'minutes'|'seconds'|'milliseconds'|'microseconds'
    """
    return now_utc().isoformat(timespec=timespec)


# ----------------------------
# Ticker helpers (HK)
# ----------------------------
_HK_TICKER_RE = re.compile(r"[^0-9]")


def normalize_hk_ticker(ticker: Union[str, int]) -> str:
    """
    Normalize HK ticker to 5-digit string used by Eastmoney/AkShare in many HK endpoints.

    Examples:
      "00001" -> "00001"
      "1" -> "00001"
      "HK00001" -> "00001"
      "0001" -> "00001"
      700 -> "00700"
    """
    s = str(ticker).strip().upper()
    s = _HK_TICKER_RE.sub("", s)
    if not s:
        raise ValueError(f"Invalid HK ticker: {ticker!r}")

    # int(...) removes leading zeros safely for "00001"
    n = int(s)
    if n < 0:
        raise ValueError(f"Invalid HK ticker (negative): {ticker!r}")
    if n > 99999:
        raise ValueError(f"Invalid HK ticker (too large): {ticker!r}")

    return str(n).zfill(5)


def hk_to_ths_code(hk_ticker_5: Union[str, int]) -> str:
    """
    Convert 5-digit HK ticker to THS 4-digit code commonly used by AkShare THS dividend endpoint.

    Examples:
      "00700" -> "0700"
      "00001" -> "0001"
      "00882" -> "0882"
      "09988" -> "9988"
    """
    t5 = normalize_hk_ticker(hk_ticker_5)
    return str(int(t5)).zfill(4)


# ----------------------------
# Filesystem
# ----------------------------
def ensure_dir(path: Union[str, Path]) -> Path:
    """Create directory if missing and return Path."""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


# ----------------------------
# DataFrame helpers
# ----------------------------
def safe_empty_df(columns: Optional[Sequence[str]] = None) -> pd.DataFrame:
    """Return an empty DataFrame with optional columns."""
    return pd.DataFrame(columns=list(columns) if columns is not None else None)


def add_trace_cols(
    df: pd.DataFrame,
    *,
    ticker: Optional[str] = None,
    asof: Optional[Union[str, datetime]] = None,
    inplace: bool = False,
    ticker_col: str = "ticker",
    asof_col: str = "asof",
) -> pd.DataFrame:
    """
    Add traceability columns to a DataFrame. Useful before persisting.

    - ticker: normalized 5-digit HK ticker is recommended
    - asof: datetime or iso string; if datetime, stored as ISO string

    By default returns a copy (inplace=False).
    """
    if df is None:
        return safe_empty_df()

    out = df if inplace else df.copy()

    if ticker is not None:
        out[ticker_col] = str(ticker)

    if asof is not None:
        if isinstance(asof, datetime):
            out[asof_col] = asof.astimezone(timezone.utc).isoformat()
        else:
            out[asof_col] = str(asof)

    return out


def require_columns(df: pd.DataFrame, cols: Sequence[str], *, where: str = "") -> None:
    """Raise DatasetError if df missing required columns."""
    missing = [c for c in cols if c not in df.columns]
    if missing:
        loc = f" ({where})" if where else ""
        raise DatasetError(f"Missing required columns{loc}: {missing}. Got columns={list(df.columns)}")


def df_brief(df: pd.DataFrame, n: int = 3) -> Dict[str, Any]:
    """
    Lightweight DF summary for logging/debug:
      - shape
      - first columns
      - head rows (as dict records)
    """
    if df is None:
        return {"shape": (0, 0), "columns": [], "head": []}
    head = df.head(n).to_dict(orient="records") if len(df) else []
    return {
        "shape": df.shape,
        "columns": list(df.columns),
        "head": head,
    }


# ----------------------------
# Errors
# ----------------------------
class DatasetError(RuntimeError):
    """Raised when a dataset payload is invalid/missing required structure."""
    pass
