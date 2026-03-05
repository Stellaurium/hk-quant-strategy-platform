# hk_value/models/bundle.py
# 为一个中间的接口, 为内存中保存的ticker的数据的结构的约定

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, Mapping, Optional, Protocol

import pandas as pd


DatasetName = str  # 允许自由扩展；不要用 Literal 把自己锁死


class BundleLike(Protocol):
    """
    作为“接口”使用：crawler/writer/reader 只需要依赖这个最小契约。
    """
    ticker: str
    asof: datetime
    data: Dict[DatasetName, pd.DataFrame]

    def get(self, name: DatasetName, default: Optional[pd.DataFrame] = None) -> Optional[pd.DataFrame]: ...
    def keys(self) -> Iterable[DatasetName]: ...
    def validate(self, *, require: Optional[Iterable[DatasetName]] = None) -> None: ...
    def to_dict(self) -> Dict[str, Any]: ...


@dataclass(slots=True)
class Bundle(BundleLike):
    """
    中间内存数据结构（纯数据，无 IO）：
      - ticker: 5位字符串（例如 "00001"）
      - asof: 抓取时间（UTC aware 推荐，但不在此强制）
      - data: name -> DataFrame
    可选：
      - meta: 运行参数（indicator=年度 等）、版本信息等
      - errors: 单个 dataset 的错误信息（不会阻断整体）
    """
    ticker: str
    asof: datetime
    data: Dict[DatasetName, pd.DataFrame] = field(default_factory=dict)

    # Optional扩展字段：不影响你当前“dict三字段”约定
    meta: Dict[str, Any] = field(default_factory=dict)
    errors: Dict[DatasetName, str] = field(default_factory=dict)

    # ---------- dict-like helpers ----------
    def get(self, name: DatasetName, default: Optional[pd.DataFrame] = None) -> Optional[pd.DataFrame]:
        return self.data.get(name, default)

    def keys(self) -> Iterable[DatasetName]:
        return self.data.keys()

    def __contains__(self, name: DatasetName) -> bool:
        return name in self.data

    def __getitem__(self, name: DatasetName) -> pd.DataFrame:
        return self.data[name]

    # ---------- validation ----------
    def validate(self, *, require: Optional[Iterable[DatasetName]] = None) -> None:
        """
        轻量校验：
        - ticker/asof 基本类型检查
        - data 中 value 必须是 DataFrame
        - 可选 require: 必须存在的 dataset keys
        """
        if not isinstance(self.ticker, str) or not self.ticker:
            raise ValueError("Bundle.ticker must be a non-empty str.")
        if not isinstance(self.asof, datetime):
            raise TypeError("Bundle.asof must be a datetime.")
        if not isinstance(self.data, dict):
            raise TypeError("Bundle.data must be a dict[name, DataFrame].")

        bad = [k for k, v in self.data.items() if not isinstance(v, pd.DataFrame)]
        if bad:
            raise TypeError(f"Bundle.data values must be pandas.DataFrame. Bad keys: {bad}")

        if require is not None:
            missing = [k for k in require if k not in self.data]
            if missing:
                raise KeyError(f"Missing required datasets: {missing}")

    # ---------- serialization (for storage layer / IPC) ----------
    def to_dict(self) -> Dict[str, Any]:
        """
        注意：DataFrame 不能直接 JSON 序列化。
        这个 to_dict() 主要用于“内部传递/调试/存储层契约”：
          - ticker/asof/meta/errors 可直接序列化
          - data 仍保留 DataFrame 对象（由 writer 负责落盘）
        """
        return {
            "ticker": self.ticker,
            "asof": self.asof,
            "data": self.data,
            "meta": self.meta,
            "errors": self.errors,
        }

    @staticmethod
    def from_dict(obj: Mapping[str, Any]) -> Bundle:
        """
        允许你用 dict 创建 Bundle（兼容你原始的三字段约定）。
        """
        ticker = obj["ticker"]
        asof = obj["asof"]
        data = obj.get("data", {}) or {}
        meta = obj.get("meta", {}) or {}
        errors = obj.get("errors", {}) or {}

        if isinstance(asof, str):
            # 允许外部把 asof 当字符串传进来（例如 ISO 格式）
            asof = datetime.fromisoformat(asof)

        b = Bundle(
            ticker=str(ticker),
            asof=asof,
            data=dict(data),
            meta=dict(meta),
            errors=dict(errors),
        )
        return b


# ---------- Optional: keys catalog (not mandatory) ----------
@dataclass(frozen=True, slots=True)
class DatasetSpec:
    """
    纯数据的 dataset 规格定义（未来 datasets.py 可以直接复用此结构）。
    放在 models 层不会引入 IO，也能让类型更清晰。
    """
    name: DatasetName
    description: str = ""
    source: str = ""
    is_snapshot: bool = False
