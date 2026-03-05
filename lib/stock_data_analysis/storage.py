# stock_data_analysis/lib/storage.py
from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd

from .utils import ensure_dir, normalize_hk_ticker, now_utc
from .bundle import Bundle, DatasetName


class StorageError(RuntimeError):
    pass


# 把 datetime 变成 ISO 字符串写入 manifest。
def _dt_to_iso(dt: datetime) -> str:
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            # store naive as UTC by convention
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    raise TypeError("asof must be a datetime")


# 把 manifest 里的 ISO 字符串还原成 datetime。
def _iso_to_dt(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _safe_json(obj: Any) -> Any:
    """
    Make sure manifest is JSON-serializable.
    DataFrames are not allowed here; they must be stored as parquet.
    """
    if isinstance(obj, datetime):
        return _dt_to_iso(obj)
    return obj


class ParquetStore:
    """
    A file-based store for Bundle(ticker, asof, data[name]->DataFrame, meta, errors).

    Layout:
      root/
        tickers/
          00001/
            manifest.json
            datasets/
              company_profile.parquet
              ...
        globals/
          manifest.json           # optional; we maintain it
          hk_spot_all.parquet     # global dataset example
    """

    def __init__(
        self,
        root_dir: Union[str, Path],
        *,
        engine: str = "pyarrow",
        compression: str = "zstd",
    ):
        self.root = ensure_dir(Path(root_dir))
        self.engine = engine
        self.compression = compression

        self.tickers_root = ensure_dir(self.root / "tickers")
        self.globals_root = ensure_dir(self.root / "globals")

        # ensure a global manifest exists
        gmf = self._global_manifest_path()
        if not gmf.exists():
            self._write_json_atomic(gmf, {"globals": {}, "updated_at": _dt_to_iso(now_utc())})

    # ----------------------------
    # Path helpers
    # ----------------------------
    def _ticker_dir(self, ticker: str) -> Path:
        t = normalize_hk_ticker(ticker)
        return self.tickers_root / t

    def _datasets_dir(self, ticker: str) -> Path:
        return self._ticker_dir(ticker) / "datasets"

    def _manifest_path(self, ticker: str) -> Path:
        return self._ticker_dir(ticker) / "manifest.json"

    def _dataset_path(self, ticker: str, key: str) -> Path:
        # keep key as-is; users own the namespace
        return self._datasets_dir(ticker) / f"{key}.parquet"

    def _global_manifest_path(self) -> Path:
        return self.globals_root / "manifest.json"

    def _global_path(self, key: str) -> Path:
        # global datasets also stored as parquet (or json if you choose)
        return self.globals_root / f"{key}.parquet"

    # ----------------------------
    # Atomic writers
    # ----------------------------
    # 原子写文件：先写临时文件，再 os.replace 覆盖目标文件。 防止写到一半崩溃导致文件损坏。
    def _atomic_write_bytes(self, path: Path, data: bytes) -> None:
        ensure_dir(path.parent)
        fd, tmp_path = tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent))
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path)
        except Exception:
            try:
                os.remove(tmp_path)
            except Exception:
                pass
            raise

    # 把 dict 写入 JSON（原子写，带 pretty indent）。
    def _write_json_atomic(self, path: Path, obj: Dict[str, Any]) -> None:
        payload = json.dumps(obj, ensure_ascii=False, indent=2, default=_safe_json).encode("utf-8")
        self._atomic_write_bytes(path, payload)

    def _read_json(self, path: Path) -> Dict[str, Any]:
        if not path.exists():
            raise StorageError(f"JSON not found: {path}")
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    # 把 DataFrame 写 parquet，并用“写 tmp + replace”保证原子性。
    def _write_df_atomic(self, path: Path, df: pd.DataFrame) -> None:
        """
        Atomic write for parquet by writing to temp file then replace.
        """
        ensure_dir(path.parent)
        tmp = path.with_suffix(path.suffix + ".tmp")
        # Write to tmp (overwrite if exists)
        df.to_parquet(tmp, engine=self.engine, compression=self.compression, index=False)
        os.replace(tmp, path)

    def _read_df(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            raise StorageError(f"Parquet not found: {path}")
        return pd.read_parquet(path, engine=self.engine)

    # ----------------------------
    # Listing / existence
    # ----------------------------
    # 判断 root/tickers/{ticker} 是否存在
    def exists_ticker(self, ticker: str) -> bool:
        return self._ticker_dir(ticker).exists()

    # 列出所有已保存并且包含 manifest.json 的 ticker
    def list_tickers(self) -> List[str]:
        out: List[str] = []
        for p in self.tickers_root.iterdir():
            if p.is_dir() and (p / "manifest.json").exists():
                out.append(p.name)
        out.sort()
        return out

    # 仅检查 parquet 文件是否存在（不看 manifest）
    def exists_dataset(self, ticker: str, key: str) -> bool:
        return self._dataset_path(ticker, key).exists()

    # 扫描 datasets/ 下的 parquet 文件名（stem）作为 key 列表
    def list_datasets(self, ticker: str) -> List[str]:
        ddir = self._datasets_dir(ticker)
        if not ddir.exists():
            return []
        keys = [p.stem for p in ddir.glob("*.parquet") if p.is_file()]
        keys.sort()
        return keys

    # ----------------------------
    # Manifest helpers
    # ----------------------------
    # 直接返回某 ticker 的 manifest dict
    def get_manifest(self, ticker: str) -> Dict[str, Any]:
        return self._read_json(self._manifest_path(ticker))

    # 构造 dataset 元信息（rows/cols/updated_at/path）
    def _build_dataset_meta(self, path: Path, df: pd.DataFrame) -> Dict[str, Any]:
        return {
            "path": str(path.name),  # relative to datasets/
            "rows": int(df.shape[0]),
            "cols": int(df.shape[1]),
            "updated_at": _dt_to_iso(now_utc()),
        }

    # ----------------------------
    # Bundle I/O
    # ----------------------------
    def save_bundle(self, bundle: Bundle, *, overwrite: bool = True, prune: bool = True) -> None:
        """
        Full write (覆盖写):
          - write each dataset parquet
          - write manifest.json
          - optionally prune old datasets not present anymore
        """
        bundle.validate()
        t = normalize_hk_ticker(bundle.ticker)

        tdir = ensure_dir(self._ticker_dir(t))
        ddir = ensure_dir(self._datasets_dir(t))

        # prune stale parquet files if requested
        if prune and ddir.exists():
            existing = {p.stem for p in ddir.glob("*.parquet")}
        else:
            existing = set()

        datasets_meta: Dict[str, Any] = {}
        for key, df in bundle.data.items():
            if not isinstance(df, pd.DataFrame):
                raise StorageError(f"Dataset {key} is not a DataFrame")

            fpath = self._dataset_path(t, key)
            if fpath.exists() and not overwrite:
                raise StorageError(f"Dataset already exists and overwrite=False: {t}/{key}")

            self._write_df_atomic(fpath, df)
            datasets_meta[key] = self._build_dataset_meta(fpath, df)

        if prune:
            keep = set(bundle.data.keys())
            for k in existing - keep:
                try:
                    (ddir / f"{k}.parquet").unlink(missing_ok=True)
                except Exception:
                    pass

        manifest = {
            "ticker": t,
            "asof": _dt_to_iso(bundle.asof),
            "datasets": datasets_meta,
            "meta": bundle.meta or {},
            "errors": bundle.errors or {},
            "updated_at": _dt_to_iso(now_utc()),
        }
        self._write_json_atomic(self._manifest_path(t), manifest)

    def load_bundle(self, ticker: str, *, load: Optional[List[str]] = None) -> Bundle:
        """
        Read a bundle back into memory.
        If load is provided, only load those dataset keys (others are omitted from Bundle.data).
        """
        t = normalize_hk_ticker(ticker)
        manifest = self._read_json(self._manifest_path(t))

        asof = _iso_to_dt(manifest["asof"])
        meta = manifest.get("meta", {}) or {}
        errors = manifest.get("errors", {}) or {}
        datasets = manifest.get("datasets", {}) or {}

        if load is None:
            keys = list(datasets.keys())
        else:
            keys = list(load)

        data: Dict[DatasetName, pd.DataFrame] = {}
        ddir = self._datasets_dir(t)

        for key in keys:
            info = datasets.get(key)
            if info is None:
                # dataset not present; keep absent rather than error
                continue
            rel = info.get("path", f"{key}.parquet")
            path = ddir / rel
            data[key] = self._read_df(path)

        b = Bundle(ticker=t, asof=asof, data=data, meta=meta, errors=errors)
        b.validate()
        return b

    # ----------------------------
    # Dataset-level update / delete
    # ----------------------------
    def has_dataset(self, ticker: str, key: str) -> bool:
        """
        Check manifest + file existence.
        """
        t = normalize_hk_ticker(ticker)
        mp = self._manifest_path(t)
        if not mp.exists():
            return False
        m = self._read_json(mp)
        ds = m.get("datasets", {})
        if key not in ds:
            return False
        ddir = self._datasets_dir(t)
        rel = ds[key].get("path", f"{key}.parquet")
        return (ddir / rel).exists()

    def update_dataset(
        self,
        ticker: str,
        key: str,
        df: pd.DataFrame,
        *,
        must_exist: bool = True,
        update_asof: bool = False,
    ) -> None:
        """
        Replace a single dataset parquet and update its manifest entry.
        - must_exist=True: only allow update if key already exists in manifest
        - update_asof: whether to bump bundle.asof to now (default False)
        """
        t = normalize_hk_ticker(ticker)
        mp = self._manifest_path(t)
        if not mp.exists():
            raise StorageError(f"Ticker not found: {t}")

        manifest = self._read_json(mp)
        datasets = manifest.get("datasets", {}) or {}

        if must_exist and key not in datasets:
            raise KeyError(f"Dataset key not found in manifest for {t}: {key}")

        # ensure datasets dir exists
        ddir = ensure_dir(self._datasets_dir(t))

        # choose path
        rel = datasets.get(key, {}).get("path", f"{key}.parquet")
        path = ddir / rel

        self._write_df_atomic(path, df)

        datasets[key] = self._build_dataset_meta(path, df)
        manifest["datasets"] = datasets
        manifest["updated_at"] = _dt_to_iso(now_utc())
        if update_asof:
            manifest["asof"] = _dt_to_iso(now_utc())

        self._write_json_atomic(mp, manifest)

    def delete_dataset(self, ticker: str, key: str, *, must_exist: bool = False) -> None:
        """
        Delete a dataset parquet and remove it from manifest.
        """
        t = normalize_hk_ticker(ticker)
        mp = self._manifest_path(t)
        if not mp.exists():
            if must_exist:
                raise StorageError(f"Ticker not found: {t}")
            return

        manifest = self._read_json(mp)
        datasets = manifest.get("datasets", {}) or {}

        if key not in datasets:
            if must_exist:
                raise KeyError(f"Dataset key not found in manifest for {t}: {key}")
            return

        ddir = self._datasets_dir(t)
        rel = datasets[key].get("path", f"{key}.parquet")
        path = ddir / rel

        try:
            path.unlink(missing_ok=True)
        finally:
            datasets.pop(key, None)
            manifest["datasets"] = datasets
            manifest["updated_at"] = _dt_to_iso(now_utc())
            self._write_json_atomic(mp, manifest)

    def delete_ticker(self, ticker: str, *, must_exist: bool = False) -> None:
        """
        Delete all data for a ticker (directory removal).
        """
        t = normalize_hk_ticker(ticker)
        tdir = self._ticker_dir(t)
        if not tdir.exists():
            if must_exist:
                raise StorageError(f"Ticker not found: {t}")
            return
        shutil.rmtree(tdir)

    # ----------------------------
    # Globals (e.g., hk_spot_all)
    # ----------------------------
    def save_global_df(self, key: str, df: pd.DataFrame, *, overwrite: bool = True) -> None:
        """
        Save a global DataFrame once (e.g., hk_spot_all).
        Access by key later.
        """
        path = self._global_path(key)
        if path.exists() and not overwrite:
            raise StorageError(f"Global key exists and overwrite=False: {key}")

        self._write_df_atomic(path, df)

        gmf_path = self._global_manifest_path()
        gmf = self._read_json(gmf_path)
        globals_meta = gmf.get("globals", {}) or {}

        globals_meta[key] = {
            "path": str(path.name),
            "rows": int(df.shape[0]),
            "cols": int(df.shape[1]),
            "updated_at": _dt_to_iso(now_utc()),
        }
        gmf["globals"] = globals_meta
        gmf["updated_at"] = _dt_to_iso(now_utc())
        self._write_json_atomic(gmf_path, gmf)

    def load_global_df(self, key: str) -> pd.DataFrame:
        """
        Load a global DataFrame by key.
        """
        gmf = self._read_json(self._global_manifest_path())
        globals_meta = gmf.get("globals", {}) or {}
        info = globals_meta.get(key)

        # If not in manifest, fall back to direct file path
        if info is None:
            path = self._global_path(key)
            return self._read_df(path)

        path = self.globals_root / info.get("path", f"{key}.parquet")
        return self._read_df(path)

    def list_globals(self) -> List[str]:
        gmf = self._read_json(self._global_manifest_path())
        globals_meta = gmf.get("globals", {}) or {}
        keys = list(globals_meta.keys())
        keys.sort()
        return keys

    def delete_global(self, key: str, *, must_exist: bool = False) -> None:
        gmf_path = self._global_manifest_path()
        gmf = self._read_json(gmf_path)
        globals_meta = gmf.get("globals", {}) or {}

        info = globals_meta.get(key)
        if info is None:
            if must_exist:
                raise KeyError(f"Global key not found: {key}")
            # try direct delete anyway
            path = self._global_path(key)
            path.unlink(missing_ok=True)
            return

        path = self.globals_root / info.get("path", f"{key}.parquet")
        path.unlink(missing_ok=True)

        globals_meta.pop(key, None)
        gmf["globals"] = globals_meta
        gmf["updated_at"] = _dt_to_iso(now_utc())
        self._write_json_atomic(gmf_path, gmf)
