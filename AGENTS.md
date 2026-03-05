# Repository Guidelines

## Project Structure & Module Organization
- Core package: `lib/hk_quant_strategy_platform/` (data fetching, bundling, storage, refresh, analysis helpers).
- Legacy mirrors exist in `lib/` and `lib/stock_data_analysis/`; make changes in `hk_quant_strategy_platform` first and only sync others when required.
- Tests currently live under `lib/` and package subfolders (for example `lib/test_storage.py`, `lib/hk_quant_strategy_platform/test_utils.py`).
- Data assets are in `stock_data/` (`globals/`, `tickers/<code>/datasets/*.parquet`, mapping `.jsonl` files).
- Research notebook: `烟蒂股筛选.ipynb`.

## Build, Test, and Development Commands
- Create environment and install dev deps:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```
- Run tests:
```bash
pytest
```
- Run a subset while iterating:
```bash
pytest -k storage
```
- Install package in editable mode for imports from any working directory:
```bash
pip install -e .
```

## Coding Style & Naming Conventions
- Python 3.11+ with 4-space indentation, PEP 8 naming (`snake_case` for functions/modules, `PascalCase` for classes, `UPPER_CASE` constants).
- Keep ticker identifiers zero-padded to 5 digits (e.g., `"00001"`) to match storage layout.
- Prefer type hints for public functions and dataclass-style structured objects where already used.
- Keep modules focused: fetchers/crawlers for I/O, storage for persistence, analyzer modules for strategy logic.

## Testing Guidelines
- Framework: `pytest` (`pytest.ini` sets `testpaths = lib`, `python_files = test_*.py`).
- Name tests `test_<behavior>()`; keep fixtures deterministic (see fixed timestamps used in storage tests).
- Add regression tests for any change touching parquet schema, manifest handling, or bundle comparisons.

## Commit & Pull Request Guidelines
- Existing history uses short, single-line summaries in either English or Chinese.
- Prefer imperative, scoped messages like `storage: fix manifest prune behavior`.
- PRs should include: purpose, key files changed, test evidence (`pytest` output summary), and data-impact notes (schema/path/backfill effects).
- Link related issues/tasks; include notebook screenshots only when UI/visual outputs changed.

## Security & Configuration Tips
- Do not commit secrets, local `.env`, or private API keys.
- Keep large raw datasets out of Git; use the curated `stock_data/` sample footprint unless explicitly planning a data migration.
