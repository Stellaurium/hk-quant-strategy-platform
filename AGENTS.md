# Repository Guidelines

## Project Structure & Module Organization
- Core package: `lib/hk_quant_strategy_platform/` (fetchers, crawler, bundle, storage, refresh, analysis helpers).
- Single source of truth: do all code changes in `lib/hk_quant_strategy_platform/`.
- Tests live under `lib/` (for example `lib/test_storage.py`, `lib/test_refresh_module.py`).
- Data assets live in `stock_data/` (`globals/`, `tickers/<code>/datasets/*.parquet`, mapping `.jsonl`).
- Research notebook: `烟蒂股筛选.ipynb`.

## Build, Test, and Development Commands
- Create environment and install dev deps:
```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
# source .venv/bin/activate
pip install -r requirements-dev.txt
```
- Run all tests:
```bash
pytest
```
- Run a subset:
```bash
pytest -k storage
```
- Editable install:
```bash
pip install -e .
```

## Coding Style & Naming Conventions
- Python, 4-space indentation, PEP 8 naming.
- Keep ticker identifiers zero-padded to 5 digits (e.g., `"00001"`).
- Prefer type hints on public functions.
- Keep modules focused: I/O in fetch/crawl/storage, strategy logic in analyzer modules.

## Testing Guidelines
- Framework: `pytest` (`pytest.ini`: `testpaths = lib`, `python_files = test_*.py`).
- Test names: `test_<behavior>()`.
- Add regression tests for parquet schema, manifest handling, and bundle comparisons.

## Commit & Pull Request Guidelines
- Use short imperative commit messages, scoped by module when possible.
- PR should include purpose, key file changes, and test evidence (`pytest` summary).
- Note any data-impact changes (schema/path/backfill).

## Security & Configuration Tips
- Do not commit secrets, local `.env`, or API keys.
- Keep large raw datasets out of Git; commit only curated sample footprints.
