# Repository Guidelines

This trading research workspace orchestrates Zipline pipelines, Alpaca execution scripts, and analytics notebooks. Contributions should protect live-trading safety and keep data reproducible.

## Project Structure & Module Organization
- `alpaca_single_factor.py` runs the live Zipline algorithm using the Alpaca broker; factor pipeline defined inline.
- `dailyReport.py` builds the factor report and emails results; persists analytics to `daily.pkl` consumed by `dailyExecute.py`.
- `dailyExecute.py` places limit orders for the daily signals.
- `get_eps.py` collects fundamentals into ClickHouse; supporting notebooks in the project root investigate inputs.
- Auxiliary assets: `zipline.yaml` defines the bundle universe, `.ipynb` notebooks cover data validation, and `demo.state` stores Zipline state. Keep reusable utilities in dedicated modules rather than notebooks.

## Build, Test, and Development Commands
- `python dailyReport.py 2024-09-20` regenerates the analytics snapshot for a trading date (must be a recent session).
- `python dailyExecute.py` submits orders for symbols flagged in `daily.pkl`; run only against paper trading.
- `python alpaca_single_factor.py` streams the Zipline algorithm end-to-end; export Alpaca credentials before launch.
- `python test_minute.py` validates bundle minute data availability; adjust dates before use.

Ensure the Zipline bundle `alpaca_api` is ingested and that `/home/wei/Documents/zipline-yaml/zipline-trader.yaml` is reachable or overridden via environment variables.

## Coding Style & Naming Conventions
Follow PEP 8: 4-space indentation, snake_case for functions/variables, CapWords for classes. Co-locate configuration in helpers, avoid hard-coded absolute paths, and document credentials or calendar assumptions inline. Prefer pandas/vectorized operations, and keep module-level constants (e.g., `N_LONGS`) uppercase.

## Testing Guidelines
No formal test suite exists; add targeted pytest modules under `tests/` for new logic. Mock Alpaca/Yahoo/ClickHouse clients to keep runs offline and deterministic. Regenerate `daily.pkl` with sample data before testing order flows, and verify Zipline bundles with `test_minute.py` after ingestion. Include data snapshots or fixtures for regression checks.

## Commit & Pull Request Guidelines
Git history is unavailable; default to Conventional Commits (`feat:`, `fix:`, `refactor:`) with imperative subjects under 72 characters. Pull requests should explain intent, list risk mitigations, attach command outputs (`python …`), and include screenshots of analytics tables when UI-facing. Flag any configuration adjustments and confirm secret handling in the description.

## Configuration & Security Notes
Never commit API keys, email passwords, or `daily.pkl`. External config currently lives in `zipline-trader.yaml`; prefer loading via environment variables or `.env` files. When sharing notebooks, scrub tokens and financial account metadata before pushing.
