# Repository Guidelines

## Project Structure & Module Organization
Core scripts live at repo root: `alpaca_single_factor.py` (live Zipline run with inline factor pipeline), `dailyReport.py` (report build & analytics export), `dailyExecute.py` (limit order placement from `daily.pkl`), and `get_eps.py` (fundamentals loader for ClickHouse that normalizes analyst EPS ranges to USD via Yahoo FX rates). Analytics notebooks and `demo.state` sit alongside these scripts; reusable helpers should move into modules rather than notebooks. Keep the Zipline bundle configuration in `zipline.yaml`, and avoid committing generated artifacts such as `daily.pkl`.

## Build, Test, and Development Commands
Run `python dailyReport.py 2024-09-20` to regenerate analytics for a recent trading session; the date argument should match an active market day. Execute `python dailyExecute.py` only against paper trading once `daily.pkl` is refreshed. Kick off the full Zipline pipeline with `python alpaca_single_factor.py` after setting Alpaca credentials and ensuring the `alpaca_api` bundle is ingested. Validate bundle minute data via `python test_minute.py` (adjust the date range before use).

## Coding Style & Naming Conventions
Follow PEP 8: 4-space indentation, snake_case for functions and variables, CapWords for classes. Keep module-level constants uppercase (e.g., `N_LONGS`). Centralize configuration lookups, prefer environment variables over absolute paths, and document assumptions (calendar, credentials) near the logic that depends on them.

## Testing Guidelines
Add targeted pytest modules under `tests/` for new logic, mocking Alpaca, Yahoo, and ClickHouse clients to keep runs offline. Refresh `daily.pkl` with sample data before exercising order flows, and run `python test_minute.py` after bundle ingestion to confirm data integrity. Include fixtures or snapshots when fixing regressions, and rerun `python get_eps.py` against a cross-currency ticker to confirm USD-normalized analyst ranges.

## Commit & Pull Request Guidelines
Use Conventional Commit messages (`feat:`, `fix:`, `refactor:`) with imperative subjects under 72 characters. Pull requests should describe intent, outline risk mitigations, attach relevant command outputs (e.g., `python dailyReport.py ...`), and include screenshots for analytics changes. Flag configuration adjustments and confirm secret handling in the description.

## Security & Configuration Tips
Never commit API keys, email passwords, or `daily.pkl`. Source Alpaca credentials and bundle overrides from environment variables or `.env` files; default Zipline config expects `/home/wei/Documents/zipline-yaml/zipline-trader.yaml`. Scrub notebooks of tokens and financial identifiers before sharing.
