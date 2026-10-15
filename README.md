# TradingDataManager

This repository holds offline and online collection services, schedulers, database maintenance code, shared database clients, and the theme graph module. Research strategies remain in TradingAgent. The source repository is published to the user-provided GitHub remote `git@github.com:LiYiqiang0827/TradeDataManager.git` on `main`. The remote contains code and documentation only; database files, generated research artifacts, and machine credentials remain excluded from Git.

## Roots

- `TRADING_DATA_MANAGER_ROOT` selects this code tree. Default: `/Users/nickzhang/TradingDataManager`.
- `TRADING_RESEARCH_ROOT` selects the strategy-artifact root. Default: `~/TradingAgent/MrLi`; theme analysis outputs go under its `outputs/` directory.
- `TRADING_DATABASE_ROOT` selects the data root. Default: `~/TradingDatabase`; offline services use `offline/`, online SQLite services use `online/`, and Redis persistence uses `online/redis/`.
- `TRADE_AGENT_ROOT_PATH` remains a legacy code-root variable and is ignored when selecting database paths. This prevents old environments from creating or querying an empty database under the strategy checkout.
- `TRADING_AGENT_ENV_FILE` selects the local environment file for scheduler credentials. Default: `/Users/nickzhang/TradingAgent/.env`; secrets are not copied into this repository.
- `TRADING_MANAGER_PYTHON` selects the Python executable for launchd. Its default reuses the existing local TradingAgent virtual environment.

Path helpers do not create database directories on import. An offline read through `get_conn` raises `FileNotFoundError` when the expected store is absent; database initialization remains an explicit operation.

## Layout

- `offlineDataManager/scripts/` and `onlineDataManager/scripts/`: collectors, writers, schedulers, validation and maintenance.
- `offlineDataManager/docs/` and `onlineDataManager/docs/`: manager documentation.
- `coreClient/`: shared provider and database client API, including the stable `data_provider` signatures and `source="database_only"` behavior.
- `scripts/migrations/` and `scripts/research_inputs/`: data-schema tools and data-input collection/orchestration; research outputs are resolved through `TRADING_RESEARCH_ROOT`.
- `skills/`: canonical database-maintenance skills. Strategy-analysis skills remain in TradingAgent.
- `tools/data_checks/`: data-source validation programs; they are code only and write reports outside this repository.
- `theme_graph/`: shared theme graph and catalyst analysis module.

Legacy TradingAgent paths are maintained as compatibility links during cutover. The live data directories remain outside this repository.
See [the final layout and preservation receipt](docs/final_data_restructure_20261015.md) for the compatibility map, protected source archive, and restored LaunchAgent state.

`tools/data_checks/tdx_matrix_validation_v1.py` makes 24 external, read-only TDX history-tick requests when run. Its `--help` option is safe; a normal invocation performs the matrix and writes a report under `${TRADING_DATA_AUDIT_ROOT:-/tmp/trading_data_audit}`. Pass `--output /path/to/report.json` to choose an explicit external location.

## Editable installation on this computer

This project is intended to run from its local source tree. The repository defines an editable Python install and the `trading-data` command; this round does not publish or distribute a wheel. Use Python 3.10 or newer:

```sh
cd ~/TradingDataManager
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
```

`trading-data providers` lists the 49 public `get_*` functions. Query a provider and save a result without printing rows to the terminal:

```sh
trading-data query get_day \
  --params-json '{"ts_code":"600000.SH","trade_date":"2026-10-14"}' \
  --source database_only --output /tmp/600000-day.parquet
```

`database_only` is the CLI default and is passed only to providers that expose a `source` parameter. The CLI writes Parquet, CSV, or JSON; it never prints credentials or provider exception text. Provider calls that need an online source still require that provider's local configuration and credentials.

## Machine-local configuration

Copy `.env.example` to a private file outside the repository, set `TRADING_AGENT_TUSHARE_TOKEN`, and export its variables before using providers. For LaunchAgents, point `TRADING_AGENT_ENV_FILE` at that file. Set `TRADING_DATA_MANAGER_ROOT`, `TRADING_DATABASE_ROOT`, and `TRADING_RESEARCH_ROOT` for the paths on that computer. The code does not require `privateconfig.py`; those files are ignored because machine-local settings must not enter Git. Copy the needed `coreClient/*_config.py.example` template to its ignored `*_config.py` filename before using providers that import that configuration. The example files contain no credentials.

KPL authentication is read from `~/.kpl-api/.auth.json` with `token` and `user_id` keys; keep it outside this repository and restrict file permissions to the current user. Tushare uses `TRADING_AGENT_TUSHARE_TOKEN`. TDX hosts and the THS CLI are machine-specific; adjust their private config templates if those providers are used on another computer. No database directory is created by importing the package; point `TRADING_DATABASE_ROOT` at a provisioned database root first.
