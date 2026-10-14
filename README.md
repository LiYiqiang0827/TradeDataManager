# TradingDataManager

This repository holds offline and online collection services, schedulers, database maintenance code, shared database clients, and the theme graph module. Research strategies remain in TradingAgent. This round initializes local Git only; no remote was requested or configured. Database files and credentials remain excluded from Git.

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
- `theme_graph/`: shared theme graph and catalyst analysis module.

Legacy TradingAgent paths are maintained as compatibility links during cutover. The live data directories remain outside this repository.
