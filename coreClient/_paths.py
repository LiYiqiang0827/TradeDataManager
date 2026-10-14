"""Path resolution for the independent TradingDataManager and TradingDatabase roots."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Union

MANAGER_ROOT_ENV = "TRADING_DATA_MANAGER_ROOT"
DATABASE_ROOT_ENV = "TRADING_DATABASE_ROOT"
RESEARCH_ROOT_ENV = "TRADING_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path.home() / "TradingAgent" / "MrLi"
DEFAULT_MANAGER_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATABASE_ROOT = Path.home() / "TradingDatabase"

def manager_root(script_file: Union[str, Path] | None = None, n: int = 0) -> Path:
    """Return code root; only TRADING_DATA_MANAGER_ROOT overrides its location."""
    value = os.environ.get(MANAGER_ROOT_ENV)
    if value:
        path = Path(value).expanduser().resolve()
        if not path.is_dir():
            raise RuntimeError(f"{MANAGER_ROOT_ENV} must name an existing directory")
        return path
    if script_file is None:
        return DEFAULT_MANAGER_ROOT
    return Path(script_file).resolve().parents[n]

def database_root() -> Path:
    """Return the root containing offline/ and online/ stores.

    TRADE_AGENT_ROOT_PATH is intentionally ignored: it names the old code tree,
    and must never silently redirect reads or create empty databases there.
    """
    value = os.environ.get(DATABASE_ROOT_ENV)
    if value:
        path = Path(value).expanduser()
        if not path.is_absolute():
            raise RuntimeError(f"{DATABASE_ROOT_ENV} must be an absolute path")
        return path.resolve()
    return DEFAULT_DATABASE_ROOT

def offline_data_dir() -> Path:
    return database_root() / "offline"

def online_data_dir() -> Path:
    return database_root() / "online"

def resolve_root(script_file: Union[str, Path], n: int) -> Path:
    """Backward-compatible project-root helper; resolves the manager code root."""
    return manager_root(script_file, n)


def research_root() -> Path:
    """Return the strategy-artifact root, independent of manager and data roots."""
    value = os.environ.get(RESEARCH_ROOT_ENV)
    if value:
        path = Path(value).expanduser()
        if not path.is_absolute():
            raise RuntimeError(f"{RESEARCH_ROOT_ENV} must be an absolute path")
        return path.resolve()
    return DEFAULT_RESEARCH_ROOT
