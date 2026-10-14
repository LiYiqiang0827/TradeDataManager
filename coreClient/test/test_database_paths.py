import os
from pathlib import Path

from coreClient._paths import database_root, offline_data_dir, online_data_dir, manager_root, research_root


def test_explicit_database_root_separates_offline_and_online(monkeypatch, tmp_path):
    root = tmp_path / "database-root"
    monkeypatch.setenv("TRADING_DATABASE_ROOT", str(root))
    assert database_root() == root
    assert offline_data_dir() == root / "offline"
    assert online_data_dir() == root / "online"


def test_legacy_agent_root_never_redirects_database_root(monkeypatch, tmp_path):
    monkeypatch.delenv("TRADING_DATABASE_ROOT", raising=False)
    legacy = tmp_path / "old-agent-root"
    monkeypatch.setenv("TRADE_AGENT_ROOT_PATH", str(legacy))
    assert database_root() == Path.home() / "TradingDatabase"


def test_explicit_manager_root_is_independent(monkeypatch, tmp_path):
    manager = tmp_path / "manager"
    manager.mkdir()
    monkeypatch.setenv("TRADING_DATA_MANAGER_ROOT", str(manager))
    assert manager_root() == manager


def test_research_root_is_independent(monkeypatch, tmp_path):
    manager = tmp_path / "manager"
    database = tmp_path / "database"
    research = tmp_path / "research"
    manager.mkdir()
    monkeypatch.setenv("TRADING_DATA_MANAGER_ROOT", str(manager))
    monkeypatch.setenv("TRADING_DATABASE_ROOT", str(database))
    monkeypatch.setenv("TRADING_RESEARCH_ROOT", str(research))
    assert manager_root() == manager
    assert database_root() == database
    assert research_root() == research
