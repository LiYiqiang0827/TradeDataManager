import os
import shutil
import subprocess
from pathlib import Path

import pytest


TOKEN_SENTINEL = "launcher-secret-must-not-print"


def _write_fake_python(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "#!/bin/zsh\n"
        '[[ "$TRADING_AGENT_TUSHARE_TOKEN" == "$EXPECTED_TOKEN" ]] || exit 31\n'
        '[[ "$TRADING_DATA_MANAGER_ROOT" == "$EXPECTED_ROOT" ]] || exit 32\n'
        '[[ "$PYTHONPATH" == "$EXPECTED_ROOT"* ]] || exit 33\n'
        '[[ "$PWD" == "$EXPECTED_ROOT/offlineDataManager/scripts" ]] || exit 34\n'
        '[[ "$1 $2 $3" == "-m scheduler.scheduler_updateData --daemon" ]] || exit 35\n'
        '[[ "$4" == "--skip-startup-update" ]] || exit 36\n'
        '[[ "$EXPECTED_PYTHON" == "$0" ]] || exit 37\n'
    )
    path.chmod(0o755)


def _run_fixture(tmp_path: Path, explicit_overrides: bool) -> None:
    zsh = shutil.which("zsh")
    if not zsh:
        pytest.skip("zsh is required to exercise run_scheduler.sh")

    root = tmp_path / "manager"
    launcher_dir = root / "scripts"
    launcher_dir.mkdir(parents=True)
    shutil.copy2(Path(__file__).with_name("run_scheduler.sh"), launcher_dir / "run_scheduler.sh")
    (root / "offlineDataManager/scripts/scheduler").mkdir(parents=True)

    expected_python = root / ".venv/bin/python"
    if explicit_overrides:
        expected_python = tmp_path / "explicit-python"
        env_file = tmp_path / "explicit.env"
        env_file.write_text(
            f"TRADING_AGENT_TUSHARE_TOKEN={TOKEN_SENTINEL}\n"
            "TRADING_MANAGER_PYTHON=/must/not/override/explicit/python\n"
        )
        env_file_arg = str(env_file)
    else:
        (root / ".env").write_text(f"TRADING_AGENT_TUSHARE_TOKEN={TOKEN_SENTINEL}\n")
        env_file_arg = None
    _write_fake_python(expected_python)

    env = os.environ.copy()
    for name in ("TRADING_AGENT_ENV_FILE", "TRADING_DATA_MANAGER_ROOT", "TRADING_MANAGER_PYTHON"):
        env.pop(name, None)
    env.update(
        EXPECTED_TOKEN=TOKEN_SENTINEL,
        EXPECTED_ROOT=str(root),
        EXPECTED_PYTHON=str(expected_python),
    )
    if env_file_arg:
        env["TRADING_AGENT_ENV_FILE"] = env_file_arg
        env["TRADING_MANAGER_PYTHON"] = str(expected_python)

    result = subprocess.run(
        [zsh, str(launcher_dir / "run_scheduler.sh"), "offline"],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, (result.returncode, result.stderr)
    assert TOKEN_SENTINEL not in result.stdout
    assert TOKEN_SENTINEL not in result.stderr


def test_launcher_exports_bare_dotenv_values_and_uses_manager_defaults(tmp_path: Path) -> None:
    _run_fixture(tmp_path, explicit_overrides=False)


def test_launcher_preserves_explicit_python_and_env_file_overrides(tmp_path: Path) -> None:
    _run_fixture(tmp_path, explicit_overrides=True)
