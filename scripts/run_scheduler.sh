#!/bin/zsh
set -e

script_dir="${0:A:h}"
manager_root_override="${TRADING_DATA_MANAGER_ROOT:-}"
database_root_override="${TRADING_DATABASE_ROOT:-}"
python_override="${TRADING_MANAGER_PYTHON:-}"
if [[ -n "$manager_root_override" ]]; then
  root="$manager_root_override"
else
  root="$(cd -- "$script_dir/.." && pwd -P)"
fi
if [[ ! -d "$root" ]]; then
  print -u2 "TRADING_DATA_MANAGER_ROOT is not a directory"
  exit 1
fi
root="$(cd -- "$root" && pwd -P)"

env_file="${TRADING_AGENT_ENV_FILE:-$root/.env}"
if [[ ! -f "$env_file" ]]; then
  print -u2 "TRADING_AGENT_ENV_FILE does not exist"
  exit 1
fi
# .env.example uses ordinary KEY=value lines. Export values while sourcing it,
# then restore the caller's previous allexport setting.
had_allexport=0
if [[ -o allexport ]]; then
  had_allexport=1
fi
set -a
source "$env_file"
if (( ! had_allexport )); then
  set +a
fi

# Keep explicit launchd overrides stable if the shared credential file also
# defines path settings. The environment file supplies secrets and other config.
export TRADING_DATA_MANAGER_ROOT="$root"
if [[ -n "$python_override" ]]; then
  export TRADING_MANAGER_PYTHON="$python_override"
fi
if [[ -n "$database_root_override" ]]; then
  export TRADING_DATABASE_ROOT="$database_root_override"
else
  export TRADING_DATABASE_ROOT="${TRADING_DATABASE_ROOT:-$HOME/TradingDatabase}"
fi

python_bin="${TRADING_MANAGER_PYTHON:-$root/.venv/bin/python}"
if [[ ! -x "$python_bin" ]]; then
  print -u2 "TRADING_MANAGER_PYTHON is not executable"
  exit 1
fi
export PATH="/Users/nickzhang/.local/bin:$(dirname "$python_bin"):$PATH"
export PYTHONPATH="$root${PYTHONPATH:+:$PYTHONPATH}"

case "$1" in
  offline)
    cd "$root/offlineDataManager/scripts"
    exec "$python_bin" -m scheduler.scheduler_updateData --daemon --skip-startup-update
    ;;
  online)
    cd "$root/onlineDataManager/scripts"
    exec "$python_bin" -m scheduler.scheduler_onlineData --daemon
    ;;
  *)
    print -u2 'usage: run_scheduler.sh offline|online'
    exit 2
    ;;
esac
