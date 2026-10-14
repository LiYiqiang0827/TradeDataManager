#!/bin/zsh
set -e

root=/Users/nickzhang/TradingAgent
source "$root/.env"
python_bin="${TRADING_MANAGER_PYTHON:-/Users/nickzhang/TradingAgent/.venv/bin/python}"
export PATH="/Users/nickzhang/.local/bin:$(dirname "$python_bin"):$PATH"
export PYTHONPATH="$root${PYTHONPATH:+:$PYTHONPATH}"

case "$1" in
  offline)
    cd "$root/offlineDataManager/scripts"
    exec "$python_bin" -m scheduler.scheduler_updateData --daemon
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
