#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

VALIDATION_DIR="output/validation"
STALL_MINUTES="20"
RESTART="0"

for arg in "$@"; do
  case "$arg" in
    --restart)
      RESTART="1"
      ;;
    --stall_minutes=*)
      STALL_MINUTES="${arg#*=}"
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      exit 1
      ;;
  esac
done

mkdir -p "$VALIDATION_DIR"

start_if_missing() {
  local job_name="$1"
  local process_pattern="$2"
  shift 2

  if [[ "$RESTART" == "1" ]]; then
    pkill -f "$process_pattern" >/dev/null 2>&1 || true
  fi

  if pgrep -f "$process_pattern" >/dev/null 2>&1; then
    echo "[skip] ${job_name} already running"
    return 0
  fi

  nohup "$@" >/dev/null 2>&1 &
  local pid="$!"
  echo "[start] ${job_name} pid=${pid}"
}

echo "Starting sPlot monitoring jobs from: $ROOT_DIR"
echo "stall_minutes=${STALL_MINUTES} restart=${RESTART}"

start_if_missing \
  "live progress dashboard" \
  "03_live_progress_dashboard\\.R" \
  Rscript R/03_live_progress_dashboard.R \
    --validation_dir="$VALIDATION_DIR" \
    --output_md="$VALIDATION_DIR/live_completion_dashboard.md" \
    --output_csv="$VALIDATION_DIR/live_completion_snapshot.csv" \
    --history_csv="$VALIDATION_DIR/progress_history.csv" \
    --watch --interval_seconds=60 \
    --bands_low_factor=1.4 --bands_high_factor=0.7 \
    --stall_minutes="$STALL_MINUTES"

start_if_missing \
  "terminal status line" \
  "04_terminal_status_line\\.R" \
  Rscript R/04_terminal_status_line.R \
    --snapshot_csv="$VALIDATION_DIR/live_completion_snapshot.csv" \
    --watch --interval_seconds=60

start_if_missing \
  "first non-NA ETA watcher" \
  "05_wait_for_first_eta\\.R" \
  Rscript R/05_wait_for_first_eta.R \
    --snapshot_csv="$VALIDATION_DIR/live_completion_snapshot.csv" \
    --alert_file="$VALIDATION_DIR/first_non_na_eta_alert.txt" \
    --interval_seconds=60 --timeout_minutes=180

start_if_missing \
  "first non-NA NSR ETA watcher" \
  "06_wait_for_first_nsr_eta\\.R" \
  Rscript R/06_wait_for_first_nsr_eta.R \
    --snapshot_csv="$VALIDATION_DIR/live_completion_snapshot.csv" \
    --alert_file="$VALIDATION_DIR/first_non_na_nsr_eta_alert.txt" \
    --interval_seconds=60 --timeout_minutes=720

echo
echo "Monitoring process snapshot:"
ps -axo pid,stat,etime,command | grep -E "R/(03_live_progress_dashboard|04_terminal_status_line|05_wait_for_first_eta|06_wait_for_first_nsr_eta)\\.R" | grep -v grep || true
