#!/usr/bin/env bash

set -u

iterations="${1:-3000}"
jobs="${2:-${JOBS:-}}"

if [[ -z "$jobs" ]]; then
  if command -v nproc >/dev/null 2>&1; then
    jobs="$(nproc)"
  elif command -v sysctl >/dev/null 2>&1; then
    jobs="$(sysctl -n hw.ncpu)"
  else
    jobs=4
  fi
fi

if [[ "$jobs" -lt 1 ]]; then
  jobs=1
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir"

tmp_dir="$(mktemp -d)"
results_file="$(mktemp)"
cleanup_logs=1
interrupted=0

cleanup() {
  if [[ "$cleanup_logs" -eq 1 ]]; then
    rm -rf "$tmp_dir" "$results_file"
  fi
}

trap 'cleanup' EXIT
trap 'interrupted=1' INT TERM

export TMP_DIR="$tmp_dir"
export RESULTS_FILE="$results_file"

seq 1 "$iterations" | xargs -n1 -P "$jobs" -I{} bash -c '
  i="$1"
  log="$TMP_DIR/run-$i.log"
  if go test -run 3A -count=1 >"$log" 2>&1; then
    echo "PASS $i"
    printf "PASS %s\n" "$i" >>"$RESULTS_FILE"
    rm -f "$log"
  else
    echo "FAIL $i"
    printf "FAIL %s %s\n" "$i" "$log" >>"$RESULTS_FILE"
    echo "Run $i failed:" >&2
    if [[ -f "$log" ]]; then
      cat "$log" >&2
    else
      echo "Missing log $log" >&2
    fi
  fi
' _ {}
xargs_status=$?
if [[ "$xargs_status" -ne 0 ]]; then
  interrupted=1
fi

pass=$(grep -c "^PASS " "$results_file" || true)
fail=$(grep -c "^FAIL " "$results_file" || true)

printf "\nSummary: pass=%d fail=%d (jobs=%d)\n" "$pass" "$fail" "$jobs"
if [[ "$interrupted" -ne 0 ]]; then
  printf "Interrupted; results may be incomplete.\n"
fi
if [[ "$fail" -gt 0 || "$interrupted" -ne 0 ]]; then
  cleanup_logs=0
  printf "Failure logs kept in %s\n" "$tmp_dir"
fi
