#!/usr/bin/env bash
# Usage: scripts/chaos.sh <label> [harness flags...]
# Runs the harness inside docker and saves the raw, unedited output to runs/chaos-<label>-<utc>.txt
set -uo pipefail
label="${1:?label}"; shift
out="runs/chaos-${label}-$(date -u +%Y%m%dT%H%M%SZ).txt"
mkdir -p runs
docker compose run --rm -T chaos -label "$label" "$@" 2>&1 | tee "$out"
code=${PIPESTATUS[0]}
echo "exit_code=$code" | tee -a "$out"
exit "$code"
