#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-archive}"
START="${2:-$(date -u -d '1 day ago' +%F)}"
END="${3:-$(date -u +%F)}"

pixi run htcondor-accounting extract \
  --start "${START}" \
  --end "${END}" \
  --output-root "${ROOT}"
