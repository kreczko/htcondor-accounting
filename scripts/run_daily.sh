#!/usr/bin/env bash
set -euo pipefail

OUTPUT_ROOT="${1:-archive}"
DAY="${2:-$(date -u -d '1 day ago' +%F)}"

pixi run htcondor-accounting derive-daily \
  --day "${DAY}" \
  --output-root "${OUTPUT_ROOT}"

pixi run htcondor-accounting derive-rollups \
  --output-root "${OUTPUT_ROOT}"
