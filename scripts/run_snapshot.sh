#!/usr/bin/env bash
set -euo pipefail

OUTPUT_ROOT="${1:-archive}"
DAY="${2:-$(date -u -d '1 day ago' +%F)}"

pixi run htcondor-accounting snapshot-history \
  --start "${DAY}" \
  --end "${DAY}" \
  --output-root "${OUTPUT_ROOT}"
