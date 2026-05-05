#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/opt/htcondor-accounting"
OUTPUT_ROOT="${1:-/var/lib/condor/accounting}"
DAY="${2:-$(date -u -d '1 day ago' +%F)}"

cd "${REPO_ROOT}"

pixi run htcondor-accounting render-range \
  --start "${DAY}" \
  --end "${DAY}" \
  --output-root "${OUTPUT_ROOT}"
