#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-archive}"
DAY="${2:-$(date -u +%F)}"

pixi run htcondor-accounting export-apel-daily \
  --day "${DAY}" \
  --output-root "${ROOT}"

pixi run htcondor-accounting push-apel-daily \
  --day "${DAY}" \
  --output-root "${ROOT}"