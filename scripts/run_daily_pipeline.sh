#!/usr/bin/env bash
set -euo pipefail

OUTPUT_ROOT="${1:-archive}"
DAY="${2:-$(date -u -d '1 day ago' +%F)}"
PUSH_ENABLED="${HTCONDOR_ACCOUNTING_PUSH:-1}"

pixi run htcondor-accounting snapshot-history \
  --start "${DAY}" \
  --end "${DAY}" \
  --output-root "${OUTPUT_ROOT}"

pixi run htcondor-accounting extract \
  --start "${DAY}" \
  --end "${DAY}" \
  --output-root "${OUTPUT_ROOT}"

pixi run htcondor-accounting derive-daily \
  --day "${DAY}" \
  --output-root "${OUTPUT_ROOT}"

pixi run htcondor-accounting derive-rollups \
  --output-root "${OUTPUT_ROOT}"

pixi run htcondor-accounting export-apel-daily \
  --day "${DAY}" \
  --output-root "${OUTPUT_ROOT}"

if [[ "${PUSH_ENABLED}" != "0" ]]; then
  pixi run htcondor-accounting push-apel-daily \
    --day "${DAY}" \
    --output-root "${OUTPUT_ROOT}"
  PUSH_STATUS="enabled"
else
  PUSH_STATUS="skipped"
fi

printf 'Daily pipeline complete: day=%s output_root=%s push=%s\n' "${DAY}" "${OUTPUT_ROOT}" "${PUSH_STATUS}"
