# htcondor-accounting

Tools for extracting HTCondor history, deriving daily and rollup accounting views, and staging/pushing APEL exports.

## Daily Operation

Run the full daily workflow for yesterday in UTC:

```bash
pixi run daily-pipeline
```

Run the same workflow for a specific output root and day:

```bash
scripts/run_daily_pipeline.sh /srv/htcondor-accounting 2026-04-17
```

Skip the final APEL push step for testing or staging:

```bash
HTCONDOR_ACCOUNTING_PUSH=0 pixi run daily-pipeline
```

Show the resolved config:

```bash
pixi run show-config
```

## APEL Push Safety

APEL push now uses a file-based ledger under `archive/apel/ledger/` so normal pushes are idempotent by default.

Push one day of staged messages:

```bash
pixi run htcondor-accounting push-apel-daily --day 2026-04-17
```

Force an explicit resend and record it in the resend ledger:

```bash
pixi run htcondor-accounting push-apel-daily --day 2026-04-17 --force-resend
```

Inspect what has already been pushed:

```bash
pixi run htcondor-accounting inspect-apel-ledger --day 2026-04-17
```
