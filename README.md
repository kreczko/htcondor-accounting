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
