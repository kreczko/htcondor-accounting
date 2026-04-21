# Operations

## Daily Order

The recommended daily order is:

1. snapshot raw history
2. extract canonical records
3. derive daily outputs
4. derive rollups
5. export staged APEL messages
6. push staged APEL messages into the outgoing queue

This is the order used by `scripts/run_daily_pipeline.sh`.

## Why Snapshot Comes First

HTCondor history is not a permanent store. Taking a raw snapshot first preserves the original evidence before later canonicalization, derivation, or export changes make debugging harder.

## Why Export Happens After Daily Derivation

APEL export should use the deduplicated daily derived jobs as its source of truth. That keeps export aligned with the same cleaned accounting view used for reporting and rollups.

## Running The Pipeline

Run the whole pipeline for yesterday in UTC:

```bash
pixi run daily-pipeline
```

Run for a specific day:

```bash
scripts/run_daily_pipeline.sh /srv/htcondor-accounting 2026-04-17
```

Skip push during testing:

```bash
HTCONDOR_ACCOUNTING_PUSH=0 scripts/run_daily_pipeline.sh /srv/htcondor-accounting 2026-04-17
```

## Cron Example

The script computes yesterday in UTC by default, so cron only needs to pass the output root:

```cron
15 01 * * * condor /path/to/htcondor-accounting/scripts/run_daily_pipeline.sh /srv/htcondor-accounting >> /var/log/htcondor-accounting-daily.log 2>&1
```

Manual reruns for a specific day should be done by invoking the script with that day argument directly.
