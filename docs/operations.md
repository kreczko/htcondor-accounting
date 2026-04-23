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

## APEL Push Ledger

APEL push uses a file-based ledger under:

- `output_root/apel/ledger/sent/`
- `output_root/apel/ledger/resends/`

This is there to stop the same staged message from being pushed more than once by default. The message identity is the MD5 of the final message body, which also matches the dirq-compatible outgoing queue path.

Default behavior:

- if a sent marker already exists for that MD5, the push step skips the message
- if no sent marker exists, the message is promoted to the outgoing queue and then a sent marker is written

Explicit resend behavior:

- use `--force-resend` to bypass the sent-marker guardrail
- the original sent marker is kept
- a resend event is written under `apel/ledger/resends/`

Examples:

```bash
pixi run htcondor-accounting push-apel-daily --day 2026-04-17
pixi run htcondor-accounting push-apel-daily --day 2026-04-17 --force-resend
pixi run htcondor-accounting inspect-apel-ledger --day 2026-04-17
```

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

## Monthly Reporting

Generate a simple internal monthly report from the deduplicated daily jobs:

```bash
pixi run htcondor-accounting render-monthly --year 2026 --month 4
```

Outputs are written under:

```text
output_root/reports/monthly/YYYY/MM/
```

Per-schedd monthly pages are also generated under:

```text
output_root/reports/monthly/YYYY/MM/schedds/<schedd>/
```

The monthly CSV set currently includes:

- `users.csv` with a resolved `vo` marker per user
- `vos.csv` with a distinct `users` count per VO
- `accounting_groups.csv` for internal accounting-group cross-checking

The monthly `index.html` page links to those CSVs with relative paths and presents a compact human-readable view using hours and GB for display.
It is rendered from Jinja templates in `src/htcondor_accounting/templates/`, which keeps the current one-page monthly layout reusable for later per-schedd reporting work.
The top-level monthly page also links relatively to each available schedd report, and each schedd page links back to the parent monthly overview.

## Day Validation

Use the validator as a lightweight production sanity check before or after pushing APEL messages:

```bash
pixi run htcondor-accounting validate-day --day 2026-04-21
pixi run htcondor-accounting validate-day --day 2026-04-21 --format json
```

The validator is fully file-based. For one day it checks:

- raw-history file and record counts
- canonical file and record counts
- derived daily jobs, summary, and duplicates
- missing resolved VO, missing FQAN, missing accounting group, and auth-method mix
- APEL staged manifests and staged message files
- APEL sent ledger entries and resend events

It is meant to make mismatches visible quickly rather than fail at the first discrepancy.

## Cron Example

The script computes yesterday in UTC by default, so cron only needs to pass the output root:

```cron
15 01 * * * condor /path/to/htcondor-accounting/scripts/run_daily_pipeline.sh /srv/htcondor-accounting >> /var/log/htcondor-accounting-daily.log 2>&1
```

Manual reruns for a specific day should be done by invoking the script with that day argument directly.
