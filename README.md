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

## Day Validation

Run a day-level validation pass to compare pipeline stages, identity quality, and APEL state:

```bash
pixi run htcondor-accounting validate-day --day 2026-04-21
```

Machine-readable output is also available:

```bash
pixi run htcondor-accounting validate-day --day 2026-04-21 --format json
```

This is intended as a practical pre/post-production sanity check. It reads only local files, compares counts across raw history, canonical, derived, staged, and pushed outputs, and highlights missing VO/FQAN/accounting-group fields.

## Monthly Reports

Render a monthly internal report from derived daily jobs:

```bash
pixi run htcondor-accounting render-monthly --year 2026 --month 4
```

This writes:

- `archive/reports/monthly/2026/04/users.csv`
- `archive/reports/monthly/2026/04/vos.csv`
- `archive/reports/monthly/2026/04/accounting_groups.csv`
- `archive/reports/monthly/2026/04/summary.json`
- `archive/reports/monthly/2026/04/index.html`
- `archive/reports/monthly/2026/04/schedds/<schedd>/index.html`

`users.csv` now includes a resolved `vo` column, `vos.csv` includes a distinct `users` count, and `accounting_groups.csv` provides an internal accounting-group cross-check view.
The monthly HTML page is rendered from Jinja templates in `src/htcondor_accounting/templates/`, uses relative links, and keeps presentation logic separate from Python data preparation.
Per-schedd monthly pages are generated alongside the top-level month report and link back to the parent overview with relative paths.
