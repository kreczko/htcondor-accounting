# Installation and deployment

This document describes how to install and operate `htcondor-accounting` in production.

The intended deployment model is:

* `condor.manager`

  * runs the accounting pipeline
  * stores local working state under `/var/lib/condor/accounting`
* `site.ce.apel`

  * runs `ssmsend`
  * receives APEL outgoing messages via shared storage handoff
* `monitoring.server`

  * serves generated reports through a web application (e.g. FASTAPI)
* Shared storage (network-mounted):

  * `/mnt/shared/condor/accounting`

The design is deliberately file-based:

* no database
* reproducible pipeline stages
* easy inspection and replay
* easy recovery after interruptions

---

## 1. Requirements

### On `condor.manager`

Required:

* Git
* Pixi
* access to the HTCondor pool for history queries
* writable local storage:

  * `/var/lib/condor/accounting`
* config file:

  * `/etc/htcondor-accounting/site.toml`

Recommended:

* cron
* access to shared storage:

  * `/mnt/shared/condor/accounting`

---

### On `site.ce.apel`

Required:

* Python 3
* writable local APEL spool:

  * `/var/spool/apel/outgoing`
* access to shared storage:

  * `/mnt/shared/condor/accounting`

---

### On `monitoring.server`

Required:

* access to report files, either:

  * directly from shared storage
  * or from a copied local mirror

---

## 2. Repository checkout

Recommended installation location on `condor.manager`:

```bash
mkdir -p /opt
cd /opt
git clone <REPO_URL> htcondor-accounting
cd /opt/htcondor-accounting
```

The cron jobs described below assume the repository lives at:

* `/opt/htcondor-accounting`

Adjust paths if needed.

---

## 3. Install Pixi on `condor.manager`

Install Pixi according to your site preference.

Then initialize the environment:

```bash
cd /opt/htcondor-accounting
pixi install
```

Verify:

```bash
pixi run htcondor-accounting show-config
```

---

## 4. Configuration

Create:

* `/etc/htcondor-accounting/site.toml`

Start from:

* `examples/site-config.toml`

Important:

* omit optional keys instead of using `null`
* leaving extraction limits unset means “no limit”

---

## 5. Local state layout (`condor.manager`)

State root:

* `/var/lib/condor/accounting`

Expected structure:

```text
raw-history/
canonical/
derived/
reports/
apel/
  staging/
  outgoing/
  ledger/
manifests/
```

Create and set permissions:

```bash
mkdir -p /var/lib/condor/accounting
chown -R condor:condor /var/lib/condor/accounting
```

---

## 6. Daily pipeline

Recommended order:

1. snapshot raw history
2. extract canonical records
3. derive daily outputs
4. derive rollups
5. export APEL messages
6. push APEL messages into outgoing
7. render reports
8. validate the day

Run against:

* **yesterday**

to avoid partial data.

---

## 7. Cron on `condor.manager`

Example:

```cron
15 01 * * * condor /opt/htcondor-accounting/scripts/run_daily_pipeline.sh /var/lib/condor/accounting >> /var/log/condor/accounting-daily.log 2>&1
45 01 * * * condor /opt/htcondor-accounting/scripts/run_daily_reports.sh /var/lib/condor/accounting >> /var/log/condor/accounting-reports.log 2>&1
```

The repository also includes copy-editable production cron examples in `examples/cron/htcondor-accounting.cron`.

---

## 8. Shared storage sync

Shared storage path:

* `/mnt/shared/condor/accounting`

Mirror local state:

```bash
rsync -a --delete /var/lib/condor/accounting/ /mnt/shared/condor/accounting/
```

Cron example:

```cron
0 * * * * condor rsync -a --delete /var/lib/condor/accounting/ /mnt/shared/condor/accounting/ >> /var/log/condor/accounting-sync.log 2>&1
```

This provides:

* backup
* replay source
* shared APEL handoff area
* report distribution

---

## 9. APEL handoff (`site.ce.apel`)

The CE consumes APEL messages via shared storage.

### Shared directories

Under:

```text
/mnt/shared/condor/accounting/apel/
```

use:

```text
outgoing/   # produced by condor.manager
retrieved/  # copied into CE spool
sent/       # confirmed sent
```

---

### Local spool

```text
/var/spool/apel/outgoing
```

---

### State machine

```text
outgoing → retrieved → sent
```

---

### Handoff script

A CE-side script should:

1. copy files from shared `outgoing/` to local spool
2. move them to `retrieved/`
3. detect when files disappear from spool
4. move them to `sent/`

---

### Cron on CE

```cron
*/5 * * * * root /usr/bin/python3 /opt/htcondor-accounting/scripts/apel_ce_handoff.py --shared-root /mnt/shared/condor/accounting/apel --spool-root /var/spool/apel/outgoing >> /var/log/condor/apel-ce-handoff.log 2>&1
```

---

## 10. Reports (`monitoring.server`)

Reports are generated under:

```text
/mnt/shared/condor/accounting/reports/
```

Options:

* serve directly from shared storage
* or mirror locally

All links are relative → safe for web mounting.

---

## 11. Manual validation

Before enabling cron:

### Run one day manually

```bash
pixi run htcondor-accounting snapshot-history --start YYYY-MM-DD --end YYYY-MM-DD
pixi run htcondor-accounting extract --start YYYY-MM-DD --end YYYY-MM-DD
pixi run htcondor-accounting derive-daily --day YYYY-MM-DD
pixi run htcondor-accounting derive-rollups
pixi run htcondor-accounting export-apel-daily --day YYYY-MM-DD
pixi run htcondor-accounting push-apel-daily --day YYYY-MM-DD
pixi run htcondor-accounting render-monthly --year YYYY --month MM
pixi run htcondor-accounting validate-day --day YYYY-MM-DD
```

---

### Inspect data

```bash
pixi run htcondor-accounting inspect archive/derived/daily/YYYY/MM/DD/jobs.jsonl.zst --format ndjson --verbosity full | jq .
```

---

## 12. Resend workflow

To resend a message:

```bash
mv apel/sent/... apel/outgoing/...
```

Alternatively:

* regenerate from canonical data on `condor.manager`

---

## 13. Troubleshooting

### Missing VO/FQAN

* use `validate-day`
* inspect records with `jq`

---

### APEL not reaching CE

* check shared `outgoing/`
* check CE script
* check local spool

---

### Files stuck in retrieved

* check `ssmsend`
* check spool consumption

---

### Reports missing

* check shared `reports/`
* verify web server path

---

## 14. First production rollout

1. install on `condor.manager`
2. configure and test locally
3. run manual days
4. verify APEL + reports
5. enable cron on manager
6. enable shared sync
7. enable CE handoff
8. verify APEL send
9. expose reports

---

## Design principle

All state is file-based and reproducible.

This allows:

* full reprocessing
* simple debugging
* transparent operations
* safe recovery

Avoid introducing hidden state unless strictly necessary.
