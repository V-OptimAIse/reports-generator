# Hourly Local Report Systemd Job

## Purpose

This job stores reports for every completed clock hour on the device:

1. Raw pipe CSVs in each caster's configured `outputs.csv_dir`.
2. Verified pipe CSVs in the same output directory.
3. A verified shift-summary table PNG in `outputs/hourly-report-images`.

It does not send email, run diagnosis XLSX generation, create videos, upload to
Google Drive, or delete history sources. The existing shift workflow and its
systemd jobs remain unchanged.

At the start of every run, the workflow deletes hourly CSVs, table images, and
hourly state older than `hourly_csv_report.retention_days`. It defaults to seven
days and is configured in `config/runtime.yaml`. The cleanup uses hourly-specific
filename patterns, so shift CSVs and unrelated output files are not deleted.

The command omits an explicit date and time, so it selects the previous completed
hour. For example, a run at `02:05` reports `01:00:00` through `02:00:00`:

```bash
uv run python -m cli.hourly_csv_report --all-casters
```

The stop boundary is excluded, so a record at exactly `02:00:00` belongs only to
the next report.

## Manual verification

Generate a local report for one known window:

```bash
uv run python -m cli.hourly_csv_report --date 08-08-2026 --start 01:00 --stop 02:00 --caster caster3
```

If that window is already marked successful, it is skipped. Add `--force` only
when you intentionally need to regenerate it.

## Service definition

Create `/etc/systemd/system/hourly-csv-report.service`:

```ini
[Unit]
Description=Hourly local pipe CSV and table image report

[Service]
Type=oneshot
User=voptimaise-jetson-2
WorkingDirectory=/mnt/ssd/home/voptimaise-jetson-2/dev/reports-generator
Environment=PYTHONUNBUFFERED=1
ExecStart=/mnt/ssd/home/voptimaise-jetson-2/.local/bin/uv run python -m cli.hourly_csv_report --all-casters
StandardOutput=journal
StandardError=journal
TimeoutStartSec=45min
```

No email environment file or network-online dependency is required by this hourly
workflow.

## Timer definition

Create `/etc/systemd/system/hourly-csv-report.timer`:

```ini
[Unit]
Description=Save local hourly reports five minutes after every hour

[Timer]
OnCalendar=*-*-* *:05:00
Persistent=true
AccuracySec=10s
Unit=hourly-csv-report.service

[Install]
WantedBy=timers.target
```

The five-minute delay allows the producer to finish committing records near the
hour boundary. `Persistent=true` triggers the most recent missed activation after a
restart; it does not create a separate backfill for every hour missed during a long
outage. Use explicit `--date`, `--start`, and `--stop` commands for older backfills.

## Enable and inspect

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now hourly-csv-report.timer
systemctl list-timers hourly-csv-report.timer
```

Run the service immediately. It will select the previous completed hour:

```bash
sudo systemctl start hourly-csv-report.service
```

Inspect logs and status:

```bash
sudo systemctl status hourly-csv-report.service --no-pager
journalctl -u hourly-csv-report.service -n 100 --no-pager
journalctl -u hourly-csv-report.service -f
```

Hourly state files are stored in `outputs/state/hourly`. A state with
`"status": "success"` prevents duplicate local generation for that caster and
window. These state files use the same seven-day retention period as the reports.
