# Hourly Raw and Verified CSV Systemd Job

## Purpose

This job emails two reports for every completed clock hour:

1. Raw pipe CSVs to `email.recipients`.
2. Verified pipe CSVs to `verified_pipe_records_recipients`.

It does not run diagnosis XLSX generation, videos, Google Drive uploads, or history
cleanup. The existing shift workflow and its systemd jobs remain unchanged.

The verified shift-summary table is also saved as a PNG under
`outputs/hourly-report-images`, including when the command uses `--no-email`.

The systemd command omits an explicit date and time. The CLI then selects the
previous completed hour. For example, a run at `02:05` reports `01:00:00` through
`02:00:00`:

```bash
uv run python -m cli.hourly_csv_report --all-casters
```

The stop boundary is excluded, so a record at exactly `02:00:00` belongs only to
the next report.

## Test before enabling mail

Generate both CSVs without sending mail:

```bash
uv run python -m cli.hourly_csv_report --date 08-08-2026 --start 01:00 --stop 02:00 --caster caster3 --no-email
```

Send both emails only to `email.test_recipients`:

```bash
uv run python -m cli.hourly_csv_report --date 08-08-2026 --start 01:00 --stop 02:00 --caster caster3 --test
```

Run the production mail for the same window:

```bash
uv run python -m cli.hourly_csv_report --date 08-08-2026 --start 01:00 --stop 02:00 --caster caster3
```

If that window was already sent successfully, it is skipped. Add `--force` only
when you intentionally need to regenerate and resend it.

## Service definition

Create `/etc/systemd/system/hourly-csv-report.service`:

```ini
[Unit]
Description=Hourly raw and verified pipe CSV report
Wants=network-online.target
After=network-online.target

[Service]
Type=oneshot
User=voptimaise-jetson-2
WorkingDirectory=/mnt/ssd/home/voptimaise-jetson-2/dev/reports-generator
Environment=PYTHONUNBUFFERED=1
EnvironmentFile=/etc/reports-generator/hourly-csv-report.env
ExecStart=/mnt/ssd/home/voptimaise-jetson-2/.local/bin/uv run python -m cli.hourly_csv_report --all-casters
StandardOutput=journal
StandardError=journal
TimeoutStartSec=45min
```

## Timer definition

Create `/etc/systemd/system/hourly-csv-report.timer`:

```ini
[Unit]
Description=Run raw and verified CSV mail five minutes after every hour

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

## Email environment

Create `/etc/reports-generator/hourly-csv-report.env` with the password named by
`email.password_env` in `config/runtime.yaml`:

```ini
EMAIL_APP_PASSWORD="GMAIL_APP_PASSWORD"
```

Protect it and never commit the real password:

```bash
sudo chown root:root /etc/reports-generator/hourly-csv-report.env
sudo chmod 600 /etc/reports-generator/hourly-csv-report.env
```

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
`"status": "success"` prevents duplicate mail for that caster and window.
