# Gate 2 Closed-Position Systemd Job

## Purpose

This job checks the most recent 10 minutes of Gate 2 closed-position detections for every enabled caster. A Gate 2 detection is counted as closed only when its centroid is inside `roi_gate2_closed` and outside `roi_gate2_open`. ROI coverage is measured only for those closed-position detections; other Gate 2 detections are excluded from the average.

An alert email is sent when a caster's average closed-position detection-inside-ROI percentage is below `gate2_closed_position_report.min_avg_coverage_percent` in `config/runtime.yaml`. The current threshold is 70%. An interval with no closed-position detections does not have a coverage value and does not send a low-coverage alert. With `alert_on_no_samples: false`, an interval containing no YOLO samples also does not send an alert.

The command executed by the job is:

```bash
uv run python -m reports.gates.gate2_closed_position_report --all-casters --last-minutes 10
```

Do not add `--no-email` to the systemd command, because that disables alert emails.

## Production paths

| Item | Path |
|---|---|
| Project | `/mnt/ssd/home/voptimaise-jetson-2/dev/reports-generator` |
| `uv` executable | `/mnt/ssd/home/voptimaise-jetson-2/.local/bin/uv` |
| Service | `/etc/systemd/system/gate2-closed-position-report.service` |
| Timer | `/etc/systemd/system/gate2-closed-position-report.timer` |
| Email environment file | `/etc/reports-generator/gate2-report.env` |
| Runtime configuration | `config/runtime.yaml` |

## Service definition

`/etc/systemd/system/gate2-closed-position-report.service`:

```ini
[Unit]
Description=Gate2 closed-position report for all enabled casters
Wants=network-online.target
After=network-online.target

[Service]
Type=oneshot
User=voptimaise-jetson-2
WorkingDirectory=/mnt/ssd/home/voptimaise-jetson-2/dev/reports-generator
Environment=PYTHONUNBUFFERED=1
EnvironmentFile=/etc/reports-generator/gate2-report.env
ExecStart=/mnt/ssd/home/voptimaise-jetson-2/.local/bin/uv run python -m reports.gates.gate2_closed_position_report --all-casters --last-minutes 10
StandardOutput=journal
StandardError=journal
```

## Timer definition

`/etc/systemd/system/gate2-closed-position-report.timer`:

```ini
[Unit]
Description=Run Gate2 closed-position report every 10 minutes

[Timer]
OnCalendar=*-*-* *:00/10:00
Persistent=true
AccuracySec=10s
Unit=gate2-closed-position-report.service

[Install]
WantedBy=timers.target
```

`Persistent=true` causes one catch-up run after startup if a scheduled run was missed while the machine was powered off.

## Email environment

`/etc/reports-generator/gate2-report.env` contains the password referenced by `email.password_env` in `config/runtime.yaml`:

```ini
EMAIL_APP_PASSWORD="GMAIL_APP_PASSWORD"
```

Protect this file and never commit its real value:

```bash
sudo chown root:root /etc/reports-generator/gate2-report.env
sudo chmod 600 /etc/reports-generator/gate2-report.env
```

Alert recipients come from `gate2_closed_position_report.recipients` when configured; otherwise the report uses `email.diagnosis_recipients`.

## Maintenance commands

After changing the service or timer:

```bash
sudo systemctl daemon-reload
sudo systemctl restart gate2-closed-position-report.timer
```

Enable the timer at boot:

```bash
sudo systemctl enable --now gate2-closed-position-report.timer
```

Run an immediate check:

```bash
sudo systemctl start gate2-closed-position-report.service
```

Inspect status, schedule, and logs:

```bash
sudo systemctl status gate2-closed-position-report.service --no-pager
systemctl list-timers gate2-closed-position-report.timer
journalctl -u gate2-closed-position-report.service -n 100 --no-pager
```

Follow logs continuously:

```bash
journalctl -fu gate2-closed-position-report.service
```

Stop and disable scheduling:

```bash
sudo systemctl disable --now gate2-closed-position-report.timer
```

## Troubleshooting

- `status=203/EXEC`: the executable or project path is wrong. Verify with `pwd -P` and `readlink -f "$(command -v uv)"`, then update `WorkingDirectory` and `ExecStart`.
- Missing email password: confirm `/etc/reports-generator/gate2-report.env` exists and defines `EMAIL_APP_PASSWORD`.
- No alert email: confirm the result is below the threshold, `send_email: true`, recipients are configured, and `--no-email` is absent.
- A caster is not checked: only casters with `enabled: true` under `casters.items` in `config/runtime.yaml` are processed.
- ROI mismatch: verify the caster-specific `rois.path` and `rois.source_resolution`. The ROI source resolution must describe the coordinate space in which that ROI YAML was created, not merely the saved image size.

After configuration changes, first run the report manually with `--no-email`, confirm ROI scaling and summary values, and then test the systemd service.
