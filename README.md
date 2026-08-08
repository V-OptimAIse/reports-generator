# Reports Generator

This project generates shift-wise production reports for Electro Steel pipe detection.
It reads pipe records from each caster SQLite database, reads saved history images and
YOLO text files, creates CSV/XLSX/video reports, sends configured emails, uploads
selected files to Google Drive, and cleans up source history files after successful
full-shift video creation.

## Main Entry Point

Use the current workflow entrypoint:

```bash
uv run python -m cli.report_workflow
```

With no date/shift arguments, the workflow runs only during the scheduled trigger
window:

- `06:00` runs previous day `Shift_C`
- `14:00` runs same day `Shift_A`
- `22:00` runs same day `Shift_B`

The trigger window has a 5 minute slack.

To run a specific shift manually:

```bash
uv run python -m cli.report_workflow --date 13-07-2026 --shift C --caster caster2
```

Useful options:

```bash
uv run python -m cli.report_workflow --validate-config
uv run python -m cli.report_workflow --date 13-07-2026 --shift C --all-casters
uv run python -m cli.report_workflow --date 13-07-2026 --shift C --casters caster2,caster3
uv run python -m cli.report_workflow --date 13-07-2026 --shift C --verified-only
uv run python -m cli.report_workflow --date 13-07-2026 --shift C --diagnosis-only
uv run python -m cli.report_workflow --date 13-07-2026 --shift C --test
```

`--test` sends workflow emails only to `email.test_recipients`.

## Hourly Raw and Verified CSV Reports

The hourly CSV workflow is separate from the shift workflow. It creates and emails
only the raw CSV and verified CSV; it does not create XLSX/video files, upload to
Drive, or delete history sources.

Run a specific window for one caster:

```bash
uv run python -m cli.hourly_csv_report --date 08-08-2026 --start 01:00 --stop 02:00 --caster caster3
```

Run a specific window for all enabled casters:

```bash
uv run python -m cli.hourly_csv_report --date 08-08-2026 --start 01:00 --stop 02:00 --all-casters
```

When `--date`, `--start`, and `--stop` are omitted, the command automatically uses
the previous completed clock hour. This is the recommended systemd command:

```bash
uv run python -m cli.hourly_csv_report --all-casters
```

Useful safety and retry options:

```bash
uv run python -m cli.hourly_csv_report --date 08-08-2026 --start 01:00 --stop 02:00 --caster caster3 --no-email
uv run python -m cli.hourly_csv_report --date 08-08-2026 --start 01:00 --stop 02:00 --caster caster3 --test
uv run python -m cli.hourly_csv_report --date 08-08-2026 --start 01:00 --stop 02:00 --caster caster3 --force
```

Hourly windows are half-open: `01:00 <= event time < 02:00`. This prevents the
02:00 record from appearing in both the 01:00-02:00 and 02:00-03:00 reports.
Successful windows are recorded under `outputs/state/hourly`, so a repeated systemd
activation does not resend the same report. Use `--force` only when an intentional
resend is required.

Raw hourly mail uses `email.recipients`; verified hourly mail uses
`verified_pipe_records_recipients`. Optional `hourly_csv_report.raw_recipients` and
`hourly_csv_report.verified_recipients` values can override those lists without
changing shift-report recipients.

## What The Full Workflow Does

For each enabled or selected caster, the workflow runs these phases in order.

1. Loads `config/runtime.yaml`.
2. Resolves caster-specific paths and settings from `casters.defaults` and `casters.items`.
3. Creates or updates a state file in `outputs/state`.
4. Exports the raw pipe CSV from the caster SQLite database.
5. Emails the raw CSV if `email.send_csv_attachment` is enabled.
6. Creates the verified pipe CSV using checkpoint, trolley/Gate 2 intersection,
   then Gate 2 opening as the fallback sequence for rows requiring verification.
7. Emails the verified pipe CSV to `verified_pipe_records_recipients`.
8. Uploads the raw CSV to Google Drive through `rclone`.
9. Deletes the local raw CSV after successful Drive upload.
10. Creates the diagnosis XLSX.
11. Builds missing-loadcell video windows from verified-pipe results.
12. Generates missing-loadcell overlay and normal videos.
13. Uploads missing-loadcell videos to Google Drive.
14. Deletes missing-loadcell videos after upload if configured.
15. Sends the diagnosis email with the XLSX and missing-loadcell links.
16. Generates the normal full-shift video locally.
17. Only after successful full-shift video creation, deletes the source `Shift_*_img` and `Shift_*_text` folders for that shift.
18. Removes empty date folders, such as the old date folder after `Shift_C` completes.
19. Marks the caster state as `success` or `partial_failure`.

Important cleanup behavior:

- If full-shift video creation fails, history images and text files are not deleted.
- If OpenCV writes zero readable frames, video creation is treated as failed.
- Cleanup deletes whole shift source folders with `shutil.rmtree`, not image-by-image or text-file-by-text-file.
- For `Shift_C`, cleanup may touch two date folders because the shift crosses midnight.
- Date folders such as `history/2026_07_13` are removed only when they become empty.
- Cleanup logs one message per folder plus a summary, so production logs stay compact.

Example source folders:

```text
../electrosteel_pipe_detection_prod/var/caster_2/history/2026_07_13/Shift_C_img
../electrosteel_pipe_detection_prod/var/caster_2/history/2026_07_13/Shift_C_text
../electrosteel_pipe_detection_prod/var/caster_2/history/2026_07_14/Shift_C_img
../electrosteel_pipe_detection_prod/var/caster_2/history/2026_07_14/Shift_C_text
```

## Configuration

Main configuration lives in:

```text
config/runtime.yaml
```

Key sections:

- `history.shifts`: shift start/end times.
- `casters.defaults`: shared caster templates for DB, history, output, and Drive paths.
- `casters.items`: caster list, numbers, enabled flags, var dirs, and ROI files.
- `database.path`: resolved per caster to the SQLite pipe database.
- `history.image_root`: resolved per caster to the image/text history root.
- `rois`: ROI YAML path and coordinate source resolution.
- `outputs.csv_dir`: local CSV/XLSX output directory.
- `video`: FPS, codec, resolution, full-shift video dir, overlay video dir.
- `missing_loadcell_video`: missing-loadcell clip settings and delete-after-upload.
- `diagnosis`: t-origin gap thresholds for abnormal-row reporting.
- `gdrive`: rclone remote and Drive folder names.
- `email`: SMTP sender, recipients, test recipients, and password env var.
- `video_retention`: retention settings for generated videos.
- `jetson_storage_alert`: disk usage alert settings.

The current config supports multiple casters. Enabled casters are resolved by
`reports/common/caster_config.py`.

## Inputs

The workflow expects these production inputs:

- SQLite pipe database per caster, usually under the caster `var_dir`.
- History images under `history/YYYY_MM_DD/Shift_X_img`.
- YOLO text files under `history/YYYY_MM_DD/Shift_X_text`.
- ROI YAML files for overlay and gate diagnostics.
- `rclone` configured for the Google Drive remote in `gdrive.remote`.
- SMTP credentials, preferably through `email.password_env`.

## Outputs

Typical generated outputs:

- Raw pipe CSVs in `outputs/{caster_id}/csv`.
- Verified pipe CSVs in `outputs/{caster_id}/csv`.
- Diagnosis XLSX files in `outputs/{caster_id}/csv`.
- Full-shift videos in `outputs/{caster_id}/videos`.
- Missing-loadcell overlay videos in `outputs/{caster_id}/videos-overlay`.
- Workflow state files in `outputs/state`.
- Uploaded CSV/video Drive links stored in the state JSON.

## Code Map

```text
cli/report_workflow.py
```

Main multi-caster workflow. Handles scheduled/manual runs, state, emails, uploads,
diagnosis, missing-loadcell videos, full-shift videos, and source cleanup.

```text
cli/hourly_csv_report.py
```

Generates and emails only raw and verified CSVs for an explicit time window or the
previous completed hour. Its state and email flow are isolated from shift reports.

```text
cli/generate_report.py
```

Older compatibility entrypoint. Prefer `cli/report_workflow.py` for current work.

```text
reports/common/config_loader.py
```

Loads `config/runtime.yaml`.

```text
reports/common/caster_config.py
```

Builds per-caster runtime configs from shared defaults and caster items.

```text
reports/common/email_sender.py
```

Sends SMTP emails with optional attachments.

```text
reports/common/gdrive_uploader.py
```

Uploads CSV/video files to Google Drive through `rclone`.

```text
reports/pipes/pipe_exporter.py
```

Reads the SQLite `pipes` table and exports raw CSVs and diagnosis XLSX files.

```text
reports/pipes/verified_pipes.py
```

Creates client-facing verified pipe CSVs. It can verify only loadcell-missing
rows or all rows, depending on `verified_pipes_mode`. A checked row is confirmed
first by `pipe_checkpoint=1`, then by a `pipe_on_trolley=1` event from
`trolley_gate2_intersections`, and finally by a Gate 2 opening in the pipe's
verification window.

```text
reports/pipes/gate_cycles_exporter.py
```

Exports gate opening rows for a date/shift, useful for debugging verification.

```text
reports/video/video_generator.py
```

Creates the normal full-shift video from history images.

```text
reports/video/video_overlay.py
```

Creates overlay videos from history images, YOLO text files, and ROI data.

```text
reports/video/source_cleanup.py
```

Deletes source image/text files after successful full-shift video creation and
prunes empty shift/date folders.

```text
reports/video/delete_old_videos.py
```

Deletes generated `.mp4` files older than `video_retention.keep_days`.

```text
reports/gates/gate2_closed_position_report.py
```

Checks Gate2 closed-position coverage from YOLO text files and ROI data.

```text
reports/check_jetson_storage.py
```

Sends an email alert when the configured Jetson disk usage crosses a threshold.

```text
tests/
```

Unit tests for workflow ordering, multi-caster config, video overlay behavior,
verified pipe filtering, source cleanup, old-video deletion, and gate diagnostics.

## Helper Commands

Generate a full-shift video only:

```bash
uv run python -m reports.video.video_generator --date 13-07-2026 --shift C --caster caster2
```

Generate an overlay video only:

```bash
uv run python -m reports.video.video_overlay --date 13-07-2026 --shift C
```

Delete generated videos older than configured retention:

```bash
uv run python -m reports.video.delete_old_videos
uv run python -m reports.video.delete_old_videos --dry-run
```

Export verified pipes directly:

```bash
uv run python -m reports.pipes.verified_pipes --date 13-07-2026 --shift C
```

Export gate cycles for debugging:

```bash
uv run python -m reports.pipes.gate_cycles_exporter --date 13-07-2026 --shift C --caster caster2
```

Check Jetson storage:

```bash
uv run python -m reports.check_jetson_storage
```

Run tests:

```bash
uv run pytest
```

## Development Notes

- Keep cleanup logic separate from video generation. The workflow decides when
  cleanup is safe to run.
- Do not delete history source folders before `ShiftVideoGenerator.generate()`
  returns successfully.
- Prefer caster-specific config through `resolve_enabled_casters`.
- Use the state JSON files in `outputs/state` to debug what happened during a run.
- Use `--test` before changing email behavior in production.

Systemd maintenance for the 10-minute Gate 2 closed-position alert job is documented in
[`docs/gate2-closed-position-systemd.md`](docs/gate2-closed-position-systemd.md).

Systemd setup for hourly raw and verified CSV mail is documented in
[`docs/hourly-csv-report-systemd.md`](docs/hourly-csv-report-systemd.md).
