# Database Backup & Maintenance Monitor

A monitoring dashboard for database maintenance jobs — backups, index rebuilds, log cleanups, statistics updates, and data archival — across multiple database instances.

**[View Live Dashboard](https://tisha1503.github.io/db-maintenance-monitor/)**

## Quick Start

```bash
python setup.py        # Create database and seed sample data
python run_jobs.py     # Generate 30 days of simulated job history
python dashboard.py    # Build dashboard (outputs index.html)
```

## Architecture

```
Schema (schema.sql) → Simulator (run_jobs.py) → Analytics (analytics.py) → Dashboard (dashboard.py)
```

## Project Structure

```
├── schema.sql           Database schema
├── sample_data.sql      Seed data
├── setup.py             Create and initialize the database
├── job_simulator.py     Job simulation logic with realistic failure profiles
├── run_jobs.py          Generate 30 days of execution history
├── analytics.py         SQL queries for dashboard metrics and alerts
├── dashboard.py         Build interactive HTML dashboard
├── export.py            Export data to CSV
├── templates/
│   └── dashboard.html   Jinja2 template for the dashboard
└── index.html            Generated dashboard (viewable on GitHub Pages)
```

## Database Schema

**database_instances** — Monitored servers (name, engine, environment, storage)

**jobs** — Maintenance jobs per instance (backup, index_rebuild, log_cleanup, statistics, archival)

**job_executions** — Run history with status, duration, records processed, errors, and storage impact

## Simulator Design

Each job type has a behavior profile with duration ranges (scales with database size), failure rates, realistic error messages, and storage patterns (backups grow, cleanups shrink).

Jobs run on schedules: daily (backups, cleanups, stats), weekly (index rebuilds), monthly (archival).

## Dashboard Panels

1. Health Summary — success rate, execution counts, failures, warnings
2. Active Alerts — failed jobs, low success rates, slow executions
3. Success Rate by Job — horizontal bar chart color-coded by threshold
4. Daily Execution Trend — stacked bar chart (success/warning/failure)
5. Job Duration Analysis — min/avg/max duration per job
6. Storage Growth — cumulative storage line chart
7. Failure Causes — doughnut chart grouped by error type
8. Failure Log — detailed table of all failures
9. Storage by Instance — per-database storage breakdown

## Alert Rules

| Rule | Threshold | Severity |
|------|-----------|----------|
| Last run failed | Most recent execution = FAILED | CRITICAL |
| Low success rate | Below 80% | WARNING |
| Slow execution | Last run > 2x average duration | WARNING |

## AWS QuickSight Migration

The analytics layer produces data structures compatible with QuickSight datasets. To migrate:

1. Upload `monitoring.db` to S3 or migrate to RDS
2. Create a QuickSight dataset connected to the database
3. Use the SQL queries from `analytics.py` as custom SQL datasets
4. Build analyses matching the dashboard chart types

## Technologies

- Python 3 — simulation, analytics, dashboard generation
- SQLite — portable embedded database
- Chart.js — interactive visualizations
- Jinja2 — HTML templating