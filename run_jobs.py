"""
DATABASE BACKUP & MAINTENANCE MONITOR
Phase 2: Generate Historical Data

Simulates 30 days of job executions to populate the database
with enough data for meaningful dashboard visualizations.

Each job runs on its own schedule:
  - Daily jobs: backup, log_cleanup, statistics
  - Weekly jobs: index_rebuild, staging_backup
  - Monthly jobs: data_archival

Run: python run_jobs.py
"""

import sqlite3
from datetime import datetime, timedelta
from job_simulator import run_simulation

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

DB_PATH = "monitoring.db"
DAYS_TO_SIMULATE = 30

# Map each job to its scheduled hour and frequency
# (job_id, hour, frequency)
JOB_SCHEDULES = [
    (1, 2, "daily"),       # nightly_backup — 2 AM every day
    (2, 3, "weekly"),      # index_rebuild — 3 AM on Sundays
    (3, 4, "daily"),       # log_cleanup — 4 AM every day
    (4, 2, "daily"),       # replica_backup — 2 AM every day
    (5, 1, "daily"),       # warehouse_backup — 1 AM every day
    (6, 5, "daily"),       # stats_update — 5 AM every day
    (7, 0, "monthly"),     # data_archival — midnight on 1st of month
    (8, 6, "weekly"),      # staging_backup — 6 AM on Mondays
]


def should_run(job_schedule, current_date):
    """Check if a job should run on the given date based on its frequency."""
    job_id, hour, frequency = job_schedule

    if frequency == "daily":
        return True
    elif frequency == "weekly":
        # index_rebuild on Sunday (6), staging_backup on Monday (0)
        if job_id == 2:
            return current_date.weekday() == 6  # Sunday
        elif job_id == 8:
            return current_date.weekday() == 0  # Monday
    elif frequency == "monthly":
        return current_date.day == 1

    return False


def generate_history(db_path, days):
    """Generate execution history for the specified number of days."""

    # Clear existing execution data so we start fresh
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM job_executions")
    conn.commit()
    conn.close()

    # Get job and database info
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT j.job_id, j.instance_id, j.job_name, j.job_type,
               j.schedule, j.is_active, d.storage_gb
        FROM jobs j
        JOIN database_instances d ON j.instance_id = d.instance_id
        WHERE j.is_active = 1
    """)
    jobs_with_storage = {row[0]: row for row in cursor.fetchall()}
    conn.close()

    start_date = datetime.now() - timedelta(days=days)
    total_executions = 0
    status_counts = {"SUCCESS": 0, "FAILED": 0, "WARNING": 0}

    print(f"Generating {days} days of job history...")
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {datetime.now().strftime('%Y-%m-%d')}")
    print()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    execution_id = 1

    for day_offset in range(days):
        current_date = start_date + timedelta(days=day_offset)
        day_jobs = 0

        for schedule in JOB_SCHEDULES:
            job_id, hour, frequency = schedule

            if not should_run(schedule, current_date):
                continue

            if job_id not in jobs_with_storage:
                continue

            row = jobs_with_storage[job_id]
            job = row[:6]
            storage_gb = row[6]

            # Import and run simulation for this single job
            from job_simulator import simulate_job
            result = simulate_job(job, storage_gb)

            # Set the run time to the scheduled hour
            run_time = current_date.replace(hour=hour, minute=0, second=0)
            started_at = run_time.strftime("%Y-%m-%d %H:%M:%S")
            ended_at = (run_time + timedelta(seconds=result["duration_seconds"])).strftime("%Y-%m-%d %H:%M:%S")

            cursor.execute("""
                INSERT INTO job_executions
                (execution_id, job_id, status, started_at, ended_at,
                 duration_seconds, records_processed, error_message, storage_used_mb)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                execution_id,
                result["job_id"],
                result["status"],
                started_at,
                ended_at,
                result["duration_seconds"],
                result["records_processed"],
                result["error_message"],
                result["storage_used_mb"],
            ))

            status_counts[result["status"]] = status_counts.get(result["status"], 0) + 1
            execution_id += 1
            day_jobs += 1
            total_executions += 1

        if day_offset % 7 == 0:
            print(f"  Week {day_offset // 7 + 1}: {current_date.strftime('%Y-%m-%d')} — {day_jobs} jobs today")

    conn.commit()
    conn.close()

    # Print summary
    print()
    print("=" * 45)
    print("  GENERATION COMPLETE")
    print("=" * 45)
    print(f"  Total executions: {total_executions}")
    print(f"  Successes:        {status_counts.get('SUCCESS', 0)}")
    print(f"  Failures:         {status_counts.get('FAILED', 0)}")
    print(f"  Warnings:         {status_counts.get('WARNING', 0)}")
    print()
    success_rate = 100 * status_counts.get("SUCCESS", 0) / max(total_executions, 1)
    print(f"  Overall success rate: {success_rate:.1f}%")
    print(f"  Data saved to: {db_path}")


if __name__ == "__main__":
    generate_history(DB_PATH, DAYS_TO_SIMULATE)