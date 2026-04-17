import sqlite3
from datetime import datetime, timedelta
from job_simulator import simulate_job

DB_PATH = "monitoring.db"
DAYS_TO_SIMULATE = 30

JOB_SCHEDULES = [
    (1, 2, "daily"),
    (2, 3, "weekly"),
    (3, 4, "daily"),
    (4, 2, "daily"),
    (5, 1, "daily"),
    (6, 5, "daily"),
    (7, 0, "monthly"),
    (8, 6, "weekly"),
]


def should_run(job_schedule, current_date):
    job_id, _, frequency = job_schedule

    if frequency == "daily":
        return True
    elif frequency == "weekly":
        if job_id == 2:
            return current_date.weekday() == 6
        elif job_id == 8:
            return current_date.weekday() == 0
    elif frequency == "monthly":
        return current_date.day == 1

    return False


def generate_history(db_path, days):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM job_executions")
    conn.commit()

    cursor.execute("""
        SELECT j.job_id, j.instance_id, j.job_name, j.job_type,
               j.schedule, j.is_active, d.storage_gb
        FROM jobs j
        JOIN database_instances d ON j.instance_id = d.instance_id
        WHERE j.is_active = 1
    """)
    jobs_with_storage = {row[0]: row for row in cursor.fetchall()}

    start_date = datetime.now() - timedelta(days=days)
    total_executions = 0
    status_counts = {"SUCCESS": 0, "FAILED": 0, "WARNING": 0}

    print(f"Generating {days} days of job history...")
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {datetime.now().strftime('%Y-%m-%d')}")
    print()

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

            result = simulate_job(job, storage_gb)

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

            status_counts[result["status"]] += 1
            execution_id += 1
            day_jobs += 1
            total_executions += 1

        if day_offset % 7 == 0:
            print(f"  Week {day_offset // 7 + 1}: {current_date.strftime('%Y-%m-%d')} — {day_jobs} jobs today")

    conn.commit()
    conn.close()

    print()
    print("=" * 45)
    print("  GENERATION COMPLETE")
    print("=" * 45)
    print(f"  Total executions: {total_executions}")
    print(f"  Successes:        {status_counts['SUCCESS']}")
    print(f"  Failures:         {status_counts['FAILED']}")
    print(f"  Warnings:         {status_counts['WARNING']}")
    print()
    success_rate = 100 * status_counts["SUCCESS"] / max(total_executions, 1)
    print(f"  Overall success rate: {success_rate:.1f}%")
    print(f"  Data saved to: {db_path}")


if __name__ == "__main__":
    generate_history(DB_PATH, DAYS_TO_SIMULATE)