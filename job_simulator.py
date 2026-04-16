"""
DATABASE BACKUP & MAINTENANCE MONITOR
Phase 2: Job Simulator

Simulates database maintenance jobs and logs results
into the job_executions table. Each job type has realistic
behavior — durations, failure rates, error messages, and
storage usage patterns.
"""

import sqlite3
import random
from datetime import datetime, timedelta


# ─────────────────────────────────────────────
# JOB BEHAVIOR PROFILES
# Each job type has its own realistic patterns
# ─────────────────────────────────────────────

JOB_PROFILES = {
    "backup": {
        "base_duration": (900, 1800),       # 15-30 min for normal DBs
        "large_duration": (3600, 5400),     # 60-90 min for large DBs (warehouse)
        "failure_rate": 0.08,               # 8% chance of failure
        "warning_rate": 0.05,               # 5% chance of slow run warning
        "storage_per_gb": 0.5,              # 0.5 MB stored per GB of database
        "errors": [
            "Disk space exceeded on target",
            "Connection timeout to S3 bucket",
            "Authentication failed for backup user",
            "Network interruption during transfer",
            "Target backup directory not found",
        ],
    },
    "index_rebuild": {
        "base_duration": (1800, 3600),      # 30-60 min
        "large_duration": (3600, 7200),     # 60-120 min for large DBs
        "failure_rate": 0.05,
        "warning_rate": 0.03,
        "storage_per_gb": 0,                # doesn't use extra storage
        "errors": [
            "Lock timeout waiting for table access",
            "Insufficient temp tablespace",
            "Deadlock detected during rebuild",
            "Max index size exceeded",
        ],
    },
    "log_cleanup": {
        "base_duration": (60, 300),         # 1-5 min (fast job)
        "large_duration": (300, 600),
        "failure_rate": 0.03,               # rarely fails
        "warning_rate": 0.02,
        "storage_per_gb": -0.025,           # frees up space (negative)
        "errors": [
            "Permission denied on log directory",
            "Log file locked by another process",
        ],
    },
    "statistics": {
        "base_duration": (300, 600),        # 5-10 min
        "large_duration": (600, 1200),
        "failure_rate": 0.06,
        "warning_rate": 0.04,
        "storage_per_gb": 0,
        "errors": [
            "Lock timeout on analytics table",
            "Statistics collection interrupted",
            "Memory allocation failed during analysis",
        ],
    },
    "archival": {
        "base_duration": (1800, 3600),      # 30-60 min
        "large_duration": (3600, 10800),    # up to 3 hours for large DBs
        "failure_rate": 0.10,               # higher failure rate
        "warning_rate": 0.08,
        "storage_per_gb": -0.1,             # frees up significant space
        "errors": [
            "Archive destination unreachable",
            "Integrity check failed on archived data",
            "Timeout waiting for transaction completion",
            "Insufficient space on archive volume",
        ],
    },
}


def simulate_job(job, db_storage_gb):
    """
    Simulate a single job execution.

    Args:
        job: tuple of (job_id, instance_id, job_name, job_type, schedule, is_active)
        db_storage_gb: storage size of the database instance

    Returns:
        dict with execution results
    """
    job_id, instance_id, job_name, job_type, schedule, is_active = job
    profile = JOB_PROFILES.get(job_type, JOB_PROFILES["backup"])

    # Determine if this is a large database (over 1000 GB)
    if db_storage_gb and db_storage_gb > 1000:
        min_dur, max_dur = profile["large_duration"]
    else:
        min_dur, max_dur = profile["base_duration"]

    # Roll the dice: success, failure, or warning?
    roll = random.random()

    if roll < profile["failure_rate"]:
        # FAILED — short duration (fails early), no records processed
        status = "FAILED"
        duration = random.uniform(30, min_dur * 0.3)
        records = random.randint(0, 1000)
        error = random.choice(profile["errors"])
        storage = 0

    elif roll < profile["failure_rate"] + profile["warning_rate"]:
        # WARNING — runs but takes too long
        status = "WARNING"
        duration = random.uniform(max_dur, max_dur * 1.8)
        records = random.randint(50000, 200000)
        error = f"Slow: exceeded expected duration ({max_dur}s threshold)"
        storage = profile["storage_per_gb"] * (db_storage_gb or 100)

    else:
        # SUCCESS — normal run
        status = "SUCCESS"
        duration = random.uniform(min_dur, max_dur)
        records = random.randint(10000, 2500000) if job_type == "backup" else random.randint(20, 100000)
        error = None
        storage = profile["storage_per_gb"] * (db_storage_gb or 100)

    # Add some natural variation to storage
    if storage != 0:
        storage *= random.uniform(0.85, 1.15)

    return {
        "job_id": job_id,
        "status": status,
        "duration_seconds": round(duration, 1),
        "records_processed": records,
        "error_message": error,
        "storage_used_mb": round(storage, 1),
    }


def run_simulation(db_path, run_time=None):
    """
    Run all active jobs once and log results to the database.

    Args:
        db_path: path to monitoring.db
        run_time: datetime for this run (defaults to now)

    Returns:
        list of execution results
    """
    if run_time is None:
        run_time = datetime.now()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all active jobs with their database info
    cursor.execute("""
        SELECT j.job_id, j.instance_id, j.job_name, j.job_type,
               j.schedule, j.is_active, d.storage_gb
        FROM jobs j
        JOIN database_instances d ON j.instance_id = d.instance_id
        WHERE j.is_active = 1
    """)
    jobs_with_storage = cursor.fetchall()

    # Get next execution_id
    cursor.execute("SELECT COALESCE(MAX(execution_id), 0) + 1 FROM job_executions")
    next_id = cursor.fetchone()[0]

    results = []

    for row in jobs_with_storage:
        job = row[:6]       # job fields
        storage_gb = row[6]  # database storage size

        result = simulate_job(job, storage_gb)

        # Calculate timestamps
        started_at = run_time.strftime("%Y-%m-%d %H:%M:%S")
        ended_at = (run_time + timedelta(seconds=result["duration_seconds"])).strftime("%Y-%m-%d %H:%M:%S")

        # Insert into database
        cursor.execute("""
            INSERT INTO job_executions
            (execution_id, job_id, status, started_at, ended_at,
             duration_seconds, records_processed, error_message, storage_used_mb)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            next_id,
            result["job_id"],
            result["status"],
            started_at,
            ended_at,
            result["duration_seconds"],
            result["records_processed"],
            result["error_message"],
            result["storage_used_mb"],
        ))

        result["execution_id"] = next_id
        result["started_at"] = started_at
        results.append(result)
        next_id += 1

    conn.commit()
    conn.close()

    return results


if __name__ == "__main__":
    # Quick test: run one simulation cycle
    results = run_simulation("monitoring.db")
    print(f"Ran {len(results)} jobs:")
    for r in results:
        status_icon = {"SUCCESS": "✓", "FAILED": "✗", "WARNING": "⚠"}.get(r["status"], "?")
        print(f"  {status_icon} Job {r['job_id']}: {r['status']} ({r['duration_seconds']}s)")