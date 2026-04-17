import sqlite3
import random
from datetime import datetime, timedelta


JOB_PROFILES = {
    "backup": {
        "base_duration": (900, 1800),
        "large_duration": (3600, 5400),
        "failure_rate": 0.08,
        "warning_rate": 0.05,
        "storage_per_gb": 0.5,
        "errors": [
            "Disk space exceeded on target",
            "Connection timeout to S3 bucket",
            "Authentication failed for backup user",
            "Network interruption during transfer",
            "Target backup directory not found",
        ],
    },
    "index_rebuild": {
        "base_duration": (1800, 3600),
        "large_duration": (3600, 7200),
        "failure_rate": 0.05,
        "warning_rate": 0.03,
        "storage_per_gb": 0,
        "errors": [
            "Lock timeout waiting for table access",
            "Insufficient temp tablespace",
            "Deadlock detected during rebuild",
            "Max index size exceeded",
        ],
    },
    "log_cleanup": {
        "base_duration": (60, 300),
        "large_duration": (300, 600),
        "failure_rate": 0.03,
        "warning_rate": 0.02,
        "storage_per_gb": -0.025,
        "errors": [
            "Permission denied on log directory",
            "Log file locked by another process",
        ],
    },
    "statistics": {
        "base_duration": (300, 600),
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
        "base_duration": (1800, 3600),
        "large_duration": (3600, 10800),
        "failure_rate": 0.10,
        "warning_rate": 0.08,
        "storage_per_gb": -0.1,
        "errors": [
            "Archive destination unreachable",
            "Integrity check failed on archived data",
            "Timeout waiting for transaction completion",
            "Insufficient space on archive volume",
        ],
    },
}


def simulate_job(job, db_storage_gb):
    job_id, _, _, job_type, _, _ = job
    profile = JOB_PROFILES.get(job_type, JOB_PROFILES["backup"])

    if db_storage_gb and db_storage_gb > 1000:
        min_dur, max_dur = profile["large_duration"]
    else:
        min_dur, max_dur = profile["base_duration"]

    roll = random.random()

    if roll < profile["failure_rate"]:
        status = "FAILED"
        duration = random.uniform(30, min_dur * 0.3)
        records = random.randint(0, 1000)
        error = random.choice(profile["errors"])
        storage = 0

    elif roll < profile["failure_rate"] + profile["warning_rate"]:
        status = "WARNING"
        duration = random.uniform(max_dur, max_dur * 1.8)
        records = random.randint(50000, 200000)
        error = f"Slow: exceeded expected duration ({max_dur}s threshold)"
        storage = profile["storage_per_gb"] * (db_storage_gb or 100)

    else:
        status = "SUCCESS"
        duration = random.uniform(min_dur, max_dur)
        records = random.randint(10000, 2500000) if job_type == "backup" else random.randint(20, 100000)
        error = None
        storage = profile["storage_per_gb"] * (db_storage_gb or 100)

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
    if run_time is None:
        run_time = datetime.now()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT j.job_id, j.instance_id, j.job_name, j.job_type,
               j.schedule, j.is_active, d.storage_gb
        FROM jobs j
        JOIN database_instances d ON j.instance_id = d.instance_id
        WHERE j.is_active = 1
    """)
    jobs_with_storage = cursor.fetchall()

    cursor.execute("SELECT COALESCE(MAX(execution_id), 0) + 1 FROM job_executions")
    next_id = cursor.fetchone()[0]

    results = []

    for row in jobs_with_storage:
        job = row[:6]
        storage_gb = row[6]

        result = simulate_job(job, storage_gb)

        started_at = run_time.strftime("%Y-%m-%d %H:%M:%S")
        ended_at = (run_time + timedelta(seconds=result["duration_seconds"])).strftime("%Y-%m-%d %H:%M:%S")

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
    results = run_simulation("monitoring.db")
    print(f"Ran {len(results)} jobs:")
    for r in results:
        status_icon = {"SUCCESS": "✓", "FAILED": "✗", "WARNING": "⚠"}.get(r["status"], "?")
        print(f"  {status_icon} Job {r['job_id']}: {r['status']} ({r['duration_seconds']}s)")
