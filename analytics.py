"""
DATABASE BACKUP & MAINTENANCE MONITOR
Phase 3: Analytics & Metrics Queries

SQL queries that power the dashboard. Each function returns
data for a specific dashboard panel — success rates, failure
trends, duration analysis, storage growth, and alerts.

These same queries will be used in Phase 4 with QuickSight
or can be connected to any visualization tool.
"""

import sqlite3
from datetime import datetime, timedelta


def get_connection(db_path="monitoring.db"):
    """Get a database connection with row factory for dict-like access."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# ─────────────────────────────────────────────
# DASHBOARD PANEL 1: Overall Health Summary
# The top-level "scoreboard" — is everything OK?
# ─────────────────────────────────────────────

def get_health_summary(db_path="monitoring.db"):
    """
    Returns high-level stats for the dashboard header:
    total jobs, total executions, overall success rate,
    and counts by status.
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            COUNT(DISTINCT e.job_id)                                     AS active_jobs,
            COUNT(*)                                                     AS total_executions,
            SUM(CASE WHEN e.status = 'SUCCESS' THEN 1 ELSE 0 END)       AS successes,
            SUM(CASE WHEN e.status = 'FAILED'  THEN 1 ELSE 0 END)       AS failures,
            SUM(CASE WHEN e.status = 'WARNING' THEN 1 ELSE 0 END)       AS warnings,
            ROUND(100.0 * SUM(CASE WHEN e.status = 'SUCCESS' THEN 1 ELSE 0 END)
                / COUNT(*), 1)                                           AS success_rate_pct
        FROM job_executions e
    """)

    row = cursor.fetchone()
    conn.close()
    return dict(row)


# ─────────────────────────────────────────────
# DASHBOARD PANEL 2: Success Rate Per Job
# Which jobs are reliable? Which need attention?
# ─────────────────────────────────────────────

def get_success_rates_by_job(db_path="monitoring.db"):
    """
    Success rate for each job, with database and job name.
    Sorted worst-performing first so problems are visible.
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            d.instance_name,
            j.job_name,
            j.job_type,
            COUNT(*)                                                     AS total_runs,
            SUM(CASE WHEN e.status = 'SUCCESS' THEN 1 ELSE 0 END)       AS successes,
            SUM(CASE WHEN e.status = 'FAILED'  THEN 1 ELSE 0 END)       AS failures,
            ROUND(100.0 * SUM(CASE WHEN e.status = 'SUCCESS' THEN 1 ELSE 0 END)
                / COUNT(*), 1)                                           AS success_rate_pct
        FROM job_executions e
        JOIN jobs j ON e.job_id = j.job_id
        JOIN database_instances d ON j.instance_id = d.instance_id
        GROUP BY j.job_id
        ORDER BY success_rate_pct ASC
    """)

    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


# ─────────────────────────────────────────────
# DASHBOARD PANEL 3: Daily Status Trend
# How has job health changed over time?
# ─────────────────────────────────────────────

def get_daily_status_trend(db_path="monitoring.db"):
    """
    Daily counts of SUCCESS, FAILED, WARNING over time.
    Powers a stacked bar chart or line chart on the dashboard.
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            DATE(started_at)                                             AS run_date,
            COUNT(*)                                                     AS total,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END)         AS successes,
            SUM(CASE WHEN status = 'FAILED'  THEN 1 ELSE 0 END)         AS failures,
            SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END)         AS warnings,
            ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END)
                / COUNT(*), 1)                                           AS daily_success_rate
        FROM job_executions
        GROUP BY DATE(started_at)
        ORDER BY run_date ASC
    """)

    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


# ─────────────────────────────────────────────
# DASHBOARD PANEL 4: Job Duration Analysis
# Are jobs getting slower over time?
# ─────────────────────────────────────────────

def get_duration_stats(db_path="monitoring.db"):
    """
    Min, avg, max duration for each job.
    Helps identify jobs that are slowing down or inconsistent.
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            d.instance_name,
            j.job_name,
            j.job_type,
            COUNT(*)                                    AS total_runs,
            ROUND(MIN(e.duration_seconds), 1)           AS min_duration,
            ROUND(AVG(e.duration_seconds), 1)           AS avg_duration,
            ROUND(MAX(e.duration_seconds), 1)           AS max_duration,
            ROUND(MAX(e.duration_seconds) - MIN(e.duration_seconds), 1) AS duration_spread
        FROM job_executions e
        JOIN jobs j ON e.job_id = j.job_id
        JOIN database_instances d ON j.instance_id = d.instance_id
        WHERE e.status != 'FAILED'
        GROUP BY j.job_id
        ORDER BY avg_duration DESC
    """)

    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def get_duration_trend(db_path="monitoring.db"):
    """
    Daily average duration per job type over time.
    Powers a line chart showing performance trends.
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            DATE(e.started_at)                          AS run_date,
            j.job_type,
            ROUND(AVG(e.duration_seconds), 1)           AS avg_duration
        FROM job_executions e
        JOIN jobs j ON e.job_id = j.job_id
        WHERE e.status != 'FAILED'
        GROUP BY DATE(e.started_at), j.job_type
        ORDER BY run_date ASC, j.job_type
    """)

    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


# ─────────────────────────────────────────────
# DASHBOARD PANEL 5: Storage Growth
# How much storage are backups consuming?
# ─────────────────────────────────────────────

def get_storage_trend(db_path="monitoring.db"):
    """
    Cumulative storage usage over time.
    Shows how backup storage is growing day by day.
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            DATE(e.started_at)                          AS run_date,
            ROUND(SUM(e.storage_used_mb), 1)            AS daily_storage_mb,
            ROUND(SUM(SUM(e.storage_used_mb)) OVER (ORDER BY DATE(e.started_at)), 1)
                                                        AS cumulative_storage_mb
        FROM job_executions e
        WHERE e.status != 'FAILED'
        GROUP BY DATE(e.started_at)
        ORDER BY run_date ASC
    """)

    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def get_storage_by_instance(db_path="monitoring.db"):
    """
    Total storage consumed per database instance.
    Shows which databases use the most backup storage.
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            d.instance_name,
            d.db_engine,
            d.storage_gb                                AS allocated_gb,
            ROUND(SUM(e.storage_used_mb), 1)            AS total_storage_used_mb,
            ROUND(SUM(e.storage_used_mb) / 1024.0, 2)   AS total_storage_used_gb
        FROM job_executions e
        JOIN jobs j ON e.job_id = j.job_id
        JOIN database_instances d ON j.instance_id = d.instance_id
        WHERE e.status != 'FAILED'
        GROUP BY d.instance_id
        ORDER BY total_storage_used_mb DESC
    """)

    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


# ─────────────────────────────────────────────
# DASHBOARD PANEL 6: Failure Analysis
# What's failing and why?
# ─────────────────────────────────────────────

def get_failure_details(db_path="monitoring.db"):
    """
    All failed executions with job name, database, and error message.
    Powers the failure log / detail table.
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            e.started_at,
            d.instance_name,
            j.job_name,
            j.job_type,
            e.error_message,
            e.duration_seconds
        FROM job_executions e
        JOIN jobs j ON e.job_id = j.job_id
        JOIN database_instances d ON j.instance_id = d.instance_id
        WHERE e.status = 'FAILED'
        ORDER BY e.started_at DESC
    """)

    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def get_failure_counts_by_error(db_path="monitoring.db"):
    """
    Group failures by error message to find the most common causes.
    Powers a "top failure reasons" chart.
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            e.error_message,
            COUNT(*)                                    AS occurrences,
            GROUP_CONCAT(DISTINCT j.job_name)           AS affected_jobs
        FROM job_executions e
        JOIN jobs j ON e.job_id = j.job_id
        WHERE e.status = 'FAILED'
        GROUP BY e.error_message
        ORDER BY occurrences DESC
    """)

    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


# ─────────────────────────────────────────────
# DASHBOARD PANEL 7: Alerts
# What needs immediate attention?
# ─────────────────────────────────────────────

def get_active_alerts(db_path="monitoring.db"):
    """
    Check for conditions that should trigger alerts:
    - Jobs that failed in their last run
    - Jobs with success rate below 80%
    - Jobs whose last run was significantly slower than average
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()
    alerts = []

    # Alert 1: Jobs where the most recent run failed
    cursor.execute("""
        SELECT
            d.instance_name,
            j.job_name,
            e.error_message,
            e.started_at
        FROM job_executions e
        JOIN jobs j ON e.job_id = j.job_id
        JOIN database_instances d ON j.instance_id = d.instance_id
        WHERE e.execution_id IN (
            SELECT execution_id FROM job_executions e2
            WHERE e2.job_id = e.job_id
            ORDER BY e2.started_at DESC
            LIMIT 1
        )
        AND e.status = 'FAILED'
    """)

    for row in cursor.fetchall():
        alerts.append({
            "severity": "CRITICAL",
            "type": "last_run_failed",
            "instance": row["instance_name"],
            "job": row["job_name"],
            "message": f"Last run failed: {row['error_message']}",
            "timestamp": row["started_at"],
        })

    # Alert 2: Jobs with success rate below 80%
    cursor.execute("""
        SELECT
            d.instance_name,
            j.job_name,
            ROUND(100.0 * SUM(CASE WHEN e.status = 'SUCCESS' THEN 1 ELSE 0 END)
                / COUNT(*), 1) AS success_rate
        FROM job_executions e
        JOIN jobs j ON e.job_id = j.job_id
        JOIN database_instances d ON j.instance_id = d.instance_id
        GROUP BY j.job_id
        HAVING success_rate < 80
    """)

    for row in cursor.fetchall():
        alerts.append({
            "severity": "WARNING",
            "type": "low_success_rate",
            "instance": row["instance_name"],
            "job": row["job_name"],
            "message": f"Success rate is {row['success_rate']}% (below 80% threshold)",
            "timestamp": None,
        })

    # Alert 3: Last run took more than 2x the average duration
    cursor.execute("""
        SELECT
            d.instance_name,
            j.job_name,
            latest.duration_seconds AS last_duration,
            ROUND(avg_stats.avg_dur, 1) AS avg_duration
        FROM jobs j
        JOIN database_instances d ON j.instance_id = d.instance_id
        JOIN (
            SELECT job_id, duration_seconds, started_at
            FROM job_executions
            WHERE (job_id, started_at) IN (
                SELECT job_id, MAX(started_at)
                FROM job_executions
                GROUP BY job_id
            )
        ) latest ON j.job_id = latest.job_id
        JOIN (
            SELECT job_id, AVG(duration_seconds) AS avg_dur
            FROM job_executions
            WHERE status = 'SUCCESS'
            GROUP BY job_id
        ) avg_stats ON j.job_id = avg_stats.job_id
        WHERE latest.duration_seconds > avg_stats.avg_dur * 2
    """)

    for row in cursor.fetchall():
        alerts.append({
            "severity": "WARNING",
            "type": "slow_execution",
            "instance": row["instance_name"],
            "job": row["job_name"],
            "message": f"Last run took {row['last_duration']}s (avg is {row['avg_duration']}s)",
            "timestamp": None,
        })

    conn.close()
    return alerts


# ─────────────────────────────────────────────
# EXPORT: Generate a full dashboard report
# ─────────────────────────────────────────────

def generate_report(db_path="monitoring.db"):
    """
    Run all dashboard queries and return a complete report dict.
    This is what the dashboard UI will consume.
    """
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "health_summary": get_health_summary(db_path),
        "success_rates": get_success_rates_by_job(db_path),
        "daily_trend": get_daily_status_trend(db_path),
        "duration_stats": get_duration_stats(db_path),
        "duration_trend": get_duration_trend(db_path),
        "storage_trend": get_storage_trend(db_path),
        "storage_by_instance": get_storage_by_instance(db_path),
        "failure_details": get_failure_details(db_path),
        "failure_causes": get_failure_counts_by_error(db_path),
        "alerts": get_active_alerts(db_path),
    }


if __name__ == "__main__":
    import json
    report = generate_report()
    print(json.dumps(report, indent=2))