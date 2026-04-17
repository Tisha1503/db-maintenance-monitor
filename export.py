import sqlite3
import csv
import os


DB_PATH = "monitoring.db"
CSV_DIR = "csv"


def export_tables(db_path=DB_PATH, csv_dir=CSV_DIR):
    os.makedirs(csv_dir, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    for table in ["database_instances", "jobs", "job_executions"]:
        cursor.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        filepath = os.path.join(csv_dir, f"{table}.csv")
        with open(filepath, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)

        print(f"  {filepath}: {len(rows)} rows")

    cursor.execute("""
        SELECT
            e.execution_id,
            d.instance_name,
            d.db_engine,
            d.environment,
            d.storage_gb AS instance_storage_gb,
            j.job_name,
            j.job_type,
            j.schedule,
            e.status,
            e.started_at,
            e.ended_at,
            e.duration_seconds,
            ROUND(e.duration_seconds / 60.0, 2) AS duration_minutes,
            e.records_processed,
            e.error_message,
            e.storage_used_mb,
            DATE(e.started_at) AS run_date,
            TIME(e.started_at) AS run_time,
            CASE WHEN e.status = 'SUCCESS' THEN 1 ELSE 0 END AS is_success,
            CASE WHEN e.status = 'FAILED' THEN 1 ELSE 0 END AS is_failure
        FROM job_executions e
        JOIN jobs j ON e.job_id = j.job_id
        JOIN database_instances d ON j.instance_id = d.instance_id
        ORDER BY e.started_at ASC
    """)

    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    filepath = os.path.join(csv_dir, "dashboard_view.csv")
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    print(f"  {filepath}: {len(rows)} rows")

    conn.close()
    print(f"\nDone! CSV files are in the '{csv_dir}' folder.")


if __name__ == "__main__":
    export_tables()