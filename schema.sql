CREATE TABLE IF NOT EXISTS database_instances (
    instance_id    INTEGER PRIMARY KEY,
    instance_name  TEXT NOT NULL,
    db_engine      TEXT NOT NULL,
    environment    TEXT NOT NULL,
    host           TEXT,
    storage_gb     REAL,
    created_at     TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id        INTEGER PRIMARY KEY,
    instance_id   INTEGER NOT NULL,
    job_name      TEXT NOT NULL,
    job_type      TEXT NOT NULL,
    schedule      TEXT,
    is_active     INTEGER DEFAULT 1,
    created_at    TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (instance_id) REFERENCES database_instances(instance_id)
);

CREATE TABLE IF NOT EXISTS job_executions (
    execution_id      INTEGER PRIMARY KEY,
    job_id            INTEGER NOT NULL,
    status            TEXT NOT NULL,
    started_at        TEXT NOT NULL,
    ended_at          TEXT,
    duration_seconds  REAL,
    records_processed INTEGER,
    error_message     TEXT,
    storage_used_mb   REAL,
    FOREIGN KEY (job_id) REFERENCES jobs(job_id)
);