
-- TABLE 1: The databases we're monitoring
CREATE TABLE IF NOT EXISTS database_instances (
    instance_id    INTEGER PRIMARY KEY,
    instance_name  TEXT NOT NULL,        -- human-readable name
    db_engine      TEXT NOT NULL,        -- PostgreSQL, MySQL, etc.
    environment    TEXT NOT NULL,        -- production, staging, dev
    host           TEXT,                -- server address
    storage_gb     REAL,                -- allocated storage
    created_at     TEXT DEFAULT (datetime('now'))
);

-- TABLE 2: Jobs configured for each database
-- Each database can have multiple maintenance jobs
CREATE TABLE IF NOT EXISTS jobs (
    job_id        INTEGER PRIMARY KEY,
    instance_id   INTEGER NOT NULL,     -- which database does this job belong to?
    job_name      TEXT NOT NULL,        -- e.g. "nightly_backup"
    job_type      TEXT NOT NULL,        -- backup, index_rebuild, log_cleanup, statistics, archival
    schedule      TEXT,                -- cron expression or description
    is_active     INTEGER DEFAULT 1,   -- 1 = active, 0 = disabled
    created_at    TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (instance_id) REFERENCES database_instances(instance_id)
);

-- TABLE 3: Every time a job runs, we log it here
-- This is the biggest table — it grows every day
CREATE TABLE IF NOT EXISTS job_executions (
    execution_id      INTEGER PRIMARY KEY,
    job_id            INTEGER NOT NULL,  -- which job ran?
    status            TEXT NOT NULL,     -- SUCCESS, FAILED, RUNNING, WARNING
    started_at        TEXT NOT NULL,     -- when did it start?
    ended_at          TEXT,             -- when did it finish? (null if still running)
    duration_seconds  REAL,             -- how long did it take?
    records_processed INTEGER,          -- rows/files processed
    error_message     TEXT,             -- null if successful
    storage_used_mb   REAL,             -- storage consumed (negative = freed space)
    FOREIGN KEY (job_id) REFERENCES jobs(job_id)
);