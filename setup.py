"""
DATABASE BACKUP & MAINTENANCE MONITOR
Phase 1: Database Setup

Run: python setup.py
Creates: monitoring.db
"""

import sqlite3
import os

if os.path.exists("monitoring.db"):
    os.remove("monitoring.db")

conn = sqlite3.connect("monitoring.db")
cursor = conn.cursor()

with open("schema.sql", "r") as f:
    cursor.executescript(f.read())

with open("sample_data.sql", "r") as f:
    cursor.executescript(f.read())

conn.commit()

# Verify
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
for table in cursor.fetchall():
    cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
    count = cursor.fetchone()[0]
    print(f"  {table[0]}: {count} rows")

print("\nDatabase created: monitoring.db")
conn.close()