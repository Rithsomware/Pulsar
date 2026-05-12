"""
PULSAR Persistence Layer

SQLite-backed job store for state persistence and recovery.
"""

import json
import sqlite3
import logging
import threading
from datetime import datetime
from typing import Dict, List, Optional

from pulsar.pulsar_types import GPUJob, JobStatus

logger = logging.getLogger("pulsar.persistence")


class JobStore:
    """SQLite-backed persistent job store."""

    def __init__(self, db_path: str = "pulsar.db"):
        self._db_path = db_path
        self._lock = threading.RLock()
        self._init_db()

    def _init_db(self):
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    user TEXT NOT NULL,
                    namespace TEXT DEFAULT 'default',
                    gpu_required INTEGER NOT NULL,
                    gpu_memory_gb INTEGER DEFAULT 0,
                    priority TEXT DEFAULT 'NORMAL',
                    preemptible BOOLEAN DEFAULT 1,
                    workload_type TEXT DEFAULT 'Training',
                    framework TEXT DEFAULT 'PyTorch',
                    estimated_duration_minutes REAL DEFAULT 60.0,
                    status TEXT NOT NULL,
                    submitted_at TEXT,
                    admitted_at TEXT,
                    started_at TEXT,
                    completed_at TEXT,
                    preferred_gpu_class TEXT DEFAULT 'dgpu',
                    assigned_gpu_class TEXT,
                    assigned_gpu_resource TEXT,
                    fallback_applied BOOLEAN DEFAULT 0,
                    fallback_reason TEXT,
                    fallback_decided_at TEXT,
                    assigned_node TEXT,
                    assigned_gpus TEXT,
                    preemption_count INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_user ON jobs(user)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT,
                    message TEXT,
                    user TEXT,
                    job_id TEXT,
                    metadata TEXT,
                    timestamp TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self._ensure_jobs_schema(conn)
            conn.commit()
        logger.info("Job store initialized at %s", self._db_path)

    def _ensure_jobs_schema(self, conn: sqlite3.Connection):
        """Apply additive schema migrations for older sqlite files."""
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
        }
        migrations = [
            ("preferred_gpu_class", "ALTER TABLE jobs ADD COLUMN preferred_gpu_class TEXT DEFAULT 'dgpu'"),
            ("assigned_gpu_class", "ALTER TABLE jobs ADD COLUMN assigned_gpu_class TEXT"),
            ("assigned_gpu_resource", "ALTER TABLE jobs ADD COLUMN assigned_gpu_resource TEXT"),
            ("fallback_applied", "ALTER TABLE jobs ADD COLUMN fallback_applied BOOLEAN DEFAULT 0"),
            ("fallback_reason", "ALTER TABLE jobs ADD COLUMN fallback_reason TEXT"),
            ("fallback_decided_at", "ALTER TABLE jobs ADD COLUMN fallback_decided_at TEXT"),
        ]
        for col, ddl in migrations:
            if col not in columns:
                conn.execute(ddl)

    def _get_conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db_path, timeout=10)

    def save_job(self, job: GPUJob):
        with self._lock:
            d = job.to_dict()
            with self._get_conn() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO jobs
                    (job_id, user, namespace, gpu_required, gpu_memory_gb, priority,
                     preemptible, workload_type, framework, estimated_duration_minutes,
                     status, submitted_at, admitted_at, started_at, completed_at,
                     preferred_gpu_class, assigned_gpu_class, assigned_gpu_resource,
                     fallback_applied, fallback_reason, fallback_decided_at,
                     assigned_node, assigned_gpus, preemption_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    d["job_id"], d["user"], d["namespace"], d["gpu_required"],
                    d["gpu_memory_gb"], d["priority"], d["preemptible"],
                    d["workload_type"], d["framework"], d["estimated_duration_minutes"],
                    d["status"], d["submitted_at"], d["admitted_at"],
                    d["started_at"], d["completed_at"], d.get("preferred_gpu_class", "dgpu"),
                    d.get("assigned_gpu_class"), d.get("assigned_gpu_resource"),
                    int(bool(d.get("fallback_applied", False))), d.get("fallback_reason"),
                    d.get("fallback_decided_at"), d["assigned_node"],
                    json.dumps(d["assigned_gpus"]), d["preemption_count"],
                ))
                conn.commit()

    def load_job(self, job_id: str) -> Optional[GPUJob]:
        with self._lock:
            with self._get_conn() as conn:
                row = conn.execute(
                    "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
                ).fetchone()
                if row:
                    return self._row_to_job(row, conn)
                return None

    def list_jobs(self, user: Optional[str] = None,
                  status: Optional[str] = None,
                  limit: int = 100) -> List[GPUJob]:
        with self._lock:
            with self._get_conn() as conn:
                query = "SELECT * FROM jobs WHERE 1=1"
                params = []
                if user:
                    query += " AND user = ?"
                    params.append(user)
                if status:
                    query += " AND status = ?"
                    params.append(status)
                query += " ORDER BY submitted_at DESC LIMIT ?"
                params.append(limit)

                rows = conn.execute(query, params).fetchall()
                return [self._row_to_job(r, conn) for r in rows]

    def update_status(self, job_id: str, status: str):
        with self._lock:
            with self._get_conn() as conn:
                conn.execute(
                    "UPDATE jobs SET status = ? WHERE job_id = ?",
                    (status, job_id),
                )
                conn.commit()

    def get_job_count_by_status(self) -> Dict[str, int]:
        with self._lock:
            with self._get_conn() as conn:
                rows = conn.execute(
                    "SELECT status, COUNT(*) FROM jobs GROUP BY status"
                ).fetchall()
                return {r[0]: r[1] for r in rows}

    def save_event(self, event_type: str, message: str,
                   user: str = None, job_id: str = None,
                   metadata: dict = None):
        with self._lock:
            with self._get_conn() as conn:
                conn.execute(
                    "INSERT INTO events (event_type, message, user, job_id, metadata) VALUES (?, ?, ?, ?, ?)",
                    (event_type, message, user, job_id, json.dumps(metadata or {})),
                )
                conn.commit()

    def get_recent_events(self, limit: int = 50) -> List[dict]:
        with self._lock:
            with self._get_conn() as conn:
                rows = conn.execute(
                    "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)
                ).fetchall()
                return [
                    {
                        "id": r[0], "event_type": r[1], "message": r[2],
                        "user": r[3], "job_id": r[4],
                        "metadata": json.loads(r[5] or "{}"),
                        "timestamp": r[6],
                    }
                    for r in rows
                ]

    def _row_to_job(self, row, conn) -> GPUJob:
        cols = [desc[0] for desc in conn.execute("SELECT * FROM jobs LIMIT 0").description]
        d = dict(zip(cols, row))
        d["assigned_gpus"] = json.loads(d.get("assigned_gpus", "[]") or "[]")
        d["preemptible"] = bool(d.get("preemptible", 1))
        d["fallback_applied"] = bool(d.get("fallback_applied", 0))
        # Remove created_at which isn't a GPUJob field
        d.pop("created_at", None)
        return GPUJob.from_dict(d)
