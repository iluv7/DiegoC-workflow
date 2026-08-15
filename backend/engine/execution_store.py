"""Small SQLite persistence layer for workflow and node execution state."""

import json
import os
import sqlite3
import threading
from pathlib import Path
from typing import Any


class ExecutionStore:
    def __init__(self, path: str | None = None) -> None:
        default = Path(__file__).resolve().parents[1] / "workflow_runs.db"
        self.path = path or os.getenv("WORKFLOW_DB_PATH", str(default))
        self._lock = threading.Lock()
        self._init()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def _init(self) -> None:
        with self._connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS workflow_runs (
                    run_id TEXT PRIMARY KEY, status TEXT NOT NULL,
                    inputs TEXT NOT NULL, output TEXT, error TEXT,
                    created_at REAL NOT NULL, updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS node_runs (
                    run_id TEXT NOT NULL, node_id TEXT NOT NULL,
                    status TEXT NOT NULL, retry_count INTEGER NOT NULL DEFAULT 0,
                    output TEXT, error TEXT, updated_at REAL NOT NULL,
                    PRIMARY KEY (run_id, node_id)
                );
            """)

    def create_run(self, run_id: str, inputs: dict[str, Any], now: float) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO workflow_runs VALUES (?, ?, ?, NULL, NULL, ?, ?)",
                (run_id, "running", json.dumps(inputs, ensure_ascii=False), now, now),
            )

    def update_run(self, run_id: str, status: str, now: float, *, output: Any = None, error: str | None = None) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE workflow_runs SET status=?, output=?, error=?, updated_at=? WHERE run_id=?",
                (status, json.dumps(output, ensure_ascii=False) if output is not None else None, error, now, run_id),
            )

    def update_node(self, run_id: str, node_id: str, status: str, retry_count: int, now: float, *, output: Any = None, error: str | None = None) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """INSERT INTO node_runs VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(run_id,node_id) DO UPDATE SET
                   status=excluded.status,retry_count=excluded.retry_count,
                   output=excluded.output,error=excluded.error,updated_at=excluded.updated_at""",
                (run_id, node_id, status, retry_count,
                 json.dumps(output, ensure_ascii=False) if output is not None else None, error, now),
            )

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            run = conn.execute("SELECT * FROM workflow_runs WHERE run_id=?", (run_id,)).fetchone()
            if not run:
                return None
            nodes = conn.execute("SELECT * FROM node_runs WHERE run_id=? ORDER BY updated_at", (run_id,)).fetchall()
        result = dict(run)
        for key in ("inputs", "output"):
            if result.get(key) is not None:
                result[key] = json.loads(result[key])
        result["nodes"] = []
        for row in nodes:
            item = dict(row)
            if item.get("output") is not None:
                item["output"] = json.loads(item["output"])
            result["nodes"].append(item)
        return result
