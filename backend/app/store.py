from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .pii import PIIProtector
from .schemas import RunMode, RunRecord, StepRecord, StepStatus, UploadPreview, UserResponse


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


class SQLiteStore:
    def __init__(self, db_path: str, upload_dir: str, protector: PIIProtector) -> None:
        self.db_path = db_path
        self.upload_dir = Path(upload_dir)
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        self.protector = protector
        self._init_db()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _table_columns(self, conn: sqlite3.Connection, table: str) -> set[str]:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {row[1] for row in rows}

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email_hash TEXT UNIQUE NOT NULL,
                    email_encrypted TEXT NOT NULL,
                    email_masked TEXT NOT NULL,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    query_encrypted TEXT NOT NULL,
                    query_display TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    final_response TEXT,
                    planner_source TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users (id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS steps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    step_id TEXT NOT NULL,
                    step_number INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    tool_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    input_payload TEXT,
                    output_payload TEXT,
                    error_message TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    FOREIGN KEY (run_id) REFERENCES runs (id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS uploads (
                    file_id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    filename TEXT NOT NULL,
                    path TEXT NOT NULL,
                    columns_json TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    preview_json_encrypted TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users (id)
                )
                """
            )
            # small compatibility upgrades in case an older DB sneaks in.
            if "user_id" not in self._table_columns(conn, "runs"):
                conn.execute("ALTER TABLE runs ADD COLUMN user_id INTEGER DEFAULT 0")
            if "query_encrypted" not in self._table_columns(conn, "runs"):
                conn.execute("ALTER TABLE runs ADD COLUMN query_encrypted TEXT DEFAULT ''")
            if "query_display" not in self._table_columns(conn, "runs"):
                conn.execute("ALTER TABLE runs ADD COLUMN query_display TEXT DEFAULT ''")
            upload_columns = self._table_columns(conn, "uploads")
            if upload_columns and "preview_json_encrypted" not in upload_columns:
                conn.execute("ALTER TABLE uploads ADD COLUMN preview_json_encrypted TEXT DEFAULT ''")
            if upload_columns and "user_id" not in upload_columns:
                conn.execute("ALTER TABLE uploads ADD COLUMN user_id INTEGER DEFAULT 0")

    def create_user(self, email: str, password_hash: str, role: str = "user") -> UserResponse:
        email_hash = self.protector.hash_email(email)
        email_masked = self.protector.mask_email(email)
        email_encrypted = self.protector.encrypt_text(self.protector.normalize_email(email))
        created_at = _now_iso()
        with self._conn() as conn:
            cursor = conn.execute(
                """
                INSERT INTO users (email_hash, email_encrypted, email_masked, password_hash, role, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (email_hash, email_encrypted, email_masked, password_hash, role, created_at),
            )
            row = conn.execute("SELECT * FROM users WHERE id = ?", (cursor.lastrowid,)).fetchone()
        return self._row_to_user(row)

    def get_user_auth(self, email: str) -> Optional[dict[str, Any]]:
        email_hash = self.protector.hash_email(email)
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM users WHERE email_hash = ?", (email_hash,)).fetchone()
        if not row:
            return None
        return dict(row)

    def get_user_by_id(self, user_id: int) -> Optional[UserResponse]:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        return self._row_to_user(row) if row else None

    def create_run(self, *, user_id: int, query: str, mode: RunMode, planner_source: str | None = None) -> RunRecord:
        with self._conn() as conn:
            cursor = conn.execute(
                """
                INSERT INTO runs (user_id, query_encrypted, query_display, mode, status, planner_source, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    self.protector.encrypt_text(query),
                    self.protector.sanitize_text(query),
                    mode.value,
                    StepStatus.pending.value,
                    planner_source,
                    _now_iso(),
                ),
            )
            row = conn.execute("SELECT * FROM runs WHERE id = ?", (cursor.lastrowid,)).fetchone()
        return self._row_to_run(row)

    def update_run(
        self,
        run_id: int,
        *,
        status: StepStatus | None = None,
        final_response: str | None = None,
        planner_source: str | None = None,
    ) -> RunRecord:
        updates = []
        params: list[Any] = []
        if status is not None:
            updates.append("status = ?")
            params.append(status.value)
        if final_response is not None:
            updates.append("final_response = ?")
            params.append(self.protector.sanitize_text(final_response))
        if planner_source is not None:
            updates.append("planner_source = ?")
            params.append(planner_source)
        if not updates:
            row = self.get_run(run_id)
            if row is None:
                raise ValueError(f"Run {run_id} not found")
            return row
        params.append(run_id)
        with self._conn() as conn:
            conn.execute(f"UPDATE runs SET {', '.join(updates)} WHERE id = ?", params)
            row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        return self._row_to_run(row)

    def get_run(self, run_id: int) -> Optional[RunRecord]:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        return self._row_to_run(row) if row else None

    def list_runs(self, user_id: int, limit: int = 20) -> List[RunRecord]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM runs WHERE user_id = ? ORDER BY id DESC LIMIT ?",
                (user_id, limit),
            ).fetchall()
        return [self._row_to_run(row) for row in rows]

    def create_step(
        self,
        run_id: int,
        step_id: str,
        step_number: int,
        title: str,
        tool_name: str,
        input_payload: Dict[str, Any],
    ) -> StepRecord:
        started_at = _now_iso()
        clean_input = self.protector.sanitize_payload(input_payload)
        with self._conn() as conn:
            cursor = conn.execute(
                """
                INSERT INTO steps (
                    run_id, step_id, step_number, title, tool_name, status,
                    input_payload, output_payload, started_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    step_id,
                    step_number,
                    title,
                    tool_name,
                    StepStatus.running.value,
                    json.dumps(clean_input),
                    json.dumps({}),
                    started_at,
                ),
            )
            row = conn.execute("SELECT * FROM steps WHERE id = ?", (cursor.lastrowid,)).fetchone()
        return self._row_to_step(row)

    def update_step(
        self,
        step_db_id: int,
        *,
        status: StepStatus,
        output_payload: Dict[str, Any] | None = None,
        error_message: str | None = None,
    ) -> StepRecord:
        finished_at = _now_iso()
        clean_output = self.protector.sanitize_payload(output_payload or {})
        clean_error = self.protector.sanitize_text(error_message) if error_message else None
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE steps
                SET status = ?, output_payload = ?, error_message = ?, finished_at = ?
                WHERE id = ?
                """,
                (status.value, json.dumps(clean_output), clean_error, finished_at, step_db_id),
            )
            row = conn.execute("SELECT * FROM steps WHERE id = ?", (step_db_id,)).fetchone()
        return self._row_to_step(row)

    def list_steps(self, run_id: int) -> List[StepRecord]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM steps WHERE run_id = ? ORDER BY step_number ASC",
                (run_id,),
            ).fetchall()
        return [self._row_to_step(row) for row in rows]

    def save_upload(
        self,
        *,
        user_id: int,
        file_id: str,
        filename: str,
        path: str,
        columns: List[str],
        row_count: int,
        preview_rows: List[Dict[str, Any]],
    ) -> UploadPreview:
        created_at = _now_iso()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO uploads (
                    file_id, user_id, filename, path, columns_json, row_count, preview_json_encrypted, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    file_id,
                    user_id,
                    filename,
                    path,
                    json.dumps(columns),
                    row_count,
                    self.protector.encrypt_json(preview_rows),
                    created_at,
                ),
            )
        return UploadPreview(
            file_id=file_id,
            filename=filename,
            columns=columns,
            row_count=row_count,
            preview_rows=self.protector.sanitize_payload(preview_rows),
        )

    def get_upload(self, file_id: str, user_id: int) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM uploads WHERE file_id = ? AND user_id = ?", (file_id, user_id)).fetchone()
        if not row:
            return None
        preview_rows = self.protector.decrypt_json(row["preview_json_encrypted"]) if row["preview_json_encrypted"] else []
        return {
            "file_id": row["file_id"],
            "user_id": row["user_id"],
            "filename": row["filename"],
            "path": row["path"],
            "columns": json.loads(row["columns_json"]),
            "row_count": row["row_count"],
            "preview_rows": self.protector.sanitize_payload(preview_rows),
            "created_at": row["created_at"],
        }

    @staticmethod
    def _row_to_user(row: sqlite3.Row) -> UserResponse:
        return UserResponse(
            id=row["id"],
            email=row["email_masked"],
            role=row["role"],
            created_at=datetime.fromisoformat(row["created_at"]) if row["created_at"] else None,
        )

    @staticmethod
    def _row_to_run(row: sqlite3.Row) -> RunRecord:
        return RunRecord(
            id=row["id"],
            user_id=row["user_id"],
            query=row["query_display"],
            mode=RunMode(row["mode"]),
            status=StepStatus(row["status"]),
            final_response=row["final_response"],
            planner_source=row["planner_source"],
            created_at=datetime.fromisoformat(row["created_at"]) if row["created_at"] else None,
        )

    @staticmethod
    def _row_to_step(row: sqlite3.Row) -> StepRecord:
        return StepRecord(
            id=row["id"],
            run_id=row["run_id"],
            step_id=row["step_id"],
            step_number=row["step_number"],
            title=row["title"],
            tool_name=row["tool_name"],
            status=StepStatus(row["status"]),
            input_payload=json.loads(row["input_payload"] or "{}"),
            output_payload=json.loads(row["output_payload"] or "{}"),
            error_message=row["error_message"],
            started_at=datetime.fromisoformat(row["started_at"]) if row["started_at"] else None,
            finished_at=datetime.fromisoformat(row["finished_at"]) if row["finished_at"] else None,
        )
