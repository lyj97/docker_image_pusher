"""SQLite journal: crash-recovery state for the worker (client README 4).

Short transactions only; sequence allocation and enqueue commit together.
After a crash no sequence is reused and events keep their exact content."""

from __future__ import annotations

import json
import os
import sqlite3
from typing import Any, Dict, List, Optional

_SCHEMA = """
CREATE TABLE IF NOT EXISTS cpu_handoffs (
    attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id),
    evidence TEXT NOT NULL, acknowledgement TEXT, confirmed_boot TEXT
);
CREATE TABLE IF NOT EXISTS attempt_monitors (
    attempt_id TEXT PRIMARY KEY, snapshot TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS performance_phases (
    attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id),
    seq INTEGER NOT NULL,
    snapshot TEXT NOT NULL,
    PRIMARY KEY (attempt_id, seq)
);
CREATE TABLE IF NOT EXISTS cpu_tail_intents (
    attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id),
    evidence TEXT NOT NULL,
    acknowledgement TEXT
);
CREATE TABLE IF NOT EXISTS cpu_tail_checkpoints (
    attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id),
    stage TEXT NOT NULL,
    snapshot TEXT NOT NULL,
    PRIMARY KEY (attempt_id, stage)
);
CREATE TABLE IF NOT EXISTS command_execution (id INTEGER PRIMARY KEY CHECK(id=1), snapshot TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS node (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    worker_id TEXT NOT NULL,
    boot_id TEXT NOT NULL,
    registered_at TEXT
);
CREATE TABLE IF NOT EXISTS claim_requests (
    request_id TEXT PRIMARY KEY,
    resolved_attempt_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS attempts (
    attempt_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    lease_token TEXT NOT NULL,
    request_snapshot TEXT NOT NULL,
    phase TEXT,
    phase_instance INTEGER NOT NULL DEFAULT 0,
    current_stage TEXT,
    cancel_intent INTEGER NOT NULL DEFAULT 0,
    heartbeat_seq INTEGER NOT NULL DEFAULT 0,
    event_seq INTEGER NOT NULL DEFAULT 0,
    lease_expires_at TEXT,
    execution_deadline_at TEXT,
    finish_request_id TEXT,
    finish_payload TEXT,
    finish_accepted INTEGER NOT NULL DEFAULT 0,
    confirmed_terminal INTEGER NOT NULL DEFAULT 0,
    process_pid INTEGER,
    process_start_identity TEXT,
    started_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS processes (
    attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id),
    pid INTEGER NOT NULL,
    pgid INTEGER NOT NULL,
    start_identity TEXT NOT NULL,
    program TEXT NOT NULL,
    started_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS artifacts (
    attempt_id TEXT NOT NULL,
    role TEXT NOT NULL,
    path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    artifact_id TEXT,
    upload_state TEXT NOT NULL DEFAULT 'local',
    PRIMARY KEY (attempt_id, role)
);
CREATE TABLE IF NOT EXISTS outbox (
    attempt_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    payload TEXT NOT NULL,
    acked INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (attempt_id, seq)
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox (attempt_id, seq)
    WHERE acked = 0;
CREATE TABLE IF NOT EXISTS monitor_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    snapshot TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


class Journal:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(_SCHEMA)
        # lightweight migration for journals created before the new columns
        for ddl in (
            "ALTER TABLE attempts ADD COLUMN operation_key TEXT",
            "ALTER TABLE attempts ADD COLUMN current_stage TEXT",
            "ALTER TABLE attempts ADD COLUMN recovery_state TEXT",
            "ALTER TABLE attempts ADD COLUMN local_result TEXT",
            "ALTER TABLE attempts ADD COLUMN recovery_reason TEXT",
            "ALTER TABLE attempts ADD COLUMN local_completed INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE attempts ADD COLUMN engine_state TEXT",
            "ALTER TABLE attempts ADD COLUMN cancel_intent "
            "INTEGER NOT NULL DEFAULT 0",
        ):
            try:
                self.conn.execute(ddl)
            except sqlite3.OperationalError:
                pass  # column already exists
        self.conn.commit()
        # Engine submit intent must survive power loss before the one-shot POST.
        self.conn.execute("PRAGMA synchronous=FULL")

    @classmethod
    def open_readonly(cls, path: str) -> "Journal":
        """Inspect an existing journal without migrations or creating files."""
        from pathlib import Path
        from urllib.parse import quote
        journal = cls.__new__(cls)
        journal.conn = sqlite3.connect("file:" + quote(str(Path(path).resolve())) + "?mode=ro", uri=True)
        journal.conn.row_factory = sqlite3.Row
        journal.conn.execute("PRAGMA query_only=ON")
        return journal

    def active_attempts(self) -> List[Dict[str, Any]]:
        return [self.get_attempt(r['attempt_id']) for r in self.conn.execute(
            "SELECT attempt_id FROM attempts WHERE confirmed_terminal=0 ORDER BY started_at,attempt_id")]

    def cpu_handoff(self, attempt_id):
        try:
            row = self.conn.execute("SELECT * FROM cpu_handoffs WHERE attempt_id=?", (attempt_id,)).fetchone()
        except sqlite3.OperationalError as exc:
            if str(exc) == 'no such table: cpu_handoffs':
                return None  # read-only export of an older serial journal
            raise
        if row is None:
            return None
        return dict(evidence=json.loads(row['evidence']),
                    acknowledgement=json.loads(row['acknowledgement']) if row['acknowledgement'] else None,
                    confirmed_boot=row['confirmed_boot'])

    def begin_cpu_handoff(self, attempt_id, proof):
        record = self.get_attempt(attempt_id)
        if not record or not record.get('local_completed') or self.get_process(attempt_id):
            raise ValueError('handoff requires durable certified output and no process')
        with self.conn:
            self.conn.execute("INSERT OR IGNORE INTO cpu_handoffs(attempt_id,evidence) VALUES (?,?)",
                              (attempt_id, json.dumps(proof, sort_keys=True)))
        return self.cpu_handoff(attempt_id)

    def ack_cpu_handoff(self, attempt_id, response, boot_id, confirmed=False):
        from shared.h3proto import digest_obj
        intent = self.cpu_handoff(attempt_id)
        if (not intent or not isinstance(response, dict) or response.get('attempt_id') != attempt_id or
            response.get('handoff_id') != digest_obj(intent['evidence']) or
            response.get('boot_id') != boot_id or response.get('confirmed') is not confirmed or
            type(response.get('gpu_occupancy_released')) is not bool or
            response.get('overlap_enabled') is not response.get('gpu_occupancy_released') or
            (confirmed and response.get('gpu_occupancy_released') is not True) or
            type(response.get('artifact_bytes')) is not int or response['artifact_bytes'] !=
            sum(f['size_bytes'] for f in intent['evidence']['files'])):
            raise ValueError('ambiguous handoff ACK')
        ack = {k: response[k] for k in ('attempt_id','handoff_id','artifact_bytes',
                                      'gpu_occupancy_released','overlap_enabled')}
        if intent['acknowledgement'] is not None and intent['acknowledgement'] != ack:
            raise ValueError('handoff ACK is immutable')
        if confirmed and intent['acknowledgement'] is None:
            raise ValueError('confirm requires the persisted original ACK')
        with self.conn:
            self.conn.execute("UPDATE cpu_handoffs SET acknowledgement=?,confirmed_boot=? WHERE attempt_id=?",
                (json.dumps(ack, sort_keys=True), boot_id if confirmed else None, attempt_id))

    def checkpoint_cpu_tail(self, attempt_id: str, stage: str,
                            max_bytes: int) -> Dict[str, Any]:
        """Durable observations only: never release ownership or capacity.

        First observation survives replay. A durable checkpoint requires the
        existing atomic output commit, not merely an exited engine or files.
        """
        import datetime
        if stage not in ("engine_exited", "durable", "uploaded", "verified"):
            raise ValueError("invalid CPU tail stage")
        if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes <= 0:
            raise ValueError("invalid CPU tail byte bound")
        record = self.get_attempt(attempt_id)
        if record is None:
            raise ValueError("unknown attempt")
        rows = self.artifacts_for(attempt_id)
        if stage != "engine_exited" and not record.get("local_completed"):
            raise ValueError("CPU tail requires durable local completion")
        if stage == "engine_exited" and self.get_process(attempt_id):
            raise ValueError("engine process exit is unproven")
        if stage in ("uploaded", "verified") and (not rows or any(
                r["upload_state"] != "uploaded" for r in rows)):
            raise ValueError("artifact delivery is incomplete")
        if stage == "verified" and not (record.get("confirmed_terminal")
                and record.get("finish_accepted")
                and (record.get("finish_payload") or {}).get("status") == "SUCCEEDED"):
            raise ValueError("remote terminal confirmation is missing")
        size = sum(r["size_bytes"] for r in rows)
        handoff = self.cpu_handoff(attempt_id)
        released = bool(handoff and handoff['acknowledgement'] and
                        handoff['acknowledgement']['gpu_occupancy_released'])
        snapshot = dict(attempt_id=attempt_id, task_id=record["task_id"],
                        stage=stage, observed_at=datetime.datetime.now(
                            datetime.timezone.utc).isoformat(),
                        artifact_bytes=size, max_bytes=max_bytes,
                        tail_slots=1, gpu_slots=1, overlap_enabled=released,
                        gpu_occupancy_released=released,
                        within_byte_bound=size <= max_bytes,
                        blocked_reason="bounded_plural_publication" if released else "claim_and_recovery_require_terminal_attempt")
        with self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO cpu_tail_checkpoints VALUES (?, ?, ?)",
                (attempt_id, stage, json.dumps(snapshot, sort_keys=True)))
        return self.cpu_tail_checkpoints(attempt_id)[stage]

    def cpu_tail_checkpoints(self, attempt_id: str) -> Dict[str, Any]:
        return {r["stage"]: json.loads(r["snapshot"]) for r in
                self.conn.execute("SELECT stage, snapshot FROM cpu_tail_checkpoints "
                                  "WHERE attempt_id=?", (attempt_id,))}

    def cpu_tail_intent(self, attempt_id: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT * FROM cpu_tail_intents WHERE attempt_id=?", (attempt_id,)).fetchone()
        if row is None:
            return None
        return dict(evidence=json.loads(row['evidence']),
                    acknowledgement=json.loads(row['acknowledgement'])
                    if row['acknowledgement'] else None)

    def begin_cpu_tail_intent(self, attempt_id: str, evidence: Dict[str, Any]) -> Dict[str, Any]:
        """Persist exact reservation evidence before network I/O; never replace it."""
        record = self.get_attempt(attempt_id)
        if not record or not record.get('local_completed') or self.get_process(attempt_id):
            raise ValueError('CPU tail intent requires durable output and engine exit')
        with self.conn:
            self.conn.execute("INSERT OR IGNORE INTO cpu_tail_intents(attempt_id,evidence) VALUES (?,?)",
                              (attempt_id, json.dumps(evidence, sort_keys=True)))
        return self.cpu_tail_intent(attempt_id)

    def acknowledge_cpu_tail_intent(self, attempt_id: str, response: Dict[str, Any]) -> None:
        """Only an explicit serial reservation ACK permits publication to continue."""
        intent = self.cpu_tail_intent(attempt_id)
        if (intent is None or not isinstance(response, dict)
                or response.get('attempt_id') != attempt_id
                or response.get('overlap_enabled') is not False
                or response.get('gpu_occupancy_released') is not False
                or type(response.get('artifact_bytes')) is not int
                or response['artifact_bytes'] != sum(f['size_bytes'] for f in intent['evidence']['files'])
                or not isinstance(response.get('reserved_at'), str) or not response['reserved_at']):
            raise ValueError('ambiguous CPU tail reservation acknowledgement')
        # Keep only authority/evidence fields, never arbitrary response metadata.
        response = {key: response[key] for key in (
            'attempt_id', 'reserved_at', 'artifact_bytes',
            'overlap_enabled', 'gpu_occupancy_released')}
        if intent['acknowledgement'] is not None and intent['acknowledgement'] != response:
            raise ValueError('CPU tail reservation acknowledgement changed')
        with self.conn:
            self.conn.execute("UPDATE cpu_tail_intents SET acknowledgement=? WHERE attempt_id=?",
                              (json.dumps(response, sort_keys=True), attempt_id))

    def cpu_tail_unsupported(self, attempt_id: str) -> None:
        """Persist a definitive first-call legacy-server response, without authority."""
        with self.conn:
            self.conn.execute("UPDATE cpu_tail_intents SET acknowledgement=? "
                              "WHERE attempt_id=? AND acknowledgement IS NULL",
                              (json.dumps({'unsupported': True}), attempt_id))

    def close(self) -> None:
        self.conn.close()

    # -- node -----------------------------------------------------------

    def set_node(self, worker_id: str, boot_id: str) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO node (id, worker_id, boot_id, registered_at) "
                "VALUES (1, ?, ?, datetime('now'))",
                (worker_id, boot_id),
            )

    def get_node(self) -> Optional[Dict[str, Any]]:
        row = self.conn.execute("SELECT * FROM node WHERE id = 1").fetchone()
        return dict(row) if row else None

    # -- claim requests ---------------------------------------------------

    def new_claim_request_id(self) -> str:
        """Persist a fresh claim request_id BEFORE calling claim (client
        README 3.2: persist the id first; reuse it when the response is
        unclear)."""
        row = self.conn.execute(
            "SELECT request_id, resolved_attempt_id FROM claim_requests "
            "WHERE resolved_attempt_id IS NULL ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if row is not None:
            return row["request_id"]
        import secrets

        request_id = f"claim_{secrets.token_hex(10)}"
        with self.conn:
            self.conn.execute(
                "INSERT INTO claim_requests (request_id) VALUES (?)", (request_id,)
            )
        return request_id

    def resolve_claim(self, request_id: str, attempt_id: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE claim_requests SET resolved_attempt_id = ? WHERE request_id = ?",
                (attempt_id, request_id),
            )

    def close_claim(self, request_id: str) -> None:
        """A definitive empty (204) answer closes the request; the next claim
        uses a new id (protocol 3)."""
        with self.conn:
            self.conn.execute(
                "UPDATE claim_requests SET resolved_attempt_id = ? "
                "WHERE request_id = ? AND resolved_attempt_id IS NULL",
                ("empty-" + request_id, request_id),
            )

    # -- attempts ---------------------------------------------------------

    def start_attempt(
        self,
        attempt_id: str,
        task_id: str,
        lease_token: str,
        request_snapshot: Dict[str, Any],
        lease_expires_at: str,
        execution_deadline_at: str,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO attempts
                    (attempt_id, task_id, lease_token, request_snapshot,
                     lease_expires_at, execution_deadline_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    attempt_id,
                    task_id,
                    lease_token,
                    json.dumps(request_snapshot, ensure_ascii=False),
                    lease_expires_at,
                    execution_deadline_at,
                ),
            )

        record = self.get_attempt(attempt_id)
        if (record['task_id'] != task_id or record['lease_token'] != lease_token or
                record['request_snapshot'] != request_snapshot):
            raise ValueError('attempt replay changes immutable identity')

    def get_attempt(self, attempt_id: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT * FROM attempts WHERE attempt_id = ?", (attempt_id,)
        ).fetchone()
        if row is None:
            return None
        data = dict(row)
        data["request_snapshot"] = json.loads(data["request_snapshot"])
        if data.get("local_result"):
            data["local_result"] = json.loads(data["local_result"])
        if data.get("finish_payload"):
            data["finish_payload"] = json.loads(data["finish_payload"])
        return data

    def active_attempt(self) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT attempt_id FROM attempts WHERE confirmed_terminal = 0 "
            "ORDER BY started_at DESC, attempt_id DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        return self.get_attempt(row["attempt_id"])

    def reconcile_attempt(self, attempt_id: str) -> None:
        """Mark an attempt reconciled without executing it (client review
        B2: a claim replay that returned a terminal/expired allocation)."""
        with self.conn:
            self.conn.execute(
                "UPDATE attempts SET confirmed_terminal = 1 "
                "WHERE attempt_id = ?", (attempt_id,)
            )

    def update_attempt(self, attempt_id: str, **fields: Any) -> None:
        if not fields:
            return
        values = []
        for v in fields.values():
            if isinstance(v, (dict, list)):
                values.append(json.dumps(v, ensure_ascii=False))
            else:
                values.append(v)
        sets = ", ".join(f"{k} = ?" for k in fields)
        with self.conn:
            self.conn.execute(
                f"UPDATE attempts SET {sets} WHERE attempt_id = ?",
                (*values, attempt_id),
            )

    def record_process(
        self, attempt_id: str, pid: int, pgid: int, start_identity: str,
        program: str,
    ) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO processes "
                "(attempt_id, pid, pgid, start_identity, program) "
                "VALUES (?, ?, ?, ?, ?)",
                (attempt_id, pid, pgid, start_identity, program),
            )
            self.conn.execute(
                "UPDATE attempts SET process_pid = ?, process_start_identity = ? "
                "WHERE attempt_id = ?",
                (pid, start_identity, attempt_id),
            )

    def get_process(self, attempt_id: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT * FROM processes WHERE attempt_id = ?", (attempt_id,)
        ).fetchone()
        return dict(row) if row else None

    def clear_process(self, attempt_id: str) -> None:
        with self.conn:
            self.conn.execute(
                "DELETE FROM processes WHERE attempt_id = ?", (attempt_id,)
            )

    # -- artifacts ---------------------------------------------------------

    def put_artifact(
        self, attempt_id: str, role: str, path: str, size_bytes: int, sha256: str
    ) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO artifacts "
                "(attempt_id, role, path, size_bytes, sha256, upload_state) "
                "VALUES (?, ?, ?, ?, ?, 'local')",
                (attempt_id, role, path, size_bytes, sha256),
            )

    def complete_local_output(self, attempt_id: str, manifest_path: str,
                              size_bytes: int, sha256: str, result: Dict[str, Any]) -> None:
        """Commit the last validated artifact and terminal intent atomically.

        Media files and manifest must already be fsynced by the validator.
        A restart sees either incomplete validation or sufficient completion
        evidence, never a manifest row without the corresponding intent.
        """
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO artifacts "
                "(attempt_id, role, path, size_bytes, sha256, upload_state) "
                "VALUES (?, 'manifest', ?, ?, ?, 'local')",
                (attempt_id, manifest_path, size_bytes, sha256))
            self.conn.execute(
                "UPDATE attempts SET local_completed=1, local_result=?, finish_request_id=? "
                "WHERE attempt_id=?", (json.dumps(result, ensure_ascii=False),
                                       f"fin_{attempt_id}", attempt_id))

    def set_artifact_remote(self, attempt_id: str, role: str,
                            artifact_id: str, state: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE artifacts SET artifact_id = ?, upload_state = ? "
                "WHERE attempt_id = ? AND role = ?",
                (artifact_id, state, attempt_id, role),
            )

    def artifacts_for(self, attempt_id: str) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM artifacts WHERE attempt_id = ?", (attempt_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    # -- outbox -------------------------------------------------------------

    def append_event(self, attempt_id: str, payload: Dict[str, Any],
                     performance: Optional[Dict[str, Any]] = None) -> int:
        """Allocate the next event seq AND enqueue the payload in ONE
        transaction (client README 4).  A plain INSERT makes any seq reuse
        fail loudly instead of silently replacing content."""
        with self.conn:
            row = self.conn.execute(
                "SELECT COALESCE(MAX(seq), 0) + 1 AS n FROM outbox "
                "WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
            seq = int(row["n"])
            self.conn.execute(
                "INSERT INTO outbox (attempt_id, seq, payload) VALUES (?, ?, ?)",
                (attempt_id, seq, json.dumps(payload, ensure_ascii=False)),
            )
            if performance is not None:
                self.conn.execute("INSERT INTO performance_phases VALUES (?, ?, ?)",
                                  (attempt_id, seq, json.dumps(performance, sort_keys=True)))
        return seq

    def performance_evidence(self, attempt_id: str) -> Dict[str, Any]:
        """Allowlisted local evidence; never export request bodies or credentials.

        Operation keys and node drain proof belong to the parent's authoritative
        task ledger; a Worker cannot certify either from its journal.
        """
        record = self.get_attempt(attempt_id)
        if record is None:
            raise ValueError("unknown attempt")
        phases = [dict(seq=r['seq'], **json.loads(r['snapshot'])) for r in
                  self.conn.execute("SELECT seq,snapshot FROM performance_phases "
                                    "WHERE attempt_id=? ORDER BY seq", (attempt_id,))]
        durations = []
        for index, phase in enumerate(phases):
            following = phases[index + 1] if index + 1 < len(phases) else None
            duration = None
            if following and phase['boot_id'] == following['boot_id']:
                delta = following['monotonic_seconds'] - phase['monotonic_seconds']
                if delta >= 0:
                    duration = delta
            durations.append(dict(phase=phase['phase'], phase_instance=phase['phase_instance'],
                                  seconds=duration, semantics="worker_phase_transition_interval"))
        denoise = [d['seconds'] for d in durations if d['phase'] == 'denoise']
        handoff = self.cpu_handoff(attempt_id)
        released = bool(handoff and handoff['acknowledgement'] and
                        handoff['acknowledgement']['gpu_occupancy_released'])
        return dict(schema_version=1, attempt_id=attempt_id, task_id=record['task_id'],
                    operation_key=record.get('operation_key'), operation_key_source="claim_or_parent_task_ledger",
                    overlap_enabled=released, gpu_occupancy_released=released, handoff=handoff,
                    verified_completion=bool(record.get('confirmed_terminal') and
                        record.get('finish_accepted') and
                        (record.get('finish_payload') or {}).get('status') == 'SUCCEEDED'),
                    verified_drain=None, drain_source="fresh_control_plane_and_node_proof_required",
                    phases=phases, phase_durations=durations,
                    denoise_seconds=sum(denoise) if denoise and None not in denoise else None,
                    checkpoints=self.cpu_tail_checkpoints(attempt_id),
                    artifacts=[{k: r[k] for k in ('role','size_bytes','sha256','upload_state')}
                               for r in self.artifacts_for(attempt_id)],
                    rollback_reason=None, rollback_source="parent_gate_evaluation_required")

    def pending_events(self, attempt_id: str, limit: int = 64) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM outbox WHERE attempt_id = ? AND acked = 0 "
            "ORDER BY seq LIMIT ?",
            (attempt_id, limit),
        ).fetchall()
        return [
            {"seq": r["seq"], "payload": json.loads(r["payload"])} for r in rows
        ]

    def ack_events(self, attempt_id: str, seqs: List[int]) -> None:
        if not seqs:
            return
        marks = ",".join("?" for _ in seqs)
        with self.conn:
            self.conn.execute(
                f"UPDATE outbox SET acked = 1 WHERE attempt_id = ? "
                f"AND seq IN ({marks})",
                (attempt_id, *seqs),
            )

    def prune_outbox(self, attempt_id: str, keep: int = 256) -> None:
        """Drop acked rows beyond a retention window so the table does not
        grow without bound (client review C13)."""
        with self.conn:
            self.conn.execute(
                "DELETE FROM outbox WHERE attempt_id = ? AND acked = 1 "
                "AND seq <= (SELECT COALESCE(MAX(seq), 0) FROM outbox "
                "           WHERE attempt_id = ?) - ?",
                (attempt_id, attempt_id, keep),
            )

    def drop_outbox(self, attempt_id: str) -> None:
        with self.conn:
            self.conn.execute(
                "DELETE FROM outbox WHERE attempt_id = ?", (attempt_id,)
            )

    def pending_event_count(self) -> int:
        row = self.conn.execute(
            "SELECT count(*) AS n FROM outbox WHERE acked = 0"
        ).fetchone()
        return int(row["n"]) if row else 0

    # -- monitoring snapshot (protocol 11) ----------------------------------

    def set_monitor_state(self, snapshot: Dict[str, Any], attempt_id=None) -> None:
        if attempt_id is not None:
            with self.conn:
                self.conn.execute("INSERT OR REPLACE INTO attempt_monitors VALUES (?,?)", (attempt_id, json.dumps(snapshot)))
            return
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO monitor_state (id, snapshot, updated_at) "
                "VALUES (1, ?, datetime('now'))",
                (json.dumps(snapshot, ensure_ascii=False),),
            )

    def get_monitor_state(self, attempt_id=None) -> Optional[Dict[str, Any]]:
        if attempt_id is not None:
            row = self.conn.execute("SELECT snapshot FROM attempt_monitors WHERE attempt_id=?", (attempt_id,)).fetchone()
            return json.loads(row[0]) if row else None
        row = self.conn.execute(
            "SELECT snapshot FROM monitor_state WHERE id = 1"
        ).fetchone()
        if row is None:
            return None
        try:
            return json.loads(row["snapshot"])
        except json.JSONDecodeError:
            return None

    def get_command(self):
        row = self.conn.execute('SELECT snapshot FROM command_execution WHERE id=1').fetchone()
        return json.loads(row[0]) if row else None

    def save_command(self, record):
        with self.conn:
            self.conn.execute('INSERT OR REPLACE INTO command_execution VALUES (1, ?)',
                              (json.dumps(record),))
