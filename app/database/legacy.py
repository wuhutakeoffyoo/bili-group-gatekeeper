"""SQLite persistence and one-time migration from legacy JSON records."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

logger = logging.getLogger("group_request_manager.database")

DATABASE_FILE = Path("data/records/gatekeeper.sqlite3")
LEGACY_JOIN_REQUEST_FILE = Path("data/records/join_request_records.json")
LEGACY_LEAVE_RECORDS_PATH = Path("data/records/leave_records")
VERY_LEGACY_LEAVE_RECORDS_PATH = Path("data/leave_records")

MIGRATION_MARKER = "legacy_json_migration_v1"

_initialization_lock = threading.RLock()
_initialized_database: Path | None = None

SCHEMA_V1_SQL = """
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user_bindings (
    qq TEXT PRIMARY KEY,
    latest_bili_name TEXT NOT NULL DEFAULT '',
    latest_bili_uid INTEGER,
    first_seen TEXT NOT NULL DEFAULT '',
    last_seen TEXT NOT NULL DEFAULT '',
    groups_json TEXT NOT NULL DEFAULT '[]',
    used_bili_uids_json TEXT NOT NULL DEFAULT '[]',
    used_bili_names_json TEXT NOT NULL DEFAULT '[]',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    approved_count INTEGER NOT NULL DEFAULT 0,
    rejected_count INTEGER NOT NULL DEFAULT 0,
    ignored_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS bili_bindings (
    bili_uid INTEGER PRIMARY KEY,
    owner_qq TEXT NOT NULL,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    bili_names_json TEXT NOT NULL DEFAULT '[]'
);

CREATE INDEX IF NOT EXISTS idx_bili_bindings_owner_qq
    ON bili_bindings(owner_qq);

CREATE TABLE IF NOT EXISTS join_request_audits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    group_id TEXT NOT NULL,
    qq TEXT NOT NULL,
    bili_name TEXT NOT NULL DEFAULT '',
    bili_uid INTEGER,
    result TEXT NOT NULL,
    reasons_json TEXT NOT NULL DEFAULT '[]',
    follow_status TEXT NOT NULL DEFAULT '',
    medal_status TEXT NOT NULL DEFAULT '',
    leave_status TEXT NOT NULL DEFAULT '',
    conflict_qq TEXT NOT NULL DEFAULT '',
    condition_states_json TEXT NOT NULL DEFAULT '{}',
    condition_reasons_json TEXT NOT NULL DEFAULT '{}',
    stage_results_json TEXT NOT NULL DEFAULT '[]'
);

CREATE INDEX IF NOT EXISTS idx_audits_qq_id
    ON join_request_audits(qq, id DESC);
CREATE INDEX IF NOT EXISTS idx_audits_group_id
    ON join_request_audits(group_id, id DESC);
CREATE INDEX IF NOT EXISTS idx_audits_bili_uid
    ON join_request_audits(bili_uid, id DESC);

CREATE TABLE IF NOT EXISTS leave_records (
    group_id TEXT NOT NULL,
    qq TEXT NOT NULL,
    leave_time TEXT NOT NULL,
    nickname TEXT NOT NULL DEFAULT '',
    count INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (group_id, qq)
);

CREATE INDEX IF NOT EXISTS idx_leave_records_qq
    ON leave_records(qq);
"""


def _join_request_audit_columns(connection: sqlite3.Connection) -> set[str]:
    # PRAGMA 不支持绑定参数，表名固定，故用字面量 SQL
    return {
        str(row["name"])
        for row in connection.execute(
            "PRAGMA table_info(join_request_audits)"
        ).fetchall()
    }


def _migrate_schema_v1(connection: sqlite3.Connection) -> None:
    """Create the original SQLite schema."""
    connection.executescript(SCHEMA_V1_SQL)


def _migrate_schema_v2(connection: sqlite3.Connection) -> None:
    """Track whether an external QQ approval action was actually applied."""
    columns = _join_request_audit_columns(connection)
    if "action_status" not in columns:
        connection.execute(
            "ALTER TABLE join_request_audits ADD COLUMN action_status TEXT NOT NULL DEFAULT 'applied'"
        )
    if "action_error" not in columns:
        connection.execute(
            "ALTER TABLE join_request_audits ADD COLUMN action_error TEXT NOT NULL DEFAULT ''"
        )
    if "updated_at" not in columns:
        connection.execute(
            "ALTER TABLE join_request_audits ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''"
        )
    connection.execute(
        "UPDATE join_request_audits SET updated_at = created_at WHERE updated_at = ''"
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_audits_action_status_id
        ON join_request_audits(action_status, id DESC)
        """
    )


def _migrate_schema_v3(connection: sqlite3.Connection) -> None:
    """Add request idempotency and distinguish no-op audits from applied actions."""
    columns = _join_request_audit_columns(connection)
    if "request_key" not in columns:
        connection.execute(
            "ALTER TABLE join_request_audits ADD COLUMN request_key TEXT"
        )
    connection.execute(
        """
        UPDATE join_request_audits
        SET action_status = CASE
            WHEN result IN ('approved', 'rejected') THEN 'applied'
            ELSE 'not_required'
        END
        WHERE action_status = 'applied'
        """
    )
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_audits_request_key
        ON join_request_audits(request_key)
        """
    )


def _migrate_schema_v4(connection: sqlite3.Connection) -> None:
    """Add authoritative versioned configuration without changing business rows."""
    statements = (
        """CREATE TABLE IF NOT EXISTS config_revisions (
            version INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at REAL NOT NULL,
            actor TEXT NOT NULL DEFAULT '',
            config_json TEXT NOT NULL,
            manifest_json TEXT NOT NULL DEFAULT '{}',
            parent_version INTEGER,
            restored_from INTEGER
        )""",
        """CREATE TABLE IF NOT EXISTS config_head (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            version INTEGER NOT NULL REFERENCES config_revisions(version)
        )""",
        """CREATE TABLE IF NOT EXISTS runtime_state (
            namespace TEXT NOT NULL,
            key TEXT NOT NULL,
            value_json TEXT NOT NULL,
            updated_at REAL NOT NULL,
            PRIMARY KEY(namespace, key)
        )""",
        """CREATE TRIGGER IF NOT EXISTS config_revisions_no_update
        BEFORE UPDATE ON config_revisions BEGIN
            SELECT RAISE(ABORT, 'configuration revisions are append-only');
        END""",
        """CREATE TRIGGER IF NOT EXISTS config_revisions_no_delete
        BEFORE DELETE ON config_revisions BEGIN
            SELECT RAISE(ABORT, 'configuration revisions are append-only');
        END""",
    )
    for statement in statements:
        connection.execute(statement)


SCHEMA_MIGRATIONS = {
    1: _migrate_schema_v1,
    2: _migrate_schema_v2,
    3: _migrate_schema_v3,
    4: _migrate_schema_v4,
}
CURRENT_SCHEMA_VERSION = max(SCHEMA_MIGRATIONS)


def _apply_schema_migrations(connection: sqlite3.Connection) -> None:
    """Upgrade an existing database one version at a time."""
    expected_versions = set(range(1, CURRENT_SCHEMA_VERSION + 1))
    if set(SCHEMA_MIGRATIONS) != expected_versions:
        raise RuntimeError("SQLite 架构迁移版本必须从 1 开始连续编号")
    current_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    if current_version > CURRENT_SCHEMA_VERSION:
        raise sqlite3.DatabaseError(
            f"数据库版本 {current_version} 高于程序支持版本 {CURRENT_SCHEMA_VERSION}"
        )

    for version in range(current_version + 1, CURRENT_SCHEMA_VERSION + 1):
        migration = SCHEMA_MIGRATIONS[version]
        try:
            with connection:
                migration(connection)
                # PRAGMA cannot bind parameters; version is an internal range integer.
                connection.execute(f"PRAGMA user_version = {version}")
        except Exception:
            logger.exception("SQLite 架构迁移失败：v%s", version)
            raise
        logger.info("SQLite 架构已升级到 v%s", version)


def _json_dump(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _open_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DATABASE_FILE, timeout=30.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 5000")
    connection.execute("PRAGMA synchronous = NORMAL")
    return connection


def _load_json_file(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("读取旧版 JSON 数据失败：%s", path)
        raise
    if not isinstance(payload, dict):
        raise ValueError(f"旧版 JSON 数据顶层必须是对象：{path}")
    return payload


def _migrate_join_request_records(connection: sqlite3.Connection) -> dict[str, int]:
    counts = {"user_bindings": 0, "bili_bindings": 0, "audits": 0}
    if not LEGACY_JOIN_REQUEST_FILE.exists():
        return counts

    payload = _load_json_file(LEGACY_JOIN_REQUEST_FILE)
    for qq_key, raw in payload.get("user_bindings", {}).items():
        qq = str(raw.get("qq") or qq_key)
        connection.execute(
            """
            INSERT OR REPLACE INTO user_bindings (
                qq, latest_bili_name, latest_bili_uid, first_seen, last_seen,
                groups_json, used_bili_uids_json, used_bili_names_json,
                attempt_count, approved_count, rejected_count, ignored_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                qq,
                str(raw.get("latest_bili_name") or ""),
                raw.get("latest_bili_uid"),
                str(raw.get("first_seen") or ""),
                str(raw.get("last_seen") or ""),
                _json_dump(raw.get("groups") or []),
                _json_dump(raw.get("used_bili_uids") or []),
                _json_dump(raw.get("used_bili_names") or []),
                int(raw.get("attempt_count") or 0),
                int(raw.get("approved_count") or 0),
                int(raw.get("rejected_count") or 0),
                int(raw.get("ignored_count") or 0),
            ),
        )
        counts["user_bindings"] += 1

    for uid_key, raw in payload.get("bili_bindings", {}).items():
        bili_uid = int(raw.get("bili_uid") or uid_key)
        connection.execute(
            """
            INSERT OR REPLACE INTO bili_bindings (
                bili_uid, owner_qq, first_seen, last_seen, bili_names_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                bili_uid,
                str(raw.get("owner_qq") or ""),
                str(raw.get("first_seen") or ""),
                str(raw.get("last_seen") or ""),
                _json_dump(raw.get("bili_names") or []),
            ),
        )
        counts["bili_bindings"] += 1

    for raw in payload.get("audits", []):
        connection.execute(
            """
            INSERT INTO join_request_audits (
                created_at, group_id, qq, bili_name, bili_uid, result,
                reasons_json, follow_status, medal_status, leave_status,
                conflict_qq, condition_states_json, condition_reasons_json,
                stage_results_json, action_status, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(raw.get("created_at") or ""),
                str(raw.get("group_id") or ""),
                str(raw.get("qq") or ""),
                str(raw.get("bili_name") or ""),
                raw.get("bili_uid"),
                str(raw.get("result") or "ignored"),
                _json_dump(raw.get("reasons") or []),
                str(raw.get("follow_status") or ""),
                str(raw.get("medal_status") or ""),
                str(raw.get("leave_status") or ""),
                str(raw.get("conflict_qq") or ""),
                _json_dump(raw.get("condition_states") or {}),
                _json_dump(raw.get("condition_reasons") or {}),
                _json_dump(raw.get("stage_results") or []),
                (
                    "applied"
                    if str(raw.get("result") or "ignored") in {"approved", "rejected"}
                    else "not_required"
                ),
                str(raw.get("created_at") or ""),
            ),
        )
        counts["audits"] += 1

    return counts


def _legacy_leave_files() -> dict[str, Path]:
    selected: dict[str, Path] = {}
    for directory in (VERY_LEGACY_LEAVE_RECORDS_PATH, LEGACY_LEAVE_RECORDS_PATH):
        if not directory.exists():
            continue
        for path in directory.glob("leave_records_*.json"):
            group_id = path.stem.removeprefix("leave_records_")
            selected[group_id] = path
    return selected


def _migrate_leave_records(connection: sqlite3.Connection) -> int:
    count = 0
    for fallback_group_id, path in sorted(_legacy_leave_files().items()):
        payload = _load_json_file(path)
        group_id = str(payload.get("group_id") or fallback_group_id)
        for qq_key, raw in payload.get("records", {}).items():
            qq = str(raw.get("qq") or qq_key)
            connection.execute(
                """
                INSERT OR REPLACE INTO leave_records (
                    group_id, qq, leave_time, nickname, count
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    group_id,
                    qq,
                    str(raw.get("leave_time") or ""),
                    str(raw.get("nickname") or ""),
                    int(raw.get("count") or 1),
                ),
            )
            count += 1
    return count


def _migrate_legacy_json(connection: sqlite3.Connection) -> None:
    marker = connection.execute(
        "SELECT value FROM metadata WHERE key = ?", (MIGRATION_MARKER,)
    ).fetchone()
    if marker:
        return

    existing_counts = {
        "user_bindings": connection.execute(
            "SELECT COUNT(*) FROM user_bindings"
        ).fetchone()[0],
        "bili_bindings": connection.execute(
            "SELECT COUNT(*) FROM bili_bindings"
        ).fetchone()[0],
        "join_request_audits": connection.execute(
            "SELECT COUNT(*) FROM join_request_audits"
        ).fetchone()[0],
        "leave_records": connection.execute(
            "SELECT COUNT(*) FROM leave_records"
        ).fetchone()[0],
    }
    if any(existing_counts.values()):
        raise sqlite3.DatabaseError(
            "SQLite 已有业务数据但缺少 JSON 迁移标记，已停止自动迁移以避免重复记录"
        )

    join_counts = _migrate_join_request_records(connection)
    leave_count = _migrate_leave_records(connection)
    connection.execute(
        "UPDATE join_request_audits SET updated_at = created_at WHERE updated_at = ''"
    )
    summary = {**join_counts, "leave_records": leave_count}
    imported_counts = {
        "user_bindings": connection.execute(
            "SELECT COUNT(*) FROM user_bindings"
        ).fetchone()[0],
        "bili_bindings": connection.execute(
            "SELECT COUNT(*) FROM bili_bindings"
        ).fetchone()[0],
        "join_request_audits": connection.execute(
            "SELECT COUNT(*) FROM join_request_audits"
        ).fetchone()[0],
        "leave_records": connection.execute(
            "SELECT COUNT(*) FROM leave_records"
        ).fetchone()[0],
    }
    expected_counts = {
        "user_bindings": summary["user_bindings"],
        "bili_bindings": summary["bili_bindings"],
        "join_request_audits": summary["audits"],
        "leave_records": summary["leave_records"],
    }
    if imported_counts != expected_counts:
        raise sqlite3.DatabaseError(
            f"JSON 迁移数量校验失败：expected={expected_counts}, actual={imported_counts}"
        )
    connection.execute(
        "INSERT INTO metadata(key, value) VALUES (?, ?)",
        (MIGRATION_MARKER, _json_dump(summary)),
    )
    logger.info("旧版 JSON 数据已迁移到 SQLite：%s", summary)


def initialize_database() -> None:
    """Create the schema and atomically import legacy JSON data once."""
    global _initialized_database

    database_path = DATABASE_FILE.resolve()
    with _initialization_lock:
        if _initialized_database == database_path and DATABASE_FILE.exists():
            return

        DATABASE_FILE.parent.mkdir(parents=True, exist_ok=True)
        connection = _open_connection()
        try:
            current_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if current_version > CURRENT_SCHEMA_VERSION:
                raise sqlite3.DatabaseError(
                    f"数据库版本 {current_version} 高于程序支持版本 {CURRENT_SCHEMA_VERSION}"
                )
            if 0 < current_version < CURRENT_SCHEMA_VERSION:
                backup_dir = DATABASE_FILE.parent / "backups"
                backup_dir.mkdir(parents=True, exist_ok=True)
                backup_dir.chmod(0o700)
                backup_path = backup_dir / f"{DATABASE_FILE.stem}.pre-v{CURRENT_SCHEMA_VERSION}.{time.time_ns()}.sqlite3"
                descriptor = os.open(backup_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                os.close(descriptor)
                backup_connection = sqlite3.connect(backup_path)
                try:
                    connection.backup(backup_connection)
                finally:
                    backup_connection.close()
                backup_path.chmod(0o600)
            connection.execute("PRAGMA journal_mode = WAL")
            _apply_schema_migrations(connection)
            with connection:
                _migrate_legacy_json(connection)
            check = connection.execute("PRAGMA quick_check").fetchone()
            if not check or check[0] != "ok":
                raise sqlite3.DatabaseError(f"SQLite quick_check 失败：{check}")
        finally:
            connection.close()

        try:
            DATABASE_FILE.chmod(0o600)
        except OSError:
            if os.name == "posix":
                raise
            logger.debug("无法调整 SQLite 文件权限：%s", DATABASE_FILE, exc_info=True)
        _initialized_database = database_path


def configure_database(path: Path | str, *, root: Path | str | None = None) -> Path:
    """Select the shared business/config database before managers are used."""
    global DATABASE_FILE, LEGACY_JOIN_REQUEST_FILE, LEGACY_LEAVE_RECORDS_PATH
    global VERY_LEGACY_LEAVE_RECORDS_PATH
    selected = Path(path).expanduser().resolve()
    source_root = Path(root).resolve() if root is not None else selected.parent.parent.parent
    with _initialization_lock:
        DATABASE_FILE = selected
        LEGACY_JOIN_REQUEST_FILE = source_root / "data/records/join_request_records.json"
        LEGACY_LEAVE_RECORDS_PATH = source_root / "data/records/leave_records"
        VERY_LEGACY_LEAVE_RECORDS_PATH = source_root / "data/leave_records"
        reset_database_state()
    return selected


@contextmanager
def database_connection() -> Iterator[sqlite3.Connection]:
    initialize_database()
    connection = _open_connection()
    try:
        yield connection
    finally:
        connection.close()


def reset_database_state() -> None:
    """Reset process-local initialization state; intended for tests."""
    global _initialized_database
    with _initialization_lock:
        _initialized_database = None
