from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any


if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from backend.database import Database
else:
    from .database import Database


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if value == float("inf"):
        return "inf"
    return value


def _db_activity_score(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        conn = sqlite3.connect(str(path))
        cur = conn.cursor()
        counts: list[int] = []
        for sql in (
            "SELECT COUNT(*) FROM signal_audit",
            "SELECT COUNT(*) FROM trades WHERE side = 'SELL'",
            "SELECT COUNT(*) FROM positions",
        ):
            cur.execute(sql)
            counts.append(int(cur.fetchone()[0] or 0))
        conn.close()
        return int(sum(counts))
    except Exception:
        return 0


def _resolve_db_path(cli_db: str | None) -> tuple[Path, str]:
    cwd = Path.cwd()
    backend_dir = Path(__file__).resolve().parent

    if cli_db:
        raw_path = Path(cli_db).expanduser()
        resolved = raw_path.resolve() if raw_path.is_absolute() else (cwd / raw_path).resolve()
        return resolved, "explicit_cli_override"

    env_db = (os.getenv("DB_PATH") or "trading_bot.db").strip() or "trading_bot.db"
    env_path = Path(env_db).expanduser()
    if env_path.is_absolute():
        return env_path.resolve(), "env_absolute_db_path"

    cwd_candidate = (cwd / env_path).resolve()
    if env_path.parent not in (Path(""), Path(".")):
        return cwd_candidate, "cwd_relative_env_db_path"

    backend_candidate = (backend_dir / env_path.name).resolve()
    cwd_score = _db_activity_score(cwd_candidate)
    backend_score = _db_activity_score(backend_candidate)

    if cwd_candidate != backend_candidate and cwd_candidate.exists() and backend_candidate.exists():
        if cwd_score == 0 and backend_score > 0:
            return backend_candidate, "preferred_backend_db_over_empty_root_db"
        if env_path.name == "trading_bot.db":
            return backend_candidate, "preferred_backend_db_default"
        return cwd_candidate, "cwd_relative_default"

    if backend_candidate.exists() and env_path.name == "trading_bot.db":
        return backend_candidate, "backend_default_exists"

    if cwd_candidate.exists():
        return cwd_candidate, "cwd_relative_default"

    return backend_candidate, "backend_default_fallback"


def main() -> None:
    parser = argparse.ArgumentParser(description="Print trade measurement report from SQLite.")
    parser.add_argument("--db", default=None, help="SQLite DB path override")
    parser.add_argument("--validate", action="store_true", help="Print DB/report validation diagnostics as JSON")
    parser.add_argument("--include-smoke", action="store_true", help="Include __codex_smoke__ rows in validation preview")
    parser.add_argument("--recent-closed-limit", type=int, default=20, help="How many recent closed trades to include")
    args = parser.parse_args()

    resolved_db_path, resolution_reason = _resolve_db_path(args.db)
    db_exists_before_open = resolved_db_path.exists()

    db = Database(str(resolved_db_path))
    db._create_tables()
    try:
        report_health = db.get_report_health()
        report_health.update(
            {
                "db_path_used": str(resolved_db_path),
                "db_exists": bool(db_exists_before_open),
                "db_path_resolution_reason": resolution_reason,
            }
        )

        if args.validate:
            blocked_signal_summary = db.get_blocked_signal_summary()
            signal_audit_validation = db.get_signal_audit_validation(preview_limit=5, include_smoke=bool(args.include_smoke))
            validation = db.get_trade_audit_validation()
            payload = {
                "report_health": report_health,
                "signal_audit_validation": signal_audit_validation,
                "blocked_signal_summary": blocked_signal_summary,
                "audit_validation": validation,
            }
        else:
            payload = db.get_trade_measurement_report(
                recent_closed_limit=max(1, int(args.recent_closed_limit)),
                report_health=report_health,
            )
        print(json.dumps(_json_safe(payload), indent=2, ensure_ascii=False))
    finally:
        db.close_db()


if __name__ == "__main__":
    main()
