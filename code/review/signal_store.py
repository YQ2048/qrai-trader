import json
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import text

from code.analytics.reason_generator import build_reason_payload
from code.core.db_manager import get_engine
from code.core.logger import get_logger

logger = get_logger(__name__)


VALID_SIGNAL_STATUSES = {"PENDING", "CONFIRMED", "EXECUTED", "CANCELED"}
ALLOWED_TRANSITIONS = {
    "PENDING": {"CONFIRMED", "CANCELED"},
    "CONFIRMED": {"EXECUTED", "CANCELED"},
    "EXECUTED": set(),
    "CANCELED": set(),
}

# 模块级标记：schema migration 只执行一次
_schema_migrated = False


def make_signal_uid(signal_date: str, ts_code: str, strategy_type: str) -> str:
    return f"{signal_date}|{ts_code}|{strategy_type}"


def _build_signal_rows(ranked_candidates: List[Dict], trade_date: str, top_n: int = 15) -> List[Dict]:
    rows: List[Dict] = []
    for candidate in ranked_candidates[:top_n]:
        strategy_signals = candidate.get("signals", [])
        strategy_ids = [s.strategy_id for s in strategy_signals]
        reason_payload = build_reason_payload(candidate)
        reason_text = reason_payload["text"]

        if not strategy_ids:
            strategy_ids = ["UNKNOWN"]

        # 为每个触发策略分别生成一行 signal
        for strategy_id in strategy_ids:
            rows.append(
                {
                    "signal_date": trade_date,
                    "ts_code": candidate.get("ts_code"),
                    "strategy_type": strategy_id,
                    "signal_uid": make_signal_uid(trade_date, candidate.get("ts_code"), strategy_id),
                    "score": float(candidate.get("composite_score", 0.0)),
                    "vlm_reason": reason_text,
                    "status": "PENDING",
                    "triggered_factors": ",".join(strategy_ids),
                    "score_detail": json.dumps(candidate.get("score_detail", {}), ensure_ascii=False),
                    "reason_text": reason_text,
                    "reason_struct": json.dumps(reason_payload, ensure_ascii=False),
                    "risk_tags": ",".join(candidate.get("risk_tags", [])),
                }
            )
    return rows


def _ensure_schema(engine) -> None:
    """一次性执行 schema migration（模块级 flag 控制）"""
    global _schema_migrated
    if _schema_migrated:
        return
    _ensure_strategy_signals_columns(engine)
    _ensure_signal_audit_table(engine)
    _ensure_signal_unique_key(engine)
    _schema_migrated = True


def _ensure_strategy_signals_columns(engine) -> None:
    required_columns = {
        "triggered_factors": "TEXT",
        "score_detail": "JSON",
        "reason_text": "TEXT",
        "reason_struct": "JSON",
        "risk_tags": "VARCHAR(255)",
        "signal_uid": "VARCHAR(64)",
        "status_updated_at": "TIMESTAMP NULL",
        "status_updated_by": "VARCHAR(64)",
    }

    with engine.connect() as conn:
        existing = conn.execute(text("SHOW COLUMNS FROM strategy_signals")).fetchall()
        existing_names = {row[0] for row in existing}

        for column_name, column_type in required_columns.items():
            if column_name in existing_names:
                continue
            conn.execute(text(f"ALTER TABLE strategy_signals ADD COLUMN {column_name} {column_type}"))
        conn.commit()


def _ensure_signal_unique_key(engine) -> None:
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                UPDATE strategy_signals
                SET signal_uid = CONCAT(signal_date, '|', ts_code, '|', COALESCE(strategy_type, 'UNKNOWN'))
                WHERE signal_uid IS NULL OR signal_uid = ''
                """
            )
        )
        conn.execute(
            text(
                """
                DELETE s1
                FROM strategy_signals s1
                INNER JOIN strategy_signals s2
                  ON s1.signal_uid = s2.signal_uid
                 AND s1.id < s2.id
                """
            )
        )

        indexes = conn.execute(text("SHOW INDEX FROM strategy_signals WHERE Key_name = 'ux_signal_uid'"))
        if not indexes.fetchone():
            conn.execute(text("CREATE UNIQUE INDEX ux_signal_uid ON strategy_signals (signal_uid)"))
        conn.commit()


def _ensure_signal_audit_table(engine) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS strategy_signal_audit (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        signal_date VARCHAR(8) NOT NULL,
        ts_code VARCHAR(10) NOT NULL,
        old_status VARCHAR(20),
        new_status VARCHAR(20) NOT NULL,
        operator VARCHAR(64),
        note TEXT,
        changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_signal (signal_date, ts_code),
        KEY idx_changed_at (changed_at)
    )
    """
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()


def normalize_ts_codes(ts_codes: str) -> List[str]:
    if not ts_codes:
        return []
    parts = [p.strip() for p in ts_codes.split(",") if p.strip()]
    dedup = []
    seen = set()
    for p in parts:
        if p in seen:
            continue
        seen.add(p)
        dedup.append(p)
    return dedup


def persist_strategy_signals(ranked_candidates: List[Dict], trade_date: str, top_n: int = 15) -> int:
    rows = _build_signal_rows(ranked_candidates, trade_date, top_n=top_n)
    if not rows:
        return 0

    engine = get_engine("db3")
    _ensure_schema(engine)

    with engine.connect() as conn:
        conn.execute(text("DELETE FROM strategy_signals WHERE signal_date = :signal_date"), {"signal_date": trade_date})
        conn.commit()

    df = pd.DataFrame(rows)
    df.to_sql("strategy_signals", engine, if_exists="append", index=False, method="multi", chunksize=200)
    return len(df)


def can_transition_status(old_status: str, new_status: str) -> bool:
    if old_status not in VALID_SIGNAL_STATUSES or new_status not in VALID_SIGNAL_STATUSES:
        return False
    return new_status in ALLOWED_TRANSITIONS.get(old_status, set())


def get_signal_status(signal_date: str, ts_code: str) -> Optional[str]:
    engine = get_engine("db3")
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT status
                FROM strategy_signals
                WHERE signal_date = :signal_date
                  AND ts_code = :ts_code
                ORDER BY id DESC
                LIMIT 1
                """
            ),
            {"signal_date": signal_date, "ts_code": ts_code},
        ).fetchone()
    if not row:
        return None
    return row[0]


def update_signal_status(
    signal_date: str,
    ts_code: str,
    new_status: str,
    expected_current_status: Optional[str] = None,
    operator: str = "system",
    note: Optional[str] = None,
) -> bool:
    ok, _ = update_signal_status_with_reason(
        signal_date=signal_date,
        ts_code=ts_code,
        new_status=new_status,
        expected_current_status=expected_current_status,
        operator=operator,
        note=note,
    )
    return ok


def update_signal_status_with_reason(
    signal_date: str,
    ts_code: str,
    new_status: str,
    expected_current_status: Optional[str] = None,
    operator: str = "system",
    note: Optional[str] = None,
) -> tuple[bool, str]:
    if new_status not in VALID_SIGNAL_STATUSES:
        return False, "INVALID_TARGET_STATUS"

    engine = get_engine("db3")
    _ensure_schema(engine)

    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, status
                FROM strategy_signals
                WHERE signal_date = :signal_date
                  AND ts_code = :ts_code
                ORDER BY id DESC
                LIMIT 1
                """
            ),
            {"signal_date": signal_date, "ts_code": ts_code},
        ).fetchone()

        if not row:
            return False, "NOT_FOUND"

        row_id, current_status = row[0], row[1]

        if expected_current_status and current_status != expected_current_status:
            return False, "STATUS_MISMATCH"

        if current_status == new_status:
            return True, "NOOP"

        if not can_transition_status(current_status, new_status):
            return False, "INVALID_TRANSITION"

        conn.execute(
            text(
                """
                UPDATE strategy_signals
                SET status = :new_status,
                    status_updated_at = CURRENT_TIMESTAMP,
                    status_updated_by = :operator
                WHERE id = :row_id
                """
            ),
            {"new_status": new_status, "operator": operator, "row_id": row_id},
        )
        conn.execute(
            text(
                """
                INSERT INTO strategy_signal_audit
                (signal_date, ts_code, old_status, new_status, operator, note)
                VALUES
                (:signal_date, :ts_code, :old_status, :new_status, :operator, :note)
                """
            ),
            {
                "signal_date": signal_date,
                "ts_code": ts_code,
                "old_status": current_status,
                "new_status": new_status,
                "operator": operator,
                "note": note,
            },
        )
        conn.commit()
        return True, "UPDATED"


def update_signal_status_batch(
    signal_date: str,
    ts_codes: List[str],
    new_status: str,
    expected_current_status: Optional[str] = None,
    operator: str = "system",
    note: Optional[str] = None,
) -> Dict[str, object]:
    if new_status not in VALID_SIGNAL_STATUSES:
        return {
            "total": len(ts_codes),
            "success_count": 0,
            "failed_count": len(ts_codes),
            "success_ts_codes": [],
            "failed_ts_codes": list(ts_codes),
            "failed_reasons": {c: "INVALID_TARGET_STATUS" for c in ts_codes},
            "failed_reason_counts": {"INVALID_TARGET_STATUS": len(ts_codes)},
        }

    engine = get_engine("db3")
    _ensure_schema(engine)

    success: List[str] = []
    failed: List[str] = []
    failed_reasons: Dict[str, str] = {}
    reason_counts: Dict[str, int] = {}

    with engine.connect() as conn:
        # 一次查出所有信号的当前状态
        if not ts_codes:
            return {"total": 0, "success_count": 0, "failed_count": 0,
                    "success_ts_codes": [], "failed_ts_codes": [],
                    "failed_reasons": {}, "failed_reason_counts": {}}

        placeholders = ",".join([f":c{i}" for i in range(len(ts_codes))])
        params = {f"c{i}": c for i, c in enumerate(ts_codes)}
        params["signal_date"] = signal_date
        rows = conn.execute(
            text(
                f"""
                SELECT ts_code, id, status
                FROM strategy_signals
                WHERE signal_date = :signal_date
                  AND ts_code IN ({placeholders})
                ORDER BY id DESC
                """
            ),
            params,
        ).fetchall()

        # 取每个 ts_code 最新的一行
        latest_by_code: Dict[str, tuple] = {}
        for r in rows:
            if r[0] not in latest_by_code:
                latest_by_code[r[0]] = (r[1], r[2])  # (id, status)

        audit_rows = []
        update_ids = []

        for ts_code in ts_codes:
            if ts_code not in latest_by_code:
                failed.append(ts_code)
                failed_reasons[ts_code] = "NOT_FOUND"
                reason_counts["NOT_FOUND"] = reason_counts.get("NOT_FOUND", 0) + 1
                continue

            row_id, current_status = latest_by_code[ts_code]

            if expected_current_status and current_status != expected_current_status:
                failed.append(ts_code)
                failed_reasons[ts_code] = "STATUS_MISMATCH"
                reason_counts["STATUS_MISMATCH"] = reason_counts.get("STATUS_MISMATCH", 0) + 1
                continue

            if current_status == new_status:
                success.append(ts_code)
                continue

            if not can_transition_status(current_status, new_status):
                failed.append(ts_code)
                failed_reasons[ts_code] = "INVALID_TRANSITION"
                reason_counts["INVALID_TRANSITION"] = reason_counts.get("INVALID_TRANSITION", 0) + 1
                continue

            update_ids.append(row_id)
            audit_rows.append({
                "signal_date": signal_date,
                "ts_code": ts_code,
                "old_status": current_status,
                "new_status": new_status,
                "operator": operator,
                "note": note,
            })
            success.append(ts_code)

        # 批量 UPDATE
        if update_ids:
            ph = ",".join([f":id{i}" for i in range(len(update_ids))])
            up_params = {f"id{i}": uid for i, uid in enumerate(update_ids)}
            up_params["new_status"] = new_status
            up_params["operator"] = operator
            conn.execute(
                text(
                    f"""
                    UPDATE strategy_signals
                    SET status = :new_status,
                        status_updated_at = CURRENT_TIMESTAMP,
                        status_updated_by = :operator
                    WHERE id IN ({ph})
                    """
                ),
                up_params,
            )

        # 批量 INSERT 审计
        for ar in audit_rows:
            conn.execute(
                text(
                    """
                    INSERT INTO strategy_signal_audit
                    (signal_date, ts_code, old_status, new_status, operator, note)
                    VALUES (:signal_date, :ts_code, :old_status, :new_status, :operator, :note)
                    """
                ),
                ar,
            )

        conn.commit()

    return {
        "total": len(ts_codes),
        "success_count": len(success),
        "failed_count": len(failed),
        "success_ts_codes": success,
        "failed_ts_codes": failed,
        "failed_reasons": failed_reasons,
        "failed_reason_counts": reason_counts,
    }
