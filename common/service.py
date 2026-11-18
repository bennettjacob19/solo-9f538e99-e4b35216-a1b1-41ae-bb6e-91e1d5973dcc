"""Core alerting logic: event ingestion, silences and the notification outbox."""
from __future__ import annotations

import sqlite3
import time
from typing import Any, Callable, Optional

from .db import transaction

VALID_KINDS = ("firing", "recovery")


class Reject(Exception):
    """A request that violates the ordering/idempotency rules (HTTP 409)."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #
def validate_event(payload: Any) -> dict:
    if not isinstance(payload, dict):
        raise Reject("invalid_payload", "event body must be a JSON object")
    fingerprint = payload.get("fingerprint")
    if not isinstance(fingerprint, str) or not fingerprint.strip():
        raise Reject("invalid_payload", "fingerprint must be a non-empty string")
    seq = payload.get("seq")
    if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
        raise Reject("invalid_payload", "seq must be a non-negative integer")
    kind = payload.get("kind")
    if kind not in VALID_KINDS:
        raise Reject("invalid_payload", "kind must be 'firing' or 'recovery'")
    summary = payload.get("summary")
    if not isinstance(summary, str) or not summary.strip():
        raise Reject("invalid_payload", "summary must be a non-empty string")
    return {"fingerprint": fingerprint, "seq": seq, "kind": kind, "summary": summary}


def notification_no(fingerprint: str, round_no: int, kind: str) -> str:
    # Stable and human-readable: retries reuse the exact same number, and the
    # unique index on (fingerprint, round, kind) guarantees global uniqueness.
    return f"{fingerprint}#r{round_no}:{kind}"


# --------------------------------------------------------------------------- #
# Event ingestion
# --------------------------------------------------------------------------- #
def ingest_event(conn: sqlite3.Connection, payload: dict, now: Optional[float] = None) -> dict:
    """Apply one trigger/recovery event.

    Returns a result dictionary. Raises Reject for stale/conflicting events.
    Same (fingerprint, seq, body) replays the original result; same seq with a
    different body is rejected; an older seq can never rewrite current state.
    """
    event = validate_event(payload)
    now = time.time() if now is None else now
    fp, seq, kind, summary = event["fingerprint"], event["seq"], event["kind"], event["summary"]

    with transaction(conn):
        existing = conn.execute(
            "SELECT kind, summary FROM events WHERE fingerprint=? AND seq=?", (fp, seq)
        ).fetchone()
        if existing is not None:
            if existing["kind"] != kind or existing["summary"] != summary:
                raise Reject(
                    "conflicting_event",
                    f"event {fp} seq={seq} was already recorded with a different body",
                )
            # Identical retry: the original application stands, no state change.
            return {"result": "duplicate_replayed", "event": event, "alert": _get_alert(conn, fp)}

        alert_row = conn.execute(
            "SELECT * FROM alerts WHERE fingerprint=?", (fp,)
        ).fetchone()

        if alert_row is None:
            if kind == "recovery":
                raise Reject("stale_event", "recovery event has no active alert")
            round_no = 1
            conn.execute(
                "INSERT INTO alerts(fingerprint, current_seq, status, summary, round, "
                "last_event_at, updated_at) VALUES (?,?, 'firing', ?, ?, ?, ?)",
                (fp, seq, summary, round_no, now, now),
            )
        else:
            if seq <= alert_row["current_seq"]:
                raise Reject(
                    "stale_event",
                    f"seq={seq} is not newer than current seq={alert_row['current_seq']}",
                )
            if kind == "firing":
                if alert_row["status"] == "recovered":
                    # Re-trigger after recovery starts a brand-new round.
                    round_no = alert_row["round"] + 1
                    conn.execute(
                        "UPDATE alerts SET status='firing', round=?, summary=?, "
                        "current_seq=?, last_event_at=?, updated_at=? WHERE fingerprint=?",
                        (round_no, summary, seq, now, now, fp),
                    )
                else:
                    # Newer trigger while still firing: same round, text refresh only.
                    round_no = alert_row["round"]
                    conn.execute(
                        "UPDATE alerts SET summary=?, current_seq=?, last_event_at=?, "
                        "updated_at=? WHERE fingerprint=?",
                        (summary, seq, now, now, fp),
                    )
            else:  # recovery
                round_no = alert_row["round"]
                conn.execute(
                    "UPDATE alerts SET status='recovered', summary=?, current_seq=?, "
                    "last_event_at=?, updated_at=? WHERE fingerprint=?",
                    (summary, seq, now, now, fp),
                )

        conn.execute(
            "INSERT INTO events(fingerprint, seq, kind, summary, received_at) "
            "VALUES (?,?,?,?,?)",
            (fp, seq, kind, summary, now),
        )

        notifications = _apply_notification_policy(conn, fp, round_no, kind, summary, now)
        alert = _get_alert(conn, fp)

    return {
        "result": "accepted",
        "event": event,
        "alert": alert,
        "notifications_enqueued": notifications,
    }


def _silence_active(conn: sqlite3.Connection, now: float) -> bool:
    row = conn.execute("SELECT until_ts FROM silences WHERE id=1").fetchone()
    return row is not None and row["until_ts"] > now


def _enqueue_notification(
    conn: sqlite3.Connection, fp: str, round_no: int, kind: str, summary: str, now: float
) -> bool:
    """Insert an outbox row. Returns False if an identical one already exists."""
    no = notification_no(fp, round_no, kind)
    cur = conn.execute(
        "INSERT OR IGNORE INTO notifications(notification_no, fingerprint, round, kind, "
        "summary, status, next_retry_at, enqueued_at) VALUES (?,?,?,?,?, 'pending', ?, ?)",
        (no, fp, round_no, kind, summary, now, now),
    )
    return cur.rowcount > 0


def _apply_notification_policy(
    conn: sqlite3.Connection, fp: str, round_no: int, kind: str, summary: str, now: float
) -> list[dict]:
    """Decide which notifications a newly applied event produces."""
    enqueued: list[dict] = []
    silenced = _silence_active(conn, now)

    if kind == "firing":
        # During a silence a new trigger only updates the alert; it is parked in
        # the outbox later when the silence expires (see sweep_silences).
        if not silenced:
            if _enqueue_notification(conn, fp, round_no, "firing", summary, now):
                enqueued.append(
                    {"notification_no": notification_no(fp, round_no, "firing"),
                     "kind": "firing"}
                )
    else:  # recovery
        # If the fault was ever announced (a firing notification exists for this
        # round, even if delivery is still pending), its recovery MUST be sent —
        # silence does not suppress it. The outbox worker preserves order.
        firing_row = conn.execute(
            "SELECT 1 FROM notifications WHERE fingerprint=? AND round=? AND kind='firing'",
            (fp, round_no),
        ).fetchone()
        if firing_row is not None:
            if _enqueue_notification(conn, fp, round_no, "recovery", summary, now):
                enqueued.append(
                    {"notification_no": notification_no(fp, round_no, "recovery"),
                     "kind": "recovery"}
                )
    return enqueued


def _get_alert(conn: sqlite3.Connection, fp: str) -> Optional[dict]:
    row = conn.execute("SELECT * FROM alerts WHERE fingerprint=?", (fp,)).fetchone()
    return dict(row) if row else None


# --------------------------------------------------------------------------- #
# Silence management
# --------------------------------------------------------------------------- #
def set_silence(
    conn: sqlite3.Connection, until_ts: float, reason: str, now: Optional[float] = None
) -> dict:
    now = time.time() if now is None else now
    with transaction(conn):
        conn.execute(
            "INSERT INTO silences(id, until_ts, reason, updated_at) VALUES (1, ?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET until_ts=excluded.until_ts, "
            "reason=excluded.reason, updated_at=excluded.updated_at",
            (until_ts, reason, now),
        )
        # Record the *effective* window history. An active silence is simply
        # extended (same open interval); an expired/absent one opens a new row.
        open_row = conn.execute(
            "SELECT id FROM silence_intervals WHERE ended_at IS NULL AND until_ts > ?",
            (now,),
        ).fetchone()
        if open_row is not None:
            conn.execute(
                "UPDATE silence_intervals SET until_ts=?, reason=?, updated_at=? WHERE id=?",
                (until_ts, reason, now, open_row["id"]),
            )
        else:
            conn.execute(
                "INSERT INTO silence_intervals(started_at, until_ts, ended_at, reason, "
                "updated_at) VALUES (?,?, NULL, ?, ?)",
                (now, until_ts, reason, now),
            )
    return get_silence(conn, now)


def clear_silence(conn: sqlite3.Connection, now: Optional[float] = None) -> dict:
    now = time.time() if now is None else now
    with transaction(conn):
        conn.execute("DELETE FROM silences WHERE id=1")
        # Early release: close the still-active interval at the release time.
        # Expired open rows are left alone — their effective end is until_ts.
        conn.execute(
            "UPDATE silence_intervals SET ended_at=?, updated_at=? "
            "WHERE ended_at IS NULL AND until_ts > ?",
            (now, now, now),
        )
    return {"active": False, "until_ts": None, "reason": None, "now": now}


def get_silence(conn: sqlite3.Connection, now: Optional[float] = None) -> dict:
    now = time.time() if now is None else now
    row = conn.execute("SELECT * FROM silences WHERE id=1").fetchone()
    if row is None or row["until_ts"] <= now:
        return {"active": False, "until_ts": None, "reason": None, "now": now}
    return {
        "active": True,
        "until_ts": row["until_ts"],
        "reason": row["reason"],
        "now": now,
        "remaining_seconds": round(row["until_ts"] - now, 3),
    }


def sweep_silences(conn: sqlite3.Connection, now: Optional[float] = None) -> list[dict]:
    """Expire silences: re-issue parked firing notifications.

    When the silence window ends, every alert that is *still firing* and whose
    current round was never announced gets its firing notification enqueued.
    Alerts that already recovered stay silent for that round.
    """
    now = time.time() if now is None else now
    released: list[dict] = []
    with transaction(conn):
        if _silence_active(conn, now):
            return released
        rows = conn.execute(
            "SELECT a.fingerprint, a.round, a.summary FROM alerts a "
            "WHERE a.status='firing'"
        ).fetchall()
        for row in rows:
            exists = conn.execute(
                "SELECT 1 FROM notifications WHERE fingerprint=? AND round=? AND kind='firing'",
                (row["fingerprint"], row["round"]),
            ).fetchone()
            if exists is None:
                if _enqueue_notification(
                    conn, row["fingerprint"], row["round"], "firing", row["summary"], now
                ):
                    released.append(
                        {"fingerprint": row["fingerprint"], "round": row["round"]}
                    )
    return released


# --------------------------------------------------------------------------- #
# Reads
# --------------------------------------------------------------------------- #
def list_alerts(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """
        SELECT a.*,
               fn.status AS firing_notify_status,
               fn.notification_no AS firing_notification_no,
               rn.status AS recovery_notify_status,
               rn.notification_no AS recovery_notification_no
        FROM alerts a
        LEFT JOIN notifications fn
            ON fn.fingerprint=a.fingerprint AND fn.round=a.round AND fn.kind='firing'
        LEFT JOIN notifications rn
            ON rn.fingerprint=a.fingerprint AND rn.round=a.round AND rn.kind='recovery'
        ORDER BY a.updated_at DESC
        """
    ).fetchall()
    return [dict(r) for r in rows]


def list_notifications(conn: sqlite3.Connection, limit: int = 200) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM notifications ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


def list_events(conn: sqlite3.Connection, fingerprint: str, limit: int = 100) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM events WHERE fingerprint=? ORDER BY seq DESC LIMIT ?",
        (fingerprint, limit),
    ).fetchall()
    return [dict(r) for r in rows]


# --------------------------------------------------------------------------- #
# Downtime budget accounting
# --------------------------------------------------------------------------- #
WINDOW_SECONDS = 24 * 3600.0


def _effective_silence_intervals(
    conn: sqlite3.Connection, lo: float, hi: float, now: float
) -> list[tuple[float, float]]:
    """Committed silence windows actually in effect, clipped to [lo, hi].

    An open row whose until_ts has already passed ends at until_ts (natural
    expiry), so accounting is correct even before the worker sweep closes it.
    """
    rows = conn.execute(
        "SELECT started_at, until_ts, ended_at FROM silence_intervals "
        "WHERE started_at < ? AND (ended_at IS NULL OR ended_at > ?) "
        "ORDER BY started_at",
        (hi, lo),
    ).fetchall()
    intervals: list[tuple[float, float]] = []
    for r in rows:
        start = r["started_at"]
        end = r["ended_at"] if r["ended_at"] is not None else min(r["until_ts"], now)
        if end <= start:
            continue
        start, end = max(start, lo), min(end, hi)
        if end > start:
            intervals.append((start, end))
    return _merge_intervals(intervals)


def _fault_intervals(conn: sqlite3.Connection, fp: str) -> list[tuple[float, float, float]]:
    """Reconstruct fault rounds from the immutable event log.

    Returns (firing_received_at, recovery_received_at_or_None, round_no)
    tuples. A firing without a following recovery is still open. Timing uses
    server-side receive time, never any timestamp supplied by the client.
    """
    rows = conn.execute(
        "SELECT kind, received_at FROM events WHERE fingerprint=? ORDER BY seq",
        (fp,),
    ).fetchall()
    faults: list[tuple[float, float, float]] = []
    opened: Optional[float] = None
    round_no = 0
    for r in rows:
        if r["kind"] == "firing":
            if opened is None:
                round_no += 1
                opened = r["received_at"]
        elif opened is not None:  # recovery closes the currently open round
            faults.append((opened, r["received_at"], round_no))
            opened = None
    if opened is not None:
        faults.append((opened, None, round_no))  # still firing: counted up to query time
    return faults


def _merge_intervals(intervals: list[tuple[float, float]]) -> list[tuple[float, float]]:
    merged: list[list[float]] = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1]:
            if end > merged[-1][1]:
                merged[-1][1] = end
        else:
            merged.append([start, end])
    return [(s, e) for s, e in merged]


def _covered_seconds(
    intervals: list[tuple[float, float]], cover: list[tuple[float, float]]
) -> float:
    """Total length of ``intervals`` that overlaps the (merged) cover set."""
    total = 0.0
    for start, end in intervals:
        for c_start, c_end in cover:
            if c_end <= start:
                continue
            if c_start >= end:
                break
            total += min(end, c_end) - max(start, c_start)
    return total


def compute_budget(
    conn: sqlite3.Connection, fp: str, now: Optional[float] = None
) -> Optional[dict]:
    """Public entry point: run the accounting inside one serialized transaction."""
    now = time.time() if now is None else now
    with transaction(conn):
        return _compute_budget(conn, fp, now)


def _compute_budget(
    conn: sqlite3.Connection, fp: str, now: float
) -> Optional[dict]:
    """Compute one fingerprint's downtime budget; caller owns the transaction.

    Counting starts at the budget's ``set_at`` (faults already firing then are
    counted from set_at) and never looks further back than 24 hours. Open
    faults are counted up to ``now``; time covered by an effective silence is
    exempt. Every number comes from the same connection/transaction so
    interleaved event ingestion or silence edits cannot mix two states.
    """
    brow = conn.execute(
        "SELECT * FROM budgets WHERE fingerprint=?", (fp,)
    ).fetchone()
    if brow is None:
        return None

    window_start = max(brow["set_at"], now - WINDOW_SECONDS)
    window_end = now
    silences = _effective_silence_intervals(conn, window_start, window_end, now)

    gross = 0.0
    exempt = 0.0
    rounds: list[dict] = []
    for fire_at, rec_at, round_no in _fault_intervals(conn, fp):
        # An open fault runs to query time; a closed one to its recovery.
        raw_end = window_end if rec_at is None else min(rec_at, window_end)
        start, end = max(fire_at, window_start), raw_end
        if end <= start:
            continue
        fault_piece = (start, end)
        piece_gross = end - start
        piece_exempt = _covered_seconds([fault_piece], silences)
        gross += piece_gross
        exempt += piece_exempt
        rounds.append({
            "round": round_no,
            "firing_at": round(fire_at, 3),
            "recovery_at": None if rec_at is None else round(rec_at, 3),
            "open": rec_at is None,
            "counted_seconds": round(piece_gross - piece_exempt, 3),
            "exempt_seconds": round(piece_exempt, 3),
        })

    counted = gross - exempt
    allowed = brow["allowed_seconds"]
    remaining = allowed - counted
    return {
        "fingerprint": fp,
        "allowed_seconds": round(allowed, 3),
        "set_at": brow["set_at"],
        "window_start": round(window_start, 3),
        "now": round(now, 3),
        "downtime_seconds": round(counted, 3),
        "exempt_seconds": round(exempt, 3),
        "remaining_seconds": round(remaining, 3),
        "exhausted": remaining <= 0,
        "rounds": rounds,
    }


def set_budget(
    conn: sqlite3.Connection,
    fingerprint: str,
    allowed_seconds: float,
    now: Optional[float] = None,
) -> dict:
    """Upsert a per-fingerprint downtime allowance; accounting restarts now."""
    if not isinstance(fingerprint, str) or not fingerprint.strip():
        raise Reject("invalid_payload", "fingerprint must be a non-empty string")
    if isinstance(allowed_seconds, bool) or not isinstance(allowed_seconds, (int, float)):
        raise Reject("invalid_payload", "allowed_seconds must be a non-negative number")
    if allowed_seconds < 0:
        raise Reject("invalid_payload", "allowed_seconds must be >= 0")
    now = time.time() if now is None else now
    with transaction(conn):
        conn.execute(
            "INSERT INTO budgets(fingerprint, allowed_seconds, set_at, updated_at) "
            "VALUES (?,?,?,?) ON CONFLICT(fingerprint) DO UPDATE SET "
            "allowed_seconds=excluded.allowed_seconds, set_at=excluded.set_at, "
            "updated_at=excluded.updated_at",
            (fingerprint, float(allowed_seconds), now, now),
        )
        result = _compute_budget(conn, fingerprint, now)
    assert result is not None
    return result


def list_budgets(conn: sqlite3.Connection, now: Optional[float] = None) -> list[dict]:
    now = time.time() if now is None else now
    with transaction(conn):
        fps = [r["fingerprint"] for r in conn.execute(
            "SELECT fingerprint FROM budgets ORDER BY fingerprint"
        ).fetchall()]
        results = [_compute_budget(conn, fp, now) for fp in fps]
    return [r for r in results if r is not None]


# --------------------------------------------------------------------------- #
# Outbox delivery worker
# --------------------------------------------------------------------------- #
SendFunc = Callable[[dict], tuple[bool, Optional[str]]]


def _backoff_seconds(attempts: int) -> float:
    return min(float(attempts), 30.0)


def dispatch_pending(
    conn: sqlite3.Connection, send: SendFunc, now: Optional[float] = None
) -> list[dict]:
    """Deliver due outbox rows, preserving per-fingerprint ordering.

    A notification is only sent when every earlier notification of the same
    fingerprint has already been delivered. Lost receipts keep the row in
    'in_transit' and the same notification number is retried later. HTTP calls
    happen *outside* write transactions so ingestion never blocks on the wire.
    """
    now = time.time() if now is None else now
    results: list[dict] = []

    while True:
        with transaction(conn):
            row = conn.execute(
                "SELECT * FROM notifications WHERE status IN ('pending','in_transit') "
                "AND next_retry_at<=? ORDER BY id",
                (now,),
            ).fetchone()
            if row is None:
                break
            blocker = conn.execute(
                "SELECT 1 FROM notifications WHERE fingerprint=? AND id<? AND "
                "status!='delivered' LIMIT 1",
                (row["fingerprint"], row["id"]),
            ).fetchone()
            if blocker is not None:
                # Head-of-line for this fingerprint: park it briefly; rows of
                # other fingerprints are picked up on the next worker tick.
                conn.execute(
                    "UPDATE notifications SET next_retry_at=? WHERE id=?",
                    (now + 0.2, row["id"]),
                )
                break
            conn.execute(
                "UPDATE notifications SET status='in_transit', attempts=attempts+1, "
                "last_error=NULL WHERE id=?",
                (row["id"],),
            )
            payload = {
                "notification_no": row["notification_no"],
                "fingerprint": row["fingerprint"],
                "round": row["round"],
                "kind": row["kind"],
                "summary": row["summary"],
            }

        # Network I/O happens outside any write transaction so event ingestion
        # never blocks waiting on the notification endpoint.
        ok, error = send(payload)

        with transaction(conn):
            if ok:
                conn.execute(
                    "UPDATE notifications SET status='delivered', delivered_at=?, "
                    "last_error=NULL WHERE id=?",
                    (now, row["id"]),
                )
                results.append({"notification_no": row["notification_no"], "delivered": True})
            else:
                retry_at = now + _backoff_seconds(row["attempts"] + 1)
                conn.execute(
                    "UPDATE notifications SET next_retry_at=?, last_error=? WHERE id=?",
                    (retry_at, error, row["id"]),
                )
                results.append(
                    {
                        "notification_no": row["notification_no"],
                        "delivered": False,
                        "error": error,
                        "next_retry_at": retry_at,
                    }
                )
    return results
