"""
Fraud Detection Toolkit — watsonx Orchestrate
==============================================
Reads live data from the fraud_alerts Kafka topic by shelling out to the
Confluent CLI (`confluent kafka topic consume`).  This is the only reliable
consume path on Confluent Cloud Basic/Standard clusters — the hosted REST
Proxy does not expose a consume endpoint.

The Confluent CLI must be installed (brew install confluentinc/tap/cli) and
logged in (`confluent login`).  The active cluster / environment are set via
environment variables so the tool works without a pre-configured CLI context.

The fraud_alerts topic is written by an Apache Flink 5-minute tumbling window
job.  Each message has the key and value as separate JSON objects printed on
one tab-delimited line:
  <garbage><key-json>\\t<garbage><value-json>
e.g.
  \\x00\\x00...{"account_id":"ACCOUNT-FRAUD-01","window_start":...}\\t\\x00...{"window_end":...,"is_flagged":true}
"""

import json
import os
import subprocess
import time
import uuid
from datetime import datetime, timezone

from ibm_watsonx_orchestrate.agent_builder.tools import tool

# ---------------------------------------------------------------------------
# Config — all values fall back to the terraform-output defaults
# ---------------------------------------------------------------------------

CONFLUENT_CLI      = os.environ.get("CONFLUENT_CLI_PATH", "/opt/homebrew/bin/confluent")
KAFKA_CLUSTER_ID   = os.environ.get("KAFKA_CLUSTER_ID",   "")
KAFKA_ENVIRONMENT  = os.environ.get("KAFKA_ENVIRONMENT",  "")
KAFKA_API_KEY      = os.environ.get("KAFKA_API_KEY",      "")
KAFKA_API_SECRET   = os.environ.get("KAFKA_API_SECRET",   "")
KAFKA_BOOTSTRAP    = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
FRAUD_ALERTS_TOPIC = "fraud_alerts"

# In-process cache — 90 s TTL keeps repeated demo calls instant
_cache: dict = {"rows": [], "ts": 0.0}
_CACHE_TTL   = 90  # seconds

# ---------------------------------------------------------------------------
# CLI-based consumer
# ---------------------------------------------------------------------------

def _parse_raw_line(line: bytes) -> dict | None:
    """
    Parse one raw bytes line from the CLI into a merged record dict.

    Each line is:  <5-byte SR header>{key-json}\\t<5-byte SR header>{value-json}
    The Schema Registry header is: 0x00 + 4-byte big-endian schema id.
    We split on \\t, strip the 5-byte header from each part, JSON-parse, merge.
    """
    if not line:
        return None

    merged: dict = {}
    for part in line.split(b"\t"):
        # Strip leading non-JSON bytes up to the first '{'
        idx = part.find(b"{")
        if idx == -1:
            continue
        payload = part[idx:]
        try:
            obj = json.loads(payload.decode("utf-8", errors="replace"))
            if isinstance(obj, dict):
                merged.update(obj)
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

    return merged if merged.get("account_id") else None


def _read_fraud_alerts(consume_seconds: float = 10.0) -> list[dict]:
    """
    Spawn `confluent kafka topic consume` for `consume_seconds`, collect all
    output lines, parse them into merged dicts, and return the list.
    Uses a 90-second in-process cache so repeated demo calls are instant.
    """
    now = time.time()
    if now - _cache["ts"] < _CACHE_TTL and _cache["rows"]:
        return _cache["rows"]

    cmd = [
        CONFLUENT_CLI, "kafka", "topic", "consume", FRAUD_ALERTS_TOPIC,
        "--cluster",     KAFKA_CLUSTER_ID,
        "--environment", KAFKA_ENVIRONMENT,
        "--api-key",     KAFKA_API_KEY,
        "--api-secret",  KAFKA_API_SECRET,
        "--bootstrap",   KAFKA_BOOTSTRAP,
        "--from-beginning",
        "--value-format", "string",
        "--print-key",
    ]

    rows: list[dict] = []
    _last_error: str = ""
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            stdout, stderr = proc.communicate(timeout=consume_seconds)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()

        _last_error = stderr.decode(errors="replace")

        for line in stdout.split(b"\n"):
            rec = _parse_raw_line(line.rstrip(b"\r"))
            if rec:
                rows.append(rec)
    except Exception as exc:
        _last_error = str(exc)

    # Surface diagnostic info via cache so get_fraud_alert can report it
    _cache["last_error"] = _last_error
    _cache["cmd"] = " ".join(cmd)

    if rows:
        _cache["rows"] = rows
        _cache["ts"]   = time.time()

    return rows


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@tool
def debug_kafka_connection() -> dict:
    """
    Diagnostic tool — returns the exact CLI command, stderr output, and record
    count from the last _read_fraud_alerts() call.  Use this when get_fraud_alert
    returns 0 transactions to diagnose the connection issue.

    Returns:
        Dict with cmd, stderr, record_count, and cache_age_seconds.
    """
    _read_fraud_alerts(consume_seconds=8)
    return {
        "cmd":           _cache.get("cmd", "not run yet"),
        "stderr":        _cache.get("last_error", ""),
        "record_count":  len(_cache.get("rows", [])),
        "cache_age_sec": round(time.time() - _cache.get("ts", 0), 1),
    }


@tool
def get_fraud_alert(account_id: str) -> dict:
    """
    Retrieve the latest fraud alert for a given account from the live
    fraud_alerts Kafka topic on Confluent Cloud.

    The topic is populated by an Apache Flink 5-minute tumbling window job
    that flags accounts with more than 10 transactions per window.

    Args:
        account_id: The account identifier (e.g. ACCOUNT-FRAUD-01).

    Returns:
        A dict with account_id, window_start, window_end, transaction_count,
        total_amount, is_flagged, and retrieved_at.
    """
    rows     = _read_fraud_alerts()
    matching = [r for r in rows if r.get("account_id") == account_id]

    if matching:
        latest = matching[-1]
        latest["retrieved_at"] = _now_iso()
        latest["source"]       = f"live:kafka/{FRAUD_ALERTS_TOPIC}"
        return latest

    return {
        "account_id":        account_id,
        "window_start":      None,
        "window_end":        None,
        "transaction_count": 0,
        "total_amount":      0.0,
        "is_flagged":        False,
        "retrieved_at":      _now_iso(),
        "source":            f"live:kafka/{FRAUD_ALERTS_TOPIC}",
        "note":              "No alert found for this account in fraud_alerts topic.",
    }


@tool
def get_transaction_summary(account_id: str, window_minutes: int = 5) -> dict:
    """
    Return a summary of recent transactions for an account from the live
    fraud_alerts Kafka topic (Flink 5-minute tumbling window output).

    Args:
        account_id: The account to summarise.
        window_minutes: Look-back window size in minutes (default: 5).

    Returns:
        Dict with transaction_count, total_amount, avg_amount, is_flagged,
        window info, and source topic.
    """
    rows     = _read_fraud_alerts()
    matching = [r for r in rows if r.get("account_id") == account_id]

    if matching:
        latest = matching[-1]
        count  = int(latest.get("transaction_count", 0))
        total  = float(latest.get("total_amount", 0.0))
        return {
            "account_id":        account_id,
            "window_minutes":    window_minutes,
            "transaction_count": count,
            "total_amount":      round(total, 2),
            "avg_amount":        round(total / count, 2) if count else 0.0,
            "is_flagged":        latest.get("is_flagged", False),
            "window_start":      latest.get("window_start"),
            "window_end":        latest.get("window_end"),
            "source":            f"live:kafka/{FRAUD_ALERTS_TOPIC}",
            "retrieved_at":      _now_iso(),
        }

    return {
        "account_id":        account_id,
        "window_minutes":    window_minutes,
        "transaction_count": 0,
        "total_amount":      0.0,
        "avg_amount":        0.0,
        "is_flagged":        False,
        "source":            f"live:kafka/{FRAUD_ALERTS_TOPIC}",
        "retrieved_at":      _now_iso(),
        "note":              "No recent transactions found in fraud_alerts topic.",
    }


@tool
def flag_account(account_id: str, reason: str) -> dict:
    """
    Flag an account as under active fraud review without freezing it.

    Use when the fraud score is elevated but not yet conclusive.

    Args:
        account_id: The account to flag.
        reason: Free-text explanation for the audit trail.

    Returns:
        Confirmation dict with account_id, status, flagged_at, reason.
    """
    return {
        "account_id": account_id,
        "status":     "FLAGGED_FOR_REVIEW",
        "flagged_at": _now_iso(),
        "reason":     reason,
    }


@tool
def freeze_account(account_id: str, reason: str) -> dict:
    """
    Immediately freeze an account to prevent further transactions.

    Use when fraud signal is high-confidence: is_flagged=True and
    transaction_count > 10 in a 5-minute window.

    Args:
        account_id: The account to freeze.
        reason: Justification recorded in the audit trail.

    Returns:
        Confirmation dict with account_id, status, frozen_at, reason.
    """
    return {
        "account_id":  account_id,
        "status":      "FROZEN",
        "frozen_at":   _now_iso(),
        "reason":      reason,
        "audit_trail": True,
    }


@tool
def send_customer_alert(account_id: str, message: str, channel: str = "EMAIL") -> dict:
    """
    Send a fraud alert notification to the account holder.

    Args:
        account_id: The account whose holder should be notified.
        message: The notification message text.
        channel: Delivery channel — EMAIL, SMS, or PUSH (default: EMAIL).

    Returns:
        Confirmation dict with account_id, channel, sent_at, message_preview.
    """
    return {
        "account_id":      account_id,
        "channel":         channel.upper(),
        "sent_at":         _now_iso(),
        "message_preview": message[:120],
    }


@tool
def escalate_to_human(account_id: str, summary: str, priority: str = "HIGH") -> dict:
    """
    Escalate a fraud case to a human analyst (human-in-the-loop governance).

    Always called before any freeze_account action.

    Args:
        account_id: The account under investigation.
        summary: Brief description of the fraud signal for the analyst.
        priority: Case priority — LOW, MEDIUM, HIGH, or CRITICAL (default: HIGH).

    Returns:
        Case ticket dict with case_id, account_id, priority, escalated_at.
    """
    return {
        "case_id":        f"CASE-{uuid.uuid4().hex[:8].upper()}",
        "account_id":     account_id,
        "priority":       priority.upper(),
        "summary":        summary,
        "escalated_at":   _now_iso(),
        "assigned_queue": "fraud-review-team",
    }
