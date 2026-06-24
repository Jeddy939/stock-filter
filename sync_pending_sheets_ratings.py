"""Retry queued MoneyMaker rating events that failed to send to Google Sheets."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List
from urllib.request import Request, urlopen


DEFAULT_PENDING_FILE = "ratings/google_sheets_pending_ratings.jsonl"


def post_event(webhook_url: str, event: Dict[str, Any], secret: str = "") -> None:
    payload = dict(event)
    payload.pop("queued_at_utc", None)
    payload.pop("queue_error", None)
    if secret:
        payload["secret"] = secret
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = Request(webhook_url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(request, timeout=15) as response:
        body = response.read().decode("utf-8", errors="replace")
        if response.status >= 400:
            raise RuntimeError(f"HTTP {response.status}: {body[:500]}")
        result = json.loads(body) if body else {}
        if isinstance(result, dict) and result.get("ok") is False:
            raise RuntimeError(str(result.get("error") or result))


def load_pending(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Retry queued Google Sheets rating events.")
    parser.add_argument("--pending-file", default=os.environ.get("MONEYMAKER_GOOGLE_SHEETS_PENDING_FILE", DEFAULT_PENDING_FILE))
    parser.add_argument("--webhook-url", default=os.environ.get("MONEYMAKER_GOOGLE_SHEETS_WEBHOOK_URL") or os.environ.get("MONEYMAKER_GOOGLE_SHEETS_WEBHOOK") or "")
    parser.add_argument("--secret", default=os.environ.get("MONEYMAKER_GOOGLE_SHEETS_SECRET", ""))
    args = parser.parse_args()

    if not args.webhook_url:
        raise SystemExit("Google Sheets webhook is not configured. Run CONFIGURE_GOOGLE_SHEETS_RATINGS.bat first.")

    pending_path = Path(args.pending_file)
    pending = load_pending(pending_path)
    if not pending:
        print("No pending Google Sheets ratings to sync.")
        return

    remaining = []
    sent = 0
    for event in pending:
        try:
            post_event(args.webhook_url, event, args.secret)
            sent += 1
        except Exception as exc:
            event["queue_error"] = str(exc)[:1000]
            remaining.append(event)

    if remaining:
        pending_path.parent.mkdir(parents=True, exist_ok=True)
        pending_path.write_text(
            "".join(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n" for event in remaining),
            encoding="utf-8",
        )
    else:
        pending_path.unlink(missing_ok=True)

    print(f"Sent {sent} pending rating events.")
    print(f"Remaining queued events: {len(remaining)}")


if __name__ == "__main__":
    main()
