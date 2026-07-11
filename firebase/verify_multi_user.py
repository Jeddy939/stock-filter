"""Verify that two Firebase identities have isolated appraisals.

This uses temporary anonymous Firebase identities, so it does not create or
alter real email/password accounts. Set FIREBASE_API_KEY and optionally
MONEYMAKER_API_URL before running it.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request


API = os.environ.get("MONEYMAKER_API_URL", "https://moneymaker-api-u2hhhgjdmq-ts.a.run.app").rstrip("/")
KEY = os.environ.get("FIREBASE_API_KEY", "").strip()


def request_json(url: str, *, method: str = "GET", token: str | None = None,
                payload: dict | None = None) -> dict:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} returned {exc.code}: {detail}") from exc


def anonymous_token() -> str:
    if not KEY:
        raise RuntimeError("Set FIREBASE_API_KEY before running this test")
    result = request_json(
        f"https://identitytoolkit.googleapis.com/v1/accounts:signUp?key={urllib.parse.quote(KEY)}",
        method="POST",
        payload={"returnSecureToken": True},
    )
    return str(result["idToken"])


def labels(token: str, scan_id: int) -> dict[str, dict]:
    result = request_json(f"{API}/api/labels?scan_id={scan_id}", token=token)
    return {str(row["ticker"]): row for row in result.get("labels", [])}


def main() -> int:
    token_a = anonymous_token()
    token_b = anonymous_token()
    filtered = request_json(f"{API}/api/filter", method="POST", token=token_a, payload={})
    summary = filtered.get("summary") or {}
    scan_id = int(summary.get("scan_id") or 0)
    rows = filtered.get("results") or []
    if not scan_id or not rows:
        raise RuntimeError("No completed filter result with a ticker is available for isolation testing")
    ticker = str(rows[0]["ticker"])

    request_json(
        f"{API}/api/label", method="POST", token=token_a,
        payload={"scan_id": scan_id, "ticker": ticker, "label": "winner", "note": "user A private test"},
    )
    user_a = labels(token_a, scan_id)
    user_b = labels(token_b, scan_id)
    if user_a.get(ticker, {}).get("label") != "winner":
        raise RuntimeError("User A could not read its own appraisal")
    if ticker in user_b:
        raise RuntimeError("User B can see User A's appraisal")

    request_json(
        f"{API}/api/label", method="POST", token=token_b,
        payload={"scan_id": scan_id, "ticker": ticker, "label": "bad", "note": "user B private test"},
    )
    user_a = labels(token_a, scan_id)
    user_b = labels(token_b, scan_id)
    if user_a.get(ticker, {}).get("label") != "winner":
        raise RuntimeError("User B overwrote User A's appraisal")
    if user_b.get(ticker, {}).get("label") != "bad":
        raise RuntimeError("User B could not read its own appraisal")

    request_json(
        f"{API}/api/label", method="POST", token=token_a,
        payload={"scan_id": scan_id, "ticker": ticker, "label": "clear"},
    )
    if labels(token_b, scan_id).get(ticker, {}).get("label") != "bad":
        raise RuntimeError("Clearing User A's appraisal changed User B's appraisal")
    request_json(
        f"{API}/api/label", method="POST", token=token_b,
        payload={"scan_id": scan_id, "ticker": ticker, "label": "clear"},
    )
    print(f"PASS: isolated appraisals verified for scan {scan_id}, ticker {ticker}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        raise SystemExit(1)
