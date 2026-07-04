import json

import pytest

import web_app


@pytest.fixture(autouse=True)
def isolated_shared_settings(tmp_path, monkeypatch):
    monkeypatch.setenv("MONEYMAKER_SHARED_SETTINGS", str(tmp_path / "shared_settings.json"))


def _seed_labelled_scan(cache_file):
    with web_app._connect_write(str(cache_file)) as conn:
        web_app._ensure_scan_schema(conn)
        cursor = conn.execute(
            """
            INSERT INTO scan_runs (
                created_at_utc, provider, cache_file, years, limit_count, query,
                scanned_count, result_count, skipped_no_history, config_json, ticker_universe_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "2026-07-01T10:00:00",
                "yfinance",
                str(cache_file),
                15,
                None,
                "",
                1,
                1,
                0,
                json.dumps({"volume_multiplier": 2.0}),
                json.dumps(["AAA"]),
            ),
        )
        scan_id = int(cursor.lastrowid)
        conn.execute(
            """
            INSERT INTO scan_results (
                scan_id, rank, ticker, signal_date, close_price, market_cap,
                avg_volume, volume_ratio, sector, industry, result_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                scan_id,
                1,
                "AAA",
                "2026-06-29",
                1.25,
                12_000_000,
                500_000,
                2.5,
                "Materials",
                "Gold",
                json.dumps({"ticker": "AAA"}),
            ),
        )
        conn.execute(
            """
            INSERT INTO scan_labels (scan_id, ticker, label, note, labeled_at_utc)
            VALUES (?, ?, ?, ?, ?)
            """,
            (scan_id, "AAA", "winner", "watch closely", "2026-07-01T10:05:00"),
        )
        conn.commit()
    return scan_id


def test_labelled_selection_rows_reads_saved_labels(tmp_path):
    cache_file = tmp_path / "stock_cache.sqlite"
    scan_id = _seed_labelled_scan(cache_file)

    rows = web_app._labelled_selection_rows(str(cache_file))

    assert len(rows) == 1
    assert rows[0]["scan_id"] == scan_id
    assert rows[0]["ticker"] == "AAA"
    assert rows[0]["label"] == "winner"
    assert rows[0]["labeled_at_utc"] == "2026-07-01T10:05:00"


def test_export_labels_to_google_docs_builds_report_and_returns_doc_link(tmp_path, monkeypatch):
    cache_file = tmp_path / "stock_cache.sqlite"
    _seed_labelled_scan(cache_file)
    created = {}

    def fake_create_google_doc(title, content):
        created["title"] = title
        created["content"] = content
        return {
            "document_id": "doc-123",
            "url": "https://docs.google.com/document/d/doc-123/edit",
        }

    monkeypatch.setattr(web_app, "_create_google_doc", fake_create_google_doc)

    result = web_app._export_labels_to_google_docs({"cache_file": str(cache_file)})

    assert result["selection_count"] == 1
    assert result["document_id"] == "doc-123"
    assert result["url"].endswith("/doc-123/edit")
    assert "Moneymaker selections" in created["title"]
    assert "AAA (scan" in created["content"]
    assert "Winner" in created["content"]
    assert "Selected UTC: 2026-07-01T10:05:00" in created["content"]


def test_export_labels_to_google_docs_rejects_empty_selection_set(tmp_path):
    cache_file = tmp_path / "stock_cache.sqlite"
    with web_app._connect_write(str(cache_file)) as conn:
        web_app._ensure_scan_schema(conn)

    with pytest.raises(ValueError, match="No labelled selections"):
        web_app._export_labels_to_google_docs({"cache_file": str(cache_file)})


def test_label_scan_result_tracks_needs_confirmation_pick(tmp_path):
    cache_file = tmp_path / "stock_cache.sqlite"
    scan_id = _seed_labelled_scan(cache_file)

    result = web_app._label_scan_result(
        {
            "cache_file": str(cache_file),
            "scan_id": scan_id,
            "ticker": "AAA",
            "label": "needs_confirmation",
        }
    )

    picks = web_app._local_tracked_picks(str(cache_file), label="needs_confirmation")
    assert result["label"] == "needs_confirmation"
    assert result["label_display"] == "Needs Confirmation"
    assert len(picks) == 1
    assert picks[0]["ticker"] == "AAA"
    assert picks[0]["label_display"] == "Needs Confirmation"


def test_saved_picks_payload_keeps_confirmation_list_visible_when_filtered(tmp_path):
    cache_file = tmp_path / "stock_cache.sqlite"
    scan_id = _seed_labelled_scan(cache_file)
    web_app._label_scan_result(
        {
            "cache_file": str(cache_file),
            "scan_id": scan_id,
            "ticker": "AAA",
            "label": "needs_confirmation",
        }
    )

    payload = web_app._saved_picks_payload(
        {
            "cache_file": [str(cache_file)],
            "market": ["all"],
            "label": ["bad"],
            "query": [""],
        }
    )

    assert payload["picks"] == []
    assert len(payload["needs_confirmation"]) == 1
    assert payload["needs_confirmation"][0]["ticker"] == "AAA"


def test_apply_saved_ratings_to_results_marks_existing_pick(tmp_path):
    cache_file = tmp_path / "stock_cache.sqlite"
    scan_id = _seed_labelled_scan(cache_file)
    web_app._label_scan_result(
        {
            "cache_file": str(cache_file),
            "scan_id": scan_id,
            "ticker": "AAA",
            "label": "potential_winner",
        }
    )
    results = [{"ticker": "AAA"}, {"ticker": "BBB"}]

    web_app._apply_saved_ratings_to_results(str(cache_file), results)

    assert results[0]["label"] == "potential_winner"
    assert results[0]["label_display"] == "Potential Winner"
    assert results[0]["saved_pick"]["source"]
    assert results[1]["label"] is None
