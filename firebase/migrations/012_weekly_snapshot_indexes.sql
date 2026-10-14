-- Indexes used by weekly static snapshot publication and latest-screen reads.

CREATE INDEX IF NOT EXISTS idx_scan_results_scan_rank
    ON scan_results (scan_id, rank);

CREATE INDEX IF NOT EXISTS idx_refresh_jobs_market_started
    ON refresh_jobs (market, started_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_rating_events_firestore_event
    ON rating_events ((result_json->>'firestore_event_id'))
    WHERE result_json ? 'firestore_event_id';
