-- Central feedback inbox for authenticated MoneyMaker users.

CREATE TABLE IF NOT EXISTS app_feedback (
    id BIGSERIAL PRIMARY KEY,
    firebase_uid TEXT NOT NULL REFERENCES user_profiles(firebase_uid) ON DELETE CASCADE,
    user_email TEXT,
    category TEXT NOT NULL CHECK (category IN ('bug', 'data', 'idea', 'usability', 'other')),
    message TEXT NOT NULL CHECK (char_length(message) BETWEEN 3 AND 4000),
    page_path TEXT,
    market TEXT CHECK (market IS NULL OR market IN ('asx', 'us')),
    ticker TEXT,
    context_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'new' CHECK (status IN ('new', 'reviewed', 'planned', 'done', 'dismissed')),
    admin_note TEXT,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_app_feedback_admin_queue
    ON app_feedback (status, created_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_app_feedback_user
    ON app_feedback (firebase_uid, created_at_utc DESC);
