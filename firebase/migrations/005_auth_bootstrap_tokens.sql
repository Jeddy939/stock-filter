-- One-time, owner-issued browser bootstrap links for invited users who cannot
-- receive the normal Firebase password setup email.

CREATE TABLE IF NOT EXISTS auth_bootstrap_tokens (
    token_hash TEXT PRIMARY KEY CHECK (length(token_hash) = 64),
    email TEXT NOT NULL REFERENCES app_user_invites(email) ON UPDATE CASCADE ON DELETE CASCADE,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at_utc TIMESTAMPTZ NOT NULL,
    used_at_utc TIMESTAMPTZ,
    used_by_uid TEXT,
    created_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_auth_bootstrap_tokens_expiry
    ON auth_bootstrap_tokens (expires_at_utc)
    WHERE used_at_utc IS NULL;
