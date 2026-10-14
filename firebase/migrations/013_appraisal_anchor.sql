-- Shared point-in-time anchor for appraisal features and outcomes.
--
-- An appraisal can only use, and be measured from, the last market session that
-- had closed when the rating was made. The session date is evaluated in the
-- exchange's local time so that, for example, a US rating at 9pm New York time
-- (already the next day in UTC) does not see the following session's bar.
-- Sessions are treated as complete from 16:30 local time. Weekends and holidays
-- resolve naturally because callers take the latest bar on or before this date.
--
-- Mirrored in cloud_backend/insights/anchor.py; keep both in sync.

CREATE OR REPLACE FUNCTION appraisal_cutoff_date(p_market TEXT, p_event_at TIMESTAMPTZ)
RETURNS DATE
LANGUAGE sql
STABLE
AS $$
    SELECT CASE
               WHEN local_at::time >= TIME '16:30' THEN local_at::date
               ELSE local_at::date - 1
           END
    FROM (
        SELECT p_event_at AT TIME ZONE CASE lower(p_market)
                                           WHEN 'asx' THEN 'Australia/Sydney'
                                           ELSE 'America/New_York'
                                       END AS local_at
    ) localized
$$;
