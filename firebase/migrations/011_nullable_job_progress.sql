-- Jobs with unknown totals must be able to report indeterminate progress.
ALTER TABLE job_runs
    ALTER COLUMN percent DROP NOT NULL;

