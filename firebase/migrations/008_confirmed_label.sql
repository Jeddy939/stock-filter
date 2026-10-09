-- Add a completed confirmation state while preserving prior appraisal history.

ALTER TABLE scan_labels
    DROP CONSTRAINT IF EXISTS scan_labels_label_check;

ALTER TABLE scan_labels
    ADD CONSTRAINT scan_labels_label_check
    CHECK (label IN ('winner', 'potential_winner', 'needs_confirmation', 'confirmed', 'maybe', 'bad'));

ALTER TABLE user_appraisals
    DROP CONSTRAINT IF EXISTS user_appraisals_label_check;

ALTER TABLE user_appraisals
    ADD CONSTRAINT user_appraisals_label_check
    CHECK (label IN ('winner', 'potential_winner', 'needs_confirmation', 'confirmed', 'maybe', 'bad'));

ALTER TABLE user_picks
    DROP CONSTRAINT IF EXISTS user_picks_label_check;

ALTER TABLE user_picks
    ADD CONSTRAINT user_picks_label_check
    CHECK (label IN ('winner', 'potential_winner', 'needs_confirmation', 'confirmed', 'maybe', 'bad'));
