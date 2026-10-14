-- Simplify active appraisals to Winner, Needs Confirmation, Maybe, and Bad.
-- Confirmed represented a positive confirmation, so it becomes Winner.
-- Potential Winner becomes Maybe as requested. Historical timestamps and
-- signal prices remain unchanged.

UPDATE rating_events SET label = 'maybe' WHERE label = 'potential_winner';
UPDATE rating_events SET label = 'winner' WHERE label = 'confirmed';
UPDATE scan_labels SET label = 'maybe' WHERE label = 'potential_winner';
UPDATE scan_labels SET label = 'winner' WHERE label = 'confirmed';
UPDATE user_appraisals SET label = 'maybe' WHERE label = 'potential_winner';
UPDATE user_appraisals SET label = 'winner' WHERE label = 'confirmed';
UPDATE user_picks SET label = 'maybe' WHERE label = 'potential_winner';
UPDATE user_picks SET label = 'winner' WHERE label = 'confirmed';

ALTER TABLE scan_labels DROP CONSTRAINT IF EXISTS scan_labels_label_check;
ALTER TABLE scan_labels ADD CONSTRAINT scan_labels_label_check
    CHECK (label IN ('winner', 'needs_confirmation', 'maybe', 'bad'));

ALTER TABLE user_appraisals DROP CONSTRAINT IF EXISTS user_appraisals_label_check;
ALTER TABLE user_appraisals ADD CONSTRAINT user_appraisals_label_check
    CHECK (label IN ('winner', 'needs_confirmation', 'maybe', 'bad'));

ALTER TABLE user_picks DROP CONSTRAINT IF EXISTS user_picks_label_check;
ALTER TABLE user_picks ADD CONSTRAINT user_picks_label_check
    CHECK (label IN ('winner', 'needs_confirmation', 'maybe', 'bad'));
