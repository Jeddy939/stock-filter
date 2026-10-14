-- Track the price basis used by long-term history so adjusted and raw closes
-- cannot be silently combined in moving-average calculations.

ALTER TABLE market_status
    ADD COLUMN IF NOT EXISTS price_basis TEXT NOT NULL DEFAULT 'legacy_mixed';

ALTER TABLE market_status DROP CONSTRAINT IF EXISTS market_status_price_basis_check;
ALTER TABLE market_status ADD CONSTRAINT market_status_price_basis_check
    CHECK (price_basis IN ('legacy_mixed', 'raw_close_v1'));
