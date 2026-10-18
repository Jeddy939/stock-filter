-- Keep broad-market benchmarks in the same refresh pipeline as company tickers.

INSERT INTO companies (market, ticker, info_json, fetched_at_utc)
VALUES (
    'asx',
    '^AORD',
    '{"symbol":"^AORD","shortName":"All Ordinaries","longName":"S&P/ASX All Ordinaries Index","quoteType":"INDEX"}'::jsonb,
    NOW()
)
ON CONFLICT (market, ticker) DO NOTHING;

INSERT INTO companies (market, ticker, info_json, fetched_at_utc)
VALUES (
    'us',
    'SPY',
    '{"symbol":"SPY","shortName":"SPDR S&P 500 ETF Trust","longName":"SPDR S&P 500 ETF Trust","quoteType":"ETF"}'::jsonb,
    NOW()
)
ON CONFLICT (market, ticker) DO NOTHING;
