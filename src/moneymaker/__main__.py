"""Command line interface for the moneymaker package."""


import argparse
from . import fetcher


def main() -> None:
    parser = argparse.ArgumentParser(prog="moneymaker", description="Utilities for fetching and filtering stock data")
    sub = parser.add_subparsers(dest="command")

    us_tickers_cmd = sub.add_parser("us-tickers", help="Download a US ticker file from Nasdaq Trader")
    us_tickers_cmd.add_argument(
        "-o",
        "--output",
        default=fetcher.DEFAULT_US_TICKER_FILE,
        help=f"Output ticker file name (default: {fetcher.DEFAULT_US_TICKER_FILE})",
    )

    fetch_cmd = sub.add_parser("fetch", help="Download stock data to JSON")
    fetch_cmd.add_argument("ticker_file", help="File containing ticker symbols")
    fetch_cmd.add_argument(
        "-o",
        "--output",
        default=fetcher.DEFAULT_OUTPUT_FILE,
        help=f"Output JSON file name (default: {fetcher.DEFAULT_OUTPUT_FILE})",
    )
    fetch_cmd.add_argument(
        "-y",
        "--years",
        type=int,
        default=fetcher.DEFAULT_DATA_YEARS,
        help=f"Number of years of historical data (default: {fetcher.DEFAULT_DATA_YEARS})",
    )
    fetch_cmd.add_argument(
        "-w",
        "--workers",
        type=int,
        default=fetcher.DEFAULT_WORKERS,
        help=f"Number of threads for downloading data (default: {fetcher.DEFAULT_WORKERS})",
    )
    fetch_cmd.add_argument(
        "--provider",
        choices=fetcher.SUPPORTED_PROVIDERS,
        default=fetcher.DEFAULT_PROVIDER,
        help=f"Historical OHLCV provider (default: {fetcher.DEFAULT_PROVIDER})",
    )
    fetch_cmd.add_argument(
        "--limit",
        type=int,
        help="Only fetch the first N tickers from the ticker file",
    )
    fetch_cmd.add_argument(
        "--cache-file",
        default=fetcher.DEFAULT_CACHE_FILE,
        help=f"SQLite cache file for incremental fetches (default: {fetcher.DEFAULT_CACHE_FILE})",
    )
    fetch_cmd.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable SQLite caching and fetch the requested data directly",
    )
    fetch_cmd.add_argument(
        "--no-json-export",
        action="store_true",
        help="Update SQLite cache without writing the legacy JSON export.",
    )
    fetch_cmd.add_argument(
        "--info-refresh-days",
        type=int,
        default=fetcher.DEFAULT_INFO_REFRESH_DAYS,
        help=(
            "Refresh cached company info after this many days "
            f"(default: {fetcher.DEFAULT_INFO_REFRESH_DAYS})"
        ),
    )
    fetch_cmd.add_argument(
        "--history-refresh-days",
        type=int,
        default=fetcher.DEFAULT_HISTORY_REFRESH_DAYS,
        help=(
            "Refetch this many days before the latest cached price bar "
            f"(default: {fetcher.DEFAULT_HISTORY_REFRESH_DAYS})"
        ),
    )
    fetch_cmd.add_argument(
        "--prune-missing-tickers",
        action="store_true",
        help=(
            "Create a new ticker file with attempted tickers that had missing "
            "historical data removed. The original file is not changed."
        ),
    )
    fetch_cmd.add_argument("--history-chunk-size", type=int, default=fetcher.DEFAULT_HISTORY_CHUNK_SIZE)
    fetch_cmd.add_argument("--history-pause-seconds", type=float, default=fetcher.DEFAULT_HISTORY_PAUSE_SECONDS)
    fetch_cmd.add_argument("--info-pause-seconds", type=float, default=fetcher.DEFAULT_INFO_PAUSE_SECONDS)
    fetch_cmd.add_argument("--rate-limit-pause-seconds", type=float, default=fetcher.DEFAULT_RATE_LIMIT_PAUSE_SECONDS)
    fetch_cmd.add_argument("--max-rate-limit-retries", type=int, default=fetcher.DEFAULT_RATE_LIMIT_RETRIES)
    fetch_cmd.add_argument(
        "--stop-on-rate-limit",
        action="store_true",
        default=fetcher.DEFAULT_STOP_ON_RATE_LIMIT,
        help="Stop when yfinance rate-limits history data so cached progress can resume later.",
    )

    args = parser.parse_args()
    if args.command == "us-tickers":
        result = fetcher.write_us_ticker_file(args.output)
        print(f"Wrote {result['ticker_count']} US tickers to {result['output_file']}")
    elif args.command == "fetch":
        success = fetcher.fetch_stock_data(
            args.ticker_file,
            args.output,
            args.years,
            args.workers,
            args.provider,
            args.limit,
            None if args.no_cache else args.cache_file,
            args.info_refresh_days,
            args.history_refresh_days,
            None,
            args.prune_missing_tickers,
            args.history_chunk_size,
            args.history_pause_seconds,
            args.info_pause_seconds,
            args.rate_limit_pause_seconds,
            args.max_rate_limit_retries,
            args.stop_on_rate_limit,
            not args.no_json_export,
        )
        if not success:
            raise SystemExit(1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
