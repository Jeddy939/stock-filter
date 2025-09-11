"""Command line interface for the moneymaker package."""


import argparse
from . import fetcher


def main() -> None:
    parser = argparse.ArgumentParser(prog="moneymaker", description="Utilities for fetching and filtering stock data")
    sub = parser.add_subparsers(dest="command")

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

    args = parser.parse_args()
    if args.command == "fetch":
        fetcher.fetch_stock_data(args.ticker_file, args.output, args.years)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
