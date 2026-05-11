"""Compatibility wrapper around ``moneymaker.fetcher``."""

from pathlib import Path
import sys

# Ensure the src package is importable when running from repo root
sys.path.append(str(Path(__file__).resolve().parent / 'src'))

from moneymaker.fetcher import cli

if __name__ == '__main__':
    cli()
