"""Utility script to aggregate total commissions paid on Aster futures accounts."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.bots.volume_generator import AccountClient, AccountConfig
from src.utils.utils import log

# Maintain high precision when summing commissions
getcontext().prec = 28

INCOME_ENDPOINT = "/fapi/v1/income"
DEFAULT_BASE_URL = "https://fapi.asterdex.com"
DEFAULT_RECV_WINDOW = 5_000
DEFAULT_START_DATE = datetime(2020, 1, 1, tzinfo=timezone.utc)
WINDOW_DAYS = 6  # API restricts queries to 7 days, stay safely below
WINDOW_MS = WINDOW_DAYS * 24 * 60 * 60 * 1000

CommissionFetcher = Callable[[Dict[str, int]], Sequence[Dict]]


def parse_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse a user-supplied datetime string into an aware UTC datetime."""

    if value is None:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid datetime format: {value}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def load_accounts(config_path: Path, *, filter_names: Optional[Sequence[str]] = None) -> Tuple[str, int, Dict[str, AccountConfig]]:
    """Load account credentials from the JSON config file."""

    with config_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    base_url = payload.get("base_url", DEFAULT_BASE_URL).rstrip("/")
    recv_window = int(payload.get("recv_window", DEFAULT_RECV_WINDOW))

    accounts: Dict[str, AccountConfig] = {}
    for entry in payload.get("accounts", []):
        name = entry.get("name")
        api_key = entry.get("api_key")
        api_secret = entry.get("api_secret")
        if not name or not api_key or not api_secret:
            raise ValueError("Each account entry must include name, api_key, and api_secret")
        account = AccountConfig(
            name=name,
            api_key=api_key,
            api_secret=api_secret,
            display_name=entry.get("display_name"),
        )
        if filter_names and name not in filter_names:
            continue
        accounts[name] = account

    if filter_names:
        missing = set(filter_names) - set(accounts.keys())
        if missing:
            raise ValueError(f"Requested account(s) not found in config: {', '.join(sorted(missing))}")

    if not accounts:
        raise ValueError("No accounts were loaded from the configuration file")

    return base_url, recv_window, accounts


def sum_commission_records(records: Iterable[Dict]) -> Tuple[Decimal, List[str]]:
    """Sum commission entries and collect the assets involved."""

    total = Decimal("0")
    assets: List[str] = []
    for record in records:
        if record.get("incomeType") and record["incomeType"].upper() != "COMMISSION":
            continue
        raw_income = record.get("income", "0")
        asset = record.get("asset")
        try:
            income = Decimal(str(raw_income))
        except Exception as exc:  # noqa: BLE001 - explicit context for parsing issues
            raise ValueError(f"Unable to parse income value '{raw_income}' from record: {record}") from exc
        if income == 0:
            continue
        if asset and asset not in assets:
            assets.append(asset)
        total += -income if income < 0 else income
    return total, assets


def collect_commissions(
    fetch_page: CommissionFetcher,
    *,
    start_ms: int,
    end_ms: int,
    symbol: Optional[str] = None,
    limit: int = 1000,
    window_ms: int = WINDOW_MS,
) -> Tuple[Decimal, List[str]]:
    """Iterate over the income API and aggregate commission totals."""

    total = Decimal("0")
    assets: List[str] = []
    cursor = start_ms

    while cursor <= end_ms:
        window_end = min(cursor + window_ms, end_ms)
        fetch_cursor = cursor

        while fetch_cursor <= window_end:
            params = {
                "incomeType": "COMMISSION",
                "startTime": int(fetch_cursor),
                "endTime": int(window_end),
                "limit": limit,
            }
            if symbol:
                params["symbol"] = symbol

            page = fetch_page(params)
            if not page:
                break

            chunk_total, chunk_assets = sum_commission_records(page)
            total += chunk_total
            for asset in chunk_assets:
                if asset not in assets:
                    assets.append(asset)

            last_time = max(int(item.get("time", fetch_cursor)) for item in page)
            next_cursor = last_time + 1
            if next_cursor <= fetch_cursor:
                break
            fetch_cursor = next_cursor

            if len(page) < limit:
                break

        cursor = window_end + 1

    return total, assets


def format_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    return format(normalized, "f") if normalized != normalized.to_integral() else f"{normalized:.0f}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch total trading fees paid for configured Aster accounts.")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("volume_bot_config.json"),
        help="Path to the JSON configuration file containing account credentials.",
    )
    parser.add_argument(
        "--accounts",
        nargs="+",
        help="Optional list of account names from the config to include (defaults to all).",
    )
    parser.add_argument(
        "--start-date",
        type=parse_datetime,
        help="Inclusive start date (ISO 8601). Defaults to 2020-01-01 UTC.",
    )
    parser.add_argument(
        "--end-date",
        type=parse_datetime,
        help="Inclusive end date (ISO 8601). Defaults to now.",
    )
    parser.add_argument(
        "--symbol",
        help="Optional symbol to filter commissions for (e.g., BTCUSDT).",
    )
    args = parser.parse_args()

    start_dt = args.start_date or DEFAULT_START_DATE
    end_dt = args.end_date or datetime.now(tz=timezone.utc)
    if end_dt < start_dt:
        raise SystemExit("End date must be greater than or equal to the start date")

    try:
        base_url, recv_window, accounts = load_accounts(args.config, filter_names=args.accounts)
    except (OSError, ValueError) as exc:
        raise SystemExit(str(exc))

    overall_total = Decimal("0")
    seen_assets: List[str] = []
    log.startup(
        "Starting fee aggregation\n"
        f"  Base URL: {base_url}\n"
        f"  Date range: {start_dt.isoformat()} → {end_dt.isoformat()}\n"
        f"  Symbol filter: {args.symbol or 'ALL'}"
    )

    for name, account in accounts.items():
        client = AccountClient(account, base_url, recv_window)

        def fetch(params: Dict[str, int]) -> Sequence[Dict]:
            return client.signed_get(INCOME_ENDPOINT, params=params)

        total, assets = collect_commissions(
            fetch,
            start_ms=int(start_dt.timestamp() * 1000),
            end_ms=int(end_dt.timestamp() * 1000),
            symbol=args.symbol,
        )
        overall_total += total
        for asset in assets:
            if asset not in seen_assets:
                seen_assets.append(asset)

        asset_list = ", ".join(assets) if assets else "USDT"
        log.info(
            f"[{account.label()}] Total commissions paid: {format_decimal(total)} {asset_list}"
        )

    asset_summary = ", ".join(seen_assets) if seen_assets else "USDT"
    log.shutdown(
        f"Aggregate commissions across {len(accounts)} account(s): {format_decimal(overall_total)} {asset_summary}"
    )


if __name__ == "__main__":
    main()
