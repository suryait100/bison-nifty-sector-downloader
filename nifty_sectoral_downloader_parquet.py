"""
Nifty Sectoral Indices - Parquet Output
========================================

Scrapes the NSE Nifty sectoral indices directory, downloads the constituent
stock CSV for each index, and saves today's (or last trading day's) data
as a Parquet file under the configured data directory.

Handles holidays and weekends automatically: if today is not a trading day,
the most recent available trading date is used instead.

Output file: <data_dir>/nifty_sectoral_<trade_date>.parquet
Columns: trade_date, year, symbol, company_name, index_name

Run with:
    uv run python nifty_sectoral_downloader_parquet.py
"""

from __future__ import annotations

import html
import json
import re
import sys
import time
from configparser import ConfigParser
from datetime import date
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from loguru import logger
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Constants ---

INDEX_DIRECTORY_URL = "https://niftyindices.com/indices/equity/sectoral-indices"
BACKPAGE_URL = "https://www.niftyindices.com/BackPage/getHistoricaldatatabletoString"
EQUITY_MASTER_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
LOG_FILE = Path(__file__).parent / "logs" / "nifty_sectoral_parquet.log"

# Some index names derived from NSE page slugs/titles do not match what the
# calendar API expects (e.g. "25 50" vs "25/50", "Ex Bank" vs "Ex-Bank").
# These overrides map the scraped name → correct API name.
CALENDAR_NAME_OVERRIDES: dict[str, str] = {
    "NIFTY FINANCIAL SERVICES 25 50 INDEX": "Nifty Financial Services 25/50",
    "NIFTY FINANCIAL  SERVICES EX BANK":    "Nifty Financial Services Ex-Bank",
    "NIFTY FINANCIAL SERVICES EX BANK":     "Nifty Financial Services Ex-Bank",
    "NIFTY REITS   REALTY":                 "Nifty Reits & Realty",
    "NIFTY MIDSMALLHEALTHCARE":             "Nifty Midsmall Healthcare",
    "NIFTY MIDSMALL  IT TELECOM":           "Nifty Midsmall IT & Telecom",
    "NIFTY MIDSMALL IT TELECOM":            "Nifty Midsmall IT & Telecom",
}

# NSE has duplicate URLs for some indices where one URL is a broken page with no CSV.
# These are silently skipped since the data is already loaded via the correct URL.
SKIP_URLS: set[str] = {
    "https://niftyindices.com/indices/equity/sectoral-indices/nifty-financial-services-ex-bank",
    "https://niftyindices.com/indices/equity/sectoral-indices/nifty-midsmall-financial-services",
    "https://niftyindices.com/indices/equity/sectoral-indices/nifty-midsmall-it-telecom",
}

# Console: INFO and above only (summary + errors)
# File: DEBUG and above (full per-index detail)
logger.remove()
logger.add(sys.stderr, level="INFO", colorize=True)
logger.add(LOG_FILE, level="DEBUG", encoding="utf-8")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class Config:
    """
    Load runtime settings from config.ini.

    All keys fall back to safe defaults so the script works even without
    a config file.

    Sections read:
        [output]   - data_dir for parquet files
        [pipeline] - request_delay between HTTP calls
        [api]      - optional URL overrides for NSE endpoints
    """

    def __init__(self) -> None:
        parser = ConfigParser()
        parser.read(Path(__file__).parent / "config.ini", encoding="utf-8")

        # Seconds to wait between HTTP requests to avoid rate-limiting
        self.request_delay = float(parser.get("pipeline", "request_delay", fallback="0.35"))

        # NSE API endpoints (override in config.ini if URLs change)
        self.equity_master_url = parser.get("api", "equity_master_url", fallback=EQUITY_MASTER_URL)
        self.backpage_url = parser.get("api", "backpage_url", fallback=BACKPAGE_URL)

        # Directory where parquet files are saved
        default_data_dir = str(Path(__file__).parent / "data")
        self.data_dir = Path(parser.get("output", "data_dir", fallback=default_data_dir))


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

def _build_session() -> requests.Session:
    """
    Create a requests Session with retry logic and a browser-like User-Agent.

    Retries up to 3 times with exponential backoff on transient errors
    (429 Too Many Requests, 5xx server errors).
    """
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    return session


# ---------------------------------------------------------------------------
# Trading calendar
# ---------------------------------------------------------------------------

class TradingCalendar:
    """
    Fetch the list of trading dates for a Nifty index from the NSE BackPage API.

    The API accepts a date range and returns historical data rows, each
    containing a 'HistoricalDate' field (e.g. "11 Sep 2026").
    """

    # Headers required by the NSE BackPage AJAX endpoint
    HEADERS = {
        "Content-Type": "application/json; charset=utf-8",
        "Referer": "https://www.niftyindices.com/",
        "Origin": "https://www.niftyindices.com",
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
    }

    def __init__(self, session: requests.Session, backpage_url: str) -> None:
        self._session = session
        self._backpage_url = backpage_url

    def fetch(self, index_name: str, year: int) -> list[str]:
        """
        Return a list of trading date strings for the given index and year.

        Args:
            index_name: The canonical API name of the index (e.g. "Nifty IT").
            year:       The calendar year to fetch dates for.

        Returns:
            List of date strings in "DD Mon YYYY" format (e.g. ["11 Sep 2026"]).
            Returns [] if the index has no data or the API returns nothing.
        """
        today = date.today()
        start = f"01 Jan {year}"
        # For the current year, cap the end date to today to avoid future dates
        end = f"31 Dec {year}" if year < today.year else today.strftime("%d %b %Y")

        # The API expects a JSON body where 'cinfo' is a single-quoted JS-style string
        payload = json.dumps(
            {
                "cinfo": (
                    f"{{'name':'{index_name}','startDate':'{start}',"
                    f"'endDate':'{end}','indexName':'{index_name}'}}"
                )
            }
        )
        response = self._session.post(
            self._backpage_url,
            data=payload,
            headers=self.HEADERS,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json() or []
        return [row["HistoricalDate"] for row in data if row.get("HistoricalDate")]


# ---------------------------------------------------------------------------
# NSE page scrapers
# ---------------------------------------------------------------------------

def discover_index_pages(session: requests.Session) -> list[tuple[str, str]]:
    """
    Scrape the sectoral indices directory and return all index detail page links.

    Parses all hrefs matching the sectoral-indices URL pattern, deduplicates
    them, strips URL fragments, and derives a provisional index name from the
    URL slug (used only as a fallback; the real name comes from find_constituent_csv).

    Returns:
        List of (provisional_index_name, page_url) tuples.
    """
    response = session.get(INDEX_DIRECTORY_URL, timeout=30)
    response.raise_for_status()

    # Extract all hrefs pointing to individual sectoral index pages
    links = re.findall(
        r'href=["\']([^"\']*/indices/equity/sectoral-indices/[^"\']+)["\']',
        response.text,
        flags=re.IGNORECASE,
    )

    discovered: list[tuple[str, str]] = []
    seen_urls: set[str] = set()
    for link in links:
        page_url = urljoin(INDEX_DIRECTORY_URL, html.unescape(link))
        page_url = page_url.split("#", 1)[0]  # strip fragment anchors
        if page_url in seen_urls:
            continue
        seen_urls.add(page_url)

        # Derive a provisional name from the URL slug (hyphens/underscores → spaces)
        slug = page_url.rstrip("/").rsplit("/", 1)[-1]
        index_name = re.sub(r"[-_]+", " ", slug).upper()
        discovered.append((index_name, page_url))

    return discovered


def find_constituent_csv(session: requests.Session, page_url: str) -> tuple[str, str] | None:
    """
    Find the constituent stock CSV download link on an index detail page.

    Also extracts the canonical index name from the page's description text
    (pattern: "The Nifty X Index is designed"). Falls back to the URL slug
    if the description is not found.

    Args:
        session:  The requests Session to use.
        page_url: URL of the index detail page.

    Returns:
        (index_name, csv_url) if a constituent CSV is found, else None.
    """
    response = session.get(page_url, timeout=30)
    response.raise_for_status()
    page = html.unescape(response.text)  # decode HTML entities before regex matching

    # Look for the official constituent CSV link (e.g. /IndexConstituent/ind_niftyit.csv)
    match = re.search(
        r'href=["\']([^"\']*IndexConstituent/[^"\']+\.csv)["\']',
        page,
        flags=re.IGNORECASE,
    )
    if match is None:
        return None

    # Extract the canonical name from the standard description sentence
    title_match = re.search(r"The\s+(Nifty\s+.+?)\s+Index\s+is\s+designed", page, re.IGNORECASE)
    index_name = (
        title_match.group(1).strip().upper()
        if title_match
        else page_url.rstrip("/").rsplit("/", 1)[-1].replace("-", " ").upper()
    )
    return index_name, urljoin(page_url, match.group(1))


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def load_master(session: requests.Session, config: Config) -> pd.DataFrame:
    """
    Download the NSE equity master file and return a Symbol-indexed lookup table.

    Filters to EQ series only (excludes BE, SM, etc.) and deduplicates on Symbol.

    Returns:
        DataFrame indexed by Symbol with columns [Company_Name, Series].
    """
    response = session.get(config.equity_master_url, timeout=30)
    response.raise_for_status()
    master = pd.read_csv(pd.io.common.StringIO(response.text), dtype=str)

    # Strip whitespace from column names (NSE CSV sometimes has trailing spaces)
    master.columns = [column.strip() for column in master.columns]
    master = master[["SYMBOL", "NAME OF COMPANY", "SERIES"]].copy()
    master.columns = ["Symbol", "Company_Name", "Series"]
    master = master.apply(lambda column: column.str.strip())  # strip all cell values

    return master[master["Series"] == "EQ"].drop_duplicates("Symbol").set_index("Symbol")


def load_constituents(
    session: requests.Session,
    csv_url: str,
    index_name: str,
    master: pd.DataFrame,
) -> pd.DataFrame:
    """
    Download an index's constituent CSV and enrich each symbol with company name.

    Supports both 'SYMBOL' and 'INDEX SYMBOL' column names (case-insensitive).
    If a symbol is not found in the equity master, the symbol itself is used
    as the company name (handles newly listed or unlisted instruments).

    Args:
        session:    The requests Session to use.
        csv_url:    Direct download URL of the constituent CSV.
        index_name: Name of the index (attached to each row as Index_Name).
        master:     Symbol-indexed equity master from load_master().

    Returns:
        DataFrame with columns [Symbol, Company_Name, Series, Index_Name].

    Raises:
        ValueError: If no recognised symbol column is found in the CSV.
    """
    response = session.get(csv_url, timeout=30)
    response.raise_for_status()
    frame = pd.read_csv(pd.io.common.StringIO(response.text), dtype=str)
    frame.columns = [column.strip() for column in frame.columns]

    # NSE uses either 'Symbol' or 'Index Symbol' depending on the index
    symbol_column = next(
        (column for column in frame.columns if column.upper() in {"SYMBOL", "INDEX SYMBOL"}),
        None,
    )
    if symbol_column is None:
        raise ValueError(f"Symbol column not found for {index_name}: {list(frame.columns)}")

    rows = []
    for symbol in frame[symbol_column].dropna().astype(str).str.strip().drop_duplicates():
        if symbol in master.index:
            company_name = master.loc[symbol, "Company_Name"]
            series = master.loc[symbol, "Series"]
        else:
            # Fallback for symbols missing from the equity master
            company_name = symbol
            series = "EQ"
        rows.append(
            {
                "Symbol": symbol,
                "Company_Name": company_name,
                "Series": series,
                "Index_Name": index_name,
            }
        )
    return pd.DataFrame(rows)


def latest_available_date(
    calendar: TradingCalendar,
    index_name: str,
    today: date,
) -> date | None:
    """
    Return the most recent trading date on or before today for the given index.

    Handles holidays and weekends automatically — if today is not a trading day,
    the last available trading date is returned instead.

    Uses CALENDAR_NAME_OVERRIDES to translate scraped index names to the exact
    names expected by the NSE calendar API (some names differ due to special
    characters or formatting in URL slugs).

    Args:
        calendar:   TradingCalendar instance to fetch dates from.
        index_name: Scraped index name (uppercase, may have extra whitespace).
        today:      Reference date (usually date.today()).

    Returns:
        Most recent trading date ≤ today, or None if no data is available.
    """
    # Use the override name if one exists, otherwise normalise whitespace and title-case
    api_name = CALENDAR_NAME_OVERRIDES.get(index_name) or " ".join(index_name.title().split())
    dates = calendar.fetch(api_name, today.year)
    if not dates:
        return None

    parsed_dates = pd.to_datetime(dates, format="%d %b %Y").date
    available_dates = [value for value in parsed_dates if value <= today]
    return max(available_dates) if available_dates else None


# ---------------------------------------------------------------------------
# Parquet output
# ---------------------------------------------------------------------------

def save_parquet(report: pd.DataFrame, data_dir: Path, year: int) -> Path:
    """
    Save the report DataFrame into a year-scoped Parquet file.

    Files are named nifty_sectoral_<year>.parquet and written to data_dir.
    If a file for the year already exists, rows for any trade_date present in
    the new report are replaced (idempotent re-runs), then the rest are kept.

    Args:
        report:   DataFrame with columns [trade_date, year, symbol, company_name, index_name].
        data_dir: Directory to write the file into (created if absent).
        year:     The calendar year, used in the filename.

    Returns:
        Path of the written file.
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / f"nifty_sectoral_{year}.parquet"
    if path.exists():
        existing = pd.read_parquet(path)
        # Remove rows for dates being replaced so re-runs stay idempotent
        new_dates = report["trade_date"].unique()
        existing = existing[~existing["trade_date"].isin(new_dates)]
        report = pd.concat([existing, report], ignore_index=True)
        report = report.sort_values(["trade_date", "index_name", "symbol"])
    report.to_parquet(path, index=False)
    return path


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Orchestrate the full pipeline:
      1. Load equity master (symbol → company name lookup)
      2. Discover all sectoral index pages from the NSE directory
      3. For each index: find constituent CSV → fetch latest trading date → load stocks
      4. Concatenate all results and save as a Parquet file

    Individual index failures are caught and logged without aborting the run.
    Parquet save failures are caught after the data collection loop so a save error
    does not discard already-fetched data (though without a retry, that data is lost
    for this run).
    """
    config = Config()
    session = _build_session()
    calendar = TradingCalendar(session, config.backpage_url)
    today = date.today()

    master = load_master(session, config)
    index_pages = discover_index_pages(session)

    logger.debug("Loading sector stocks for {}", today)
    all_frames: list[pd.DataFrame] = []

    loaded_indices: list[str] = []   # successfully loaded index names
    missed_indices: list[str] = []   # skipped or failed index names
    trade_dates: set[date] = set()   # unique trading dates seen

    for number, (_directory_name, page_url) in enumerate(index_pages, 1):
        # Silently skip known duplicate/broken URLs whose data is loaded via another URL
        if page_url in SKIP_URLS:
            logger.debug("[{}/{}] SKIP (known duplicate URL) - {}", number, len(index_pages), page_url)
            continue
        try:
            # Step 1: Find the constituent CSV link and canonical index name
            page_data = find_constituent_csv(session, page_url)
            if page_data is None:
                logger.debug("[{}/{}] SKIP - no constituent CSV", number, len(index_pages))
                missed_indices.append(page_url.rstrip("/").rsplit("/", 1)[-1])
                continue

            index_name, csv_url = page_data

            # Step 2: Get the most recent trading date (handles holidays/weekends)
            trade_date = latest_available_date(calendar, index_name, today)
            if trade_date is None:
                logger.debug("[{}/{}] {} - no trading data", number, len(index_pages), index_name)
                missed_indices.append(index_name)
                continue

            # Step 3: Download constituent stocks and enrich with company names
            constituents = load_constituents(session, csv_url, index_name, master)
            if constituents.empty:
                logger.debug("[{}/{}] {} - no stocks", number, len(index_pages), index_name)
                missed_indices.append(index_name)
                continue

            # Tag each row with the trading date and year
            constituents["Trade_Date"] = trade_date
            constituents["Year"] = trade_date.year
            all_frames.append(constituents)
            loaded_indices.append(index_name)
            trade_dates.add(trade_date)
            logger.debug(
                "[{}/{}] {} - {:,} stocks for {}",
                number, len(index_pages), index_name, len(constituents), trade_date,
            )

        except Exception as exc:
            logger.error("[{}/{}] {} - {}", number, len(index_pages), page_url, exc)
            missed_indices.append(page_url.rstrip("/").rsplit("/", 1)[-1])

        # Rate-limit: pause between requests to avoid triggering NSE's throttle
        time.sleep(config.request_delay)

    # Build the final report with consistent column order
    columns = ["Trade_Date", "Year", "Symbol", "Company_Name", "Index_Name"]
    report = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame(columns=columns)
    report = report[columns].sort_values(["Index_Name", "Symbol"])

    # Normalise column names to lowercase for parquet output
    report.columns = [col.lower() for col in report.columns]

    # --- Summary log ---
    trading_date_str = ", ".join(str(d) for d in sorted(trade_dates)) if trade_dates else "N/A"
    logger.info("=" * 60)
    logger.info("Execution Date  : {}", today)
    logger.info("Trading Date(s) : {}", trading_date_str)
    logger.info("Rows            : {:,}", len(report))
    logger.info("Loaded Indices  : [{}]", ", ".join(loaded_indices))
    if missed_indices:
        logger.warning("Missed Indices  : [{}]", ", ".join(missed_indices))
    else:
        logger.info("Missed Indices  : None")
    logger.info("=" * 60)

    # Save parquet (failures here are logged but do not crash the process)
    logger.info("Saving parquet file...")
    try:
        parquet_path = save_parquet(report, config.data_dir, today.year)
        logger.success("Parquet saved: {} ({:,} rows)", parquet_path, len(report))
    except Exception as exc:
        logger.error("Parquet save error: {}", exc)


if __name__ == "__main__":
    main()
