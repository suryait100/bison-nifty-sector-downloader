# Bison Nifty Sector Downloader V2

Scrapes all **NSE Nifty Sectoral Indices**, downloads each index's constituent stocks, and saves the data either to **PostgreSQL** or as **year-wise Parquet files** — fully automated, holiday-aware, with structured logging.

---

## Table of Contents

- [Overview](#overview)
- [Scripts](#scripts)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [How to Run](#how-to-run)
- [Output](#output)
- [Database Schema](#database-schema)
- [Parquet Schema](#parquet-schema)
- [Holiday & Weekend Handling](#holiday--weekend-handling)
- [Logging](#logging)
- [NSE Quirks & Fixes](#nse-quirks--fixes)
- [Known Sectors](#known-sectors)

---

## Overview

Each script runs once per day (manually or via scheduler) and does the following:

1. Scrapes the NSE sectoral indices directory page
2. Visits each index's detail page to find the constituent stock CSV
3. Downloads the NSE equity master to get company names
4. Fetches the latest available trading date from the NSE calendar API
5. Loads constituent stocks and enriches them with company names
6. Saves data to **PostgreSQL** or **Parquet** depending on which script is run

---

## Scripts

| Script | Output | Use When |
|---|---|---|
| `nifty_sectoral_downloader.py` | PostgreSQL table | You want a live queryable DB |
| `nifty_sectoral_downloader_parquet.py` | Year-wise `.parquet` files | You want flat files for analysis |

Both scripts share the same scraping logic — only the output layer differs.

---

## Project Structure

```
bison_nifty_sector_downloader_V2/
│
├── nifty_sectoral_downloader.py         # DB loader — upserts into PostgreSQL
├── nifty_sectoral_downloader_parquet.py # Parquet writer — saves year-wise .parquet files
│
├── config.ini                           # Configuration (DB credentials, settings)
├── pyproject.toml                       # Project dependencies
├── uv.lock                              # Locked dependency versions
│
├── data/
│   └── nifty_sectoral_<year>.parquet    # Parquet output (one file per year)
│
├── logs/
│   ├── nifty_sectoral.log               # DB script log
│   └── nifty_sectoral_parquet.log       # Parquet script log
│
└── README.md
```

---

## Prerequisites

- Python >= 3.11
- PostgreSQL >= 13 *(only for `nifty_sectoral_downloader.py`)*
- [uv](https://github.com/astral-sh/uv) package manager

---

## Installation

```bash
cd bison_nifty_sector_downloader_V2
uv sync
```

---

## Configuration

Edit `config.ini` before running:

```ini
[database]
host       = localhost               # PostgreSQL host
port       = 5432                    # PostgreSQL port
dbname     = nifty_sectoral_indices  # Database name
user       = postgres                # DB username
password   = your_password           # DB password
table_name = nifty_sectoral_today    # Target table name

[pipeline]
request_delay = 0.35                 # Seconds between HTTP requests

[output]
data_dir = data                      # Directory for parquet files
```

> `config.ini` is gitignored — never commit credentials.

---

## How to Run

**DB loader (PostgreSQL):**
```bash
uv run python nifty_sectoral_downloader.py
```

**Parquet writer:**
```bash
uv run python nifty_sectoral_downloader_parquet.py
```

Run either script daily. Both are safe to re-run — the DB script upserts (no duplicates), and the Parquet script replaces only the rows for the current trading date.

---

## Output

### DB Script

Upserts into PostgreSQL table `nifty_sectoral_today`:

```
============================================================
Execution Date  : 2026-10-17
Trading Date(s) : 2026-10-16
Rows            : 674
Loaded Indices  : [NIFTY AUTO, NIFTY BANK, ...]
Missed Indices  : None
============================================================
DB upsert complete: 674 rows affected in 'nifty_sectoral_today'
```

### Parquet Script

Saves/appends into `data/nifty_sectoral_<year>.parquet`:

```
============================================================
Execution Date  : 2026-10-17
Trading Date(s) : 2026-10-16
Rows            : 674
Loaded Indices  : [NIFTY AUTO, NIFTY BANK, ...]
Missed Indices  : None
============================================================
Parquet saved: data\nifty_sectoral_2026.parquet (674 rows)
```

Each year's file accumulates all trading days for that year. When 2027 begins, a new `nifty_sectoral_2027.parquet` is created automatically.

---

## Database Schema

**Table:** `nifty_sectoral_today`

```sql
CREATE TABLE nifty_sectoral_today (
    trade_date   DATE         NOT NULL,
    year         INTEGER      NOT NULL,
    symbol       VARCHAR(50)  NOT NULL,
    company_name VARCHAR(200) NOT NULL,
    index_name   VARCHAR(100) NOT NULL,
    PRIMARY KEY (trade_date, symbol, index_name)
);
```

| Column | Type | Description |
|---|---|---|
| `trade_date` | `DATE` | Most recent trading date |
| `year` | `INTEGER` | Year extracted from trade_date |
| `symbol` | `VARCHAR(50)` | NSE stock symbol e.g. `RELIANCE` |
| `company_name` | `VARCHAR(200)` | Full company name |
| `index_name` | `VARCHAR(100)` | Sectoral index name e.g. `NIFTY IT` |

**Primary Key:** `(trade_date, symbol, index_name)` — a stock can appear in multiple indices.

---

## Parquet Schema

**File:** `data/nifty_sectoral_<year>.parquet`

| Column | Type | Description |
|---|---|---|
| `trade_date` | `date` | Trading date |
| `year` | `int` | Year |
| `symbol` | `string` | NSE stock symbol |
| `company_name` | `string` | Full company name |
| `index_name` | `string` | Sectoral index name |

Sorted by `trade_date → index_name → symbol`. Re-running the same day overwrites only that day's rows (idempotent).

---

## Holiday & Weekend Handling

The `latest_available_date()` function always picks the **most recent trading date ≤ today**:

| Today | Result |
|---|---|
| Trading day | Today's data |
| Weekend (Sat/Sun) | Last Friday's data |
| Public holiday | Last trading day before the holiday |

No manual calendar configuration needed — uses the NSE BackPage API.

---

## Logging

| Destination | Level | Content |
|---|---|---|
| Console | `INFO` | Summary block + errors only |
| `logs/nifty_sectoral.log` | `DEBUG` | Full per-index detail (DB script) |
| `logs/nifty_sectoral_parquet.log` | `DEBUG` | Full per-index detail (Parquet script) |

---

## NSE Quirks & Fixes

### Calendar API Name Mismatches

| Scraped Name | API Name Used |
|---|---|
| `NIFTY FINANCIAL SERVICES 25 50 INDEX` | `Nifty Financial Services 25/50` |
| `NIFTY FINANCIAL SERVICES EX BANK` | `Nifty Financial Services Ex-Bank` |
| `NIFTY REITS   REALTY` | `Nifty Reits & Realty` |
| `NIFTY MIDSMALLHEALTHCARE` | `Nifty Midsmall Healthcare` |
| `NIFTY MIDSMALL IT TELECOM` | `Nifty Midsmall IT & Telecom` |

### Duplicate URLs (auto-skipped)

NSE lists some indices under two URLs — the broken one is skipped, the correct one loads:

| Skipped | Loaded Via |
|---|---|
| `/nifty-financial-services-ex-bank` | `/nifty-financial--services-ex-bank` |
| `/nifty-midsmall-financial-services` | `/nifty-midsmall--financial-services` |
| `/nifty-midsmall-it-telecom` | `/nifty-midsmall--it-telecom` |

---

## Known Sectors

| # | Index Name |
|---|---|
| 1 | Nifty Auto |
| 2 | Nifty Bank |
| 3 | Nifty Capital Goods |
| 4 | Nifty Cement |
| 5 | Nifty Chemicals |
| 6 | Nifty Commercial & Transport Services |
| 7 | Nifty Construction |
| 8 | Nifty Consumer Durables |
| 9 | Nifty Consumer Services |
| 10 | Nifty Financial Services |
| 11 | Nifty Financial Services 25/50 |
| 12 | Nifty Financial Services Ex-Bank |
| 13 | Nifty FMCG |
| 14 | Nifty Healthcare |
| 15 | Nifty Hospitals |
| 16 | Nifty Housing Finance |
| 17 | Nifty Insurance |
| 18 | Nifty IT |
| 19 | Nifty Media |
| 20 | Nifty Metal |
| 21 | Nifty NBFC |
| 22 | Nifty Oil and Gas |
| 23 | Nifty Pharma |
| 24 | Nifty Power |
| 25 | Nifty Private Bank |
| 26 | Nifty PSU Bank |
| 27 | Nifty Realty |
| 28 | Nifty REITs & Realty |
| 29 | Nifty Retail |
| 30 | Nifty Telecommunications |
| 31 | Nifty500 Healthcare |
| 32 | Nifty MidSmall Financial Services |
| 33 | Nifty MidSmall Healthcare |
| 34 | Nifty MidSmall IT & Telecom |
