[Finvault -- README.rtf](https://github.com/user-attachments/files/23485397/Finvault.--.README.rtf)
{\rtf1\ansi\ansicpg1252\cocoartf2822
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 # FinVault\
\
FinVault is a **config-driven market data store** for traders, quants, and researchers.\
\
It pulls data from pluggable sources (e.g. equities from Stooq, macro from FRED), normalizes everything into a unified schema in **DuckDB**, and optionally snapshots to **Parquet** for fast analysis in Python, R, or any analytics stack.\
\
> Core idea: **one command** to update your local \'93data lake\'94 each month, driven entirely by a YAML config.\
\
---\
\
## Features\
\
- \uc0\u55358 \u56809  **Config-driven sources (YAML)**\
  - Define which datasets to pull, from where, and with what parameters.\
  - Easily switch universes (S&P 500, crypto, commodities) by editing `config.yaml`.\
\
- \uc0\u55357 \u56588  **Plugin architecture**\
  - Built-in plugins:\
    - `stooq` \'96 daily OHLCV for equities via Stooq CSV.\
    - `fred` \'96 macroeconomic series from FRED (Fed) API.\
  - Add new plugins by registering a class with `@register_plugin("name")`.\
\
- \uc0\u55357 \u56356  **Lightweight storage layer**\
  - Central **DuckDB** database (`observations`, `series_catalog`).\
  - Auto-created **monthly view**: `observations_monthly` (one row per series per month).\
  - Optional **Parquet exports** for downstream analysis.\
\
- \uc0\u55358 \u56821  **CLI via Typer**\
  - `init` \'96 initialize the DuckDB schema & view.\
  - `update` \'96 fetch all configured sources and upsert into DuckDB.\
  - `sql` \'96 run ad-hoc SQL against the database.\
  - `export` \'96 export query results to Parquet or CSV.\
\
- \uc0\u55357 \u56741 \u65039  **Interactive menu (optional)**\
  - `menu.py` provides a simple text UI (Rich prompts) to:\
    - Run `init`, `update`, `sql`, `export` interactively.\
    - Build `series_id` strings for specific tickers/fields.\
\
---\
\
## Project Structure\
\
Typical layout:\
\
```text\
finvault/\
  finvault.py      # core CLI + plugins + DuckDB storage\
  menu.py          # interactive menu wrapper around finvault.py\
  config.yaml      # user configuration for data sources\
  README.md        # this file\
}
