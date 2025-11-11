#!/usr/bin/env python3
# FinVault: modular monthly data store (DuckDB + Parquet) with plugin fetchers
# Run: python finvault.py init | update | sql | export
import os
import io
import sys
import json
import time
import typing as t
from datetime import datetime, date, timedelta

import duckdb
import polars as pl
import requests
import requests_cache
import typer
import yaml
from dateutil.relativedelta import relativedelta
from pydantic import BaseModel, Field
from rich.console import Console
from datetime import timezone
console = Console()

app = typer.Typer(help="FinVault: modular monthly data store (DuckDB + Parquet)")

# ---------------------------
# Config models
# ---------------------------
class SourceConfig(BaseModel):
    id: str
    type: str
    enabled: bool = True
    params: dict = Field(default_factory=dict)

class AppConfig(BaseModel):
    duckdb_path: str = "data/finvault.duckdb"
    cache_days: int = 45
    export_parquet_dir: str = "data/parquet"
    sources: list[SourceConfig] = Field(default_factory=list)

# ---------------------------
# Storage layer (DuckDB)
# ---------------------------
DDL_OBS = """
CREATE TABLE IF NOT EXISTS observations (
    dataset TEXT,
    series_id TEXT,
    date DATE,
    value DOUBLE,
    attributes JSON,
    updated_at TIMESTAMP
);
"""

DDL_SERIES = """
CREATE TABLE IF NOT EXISTS series_catalog (
    dataset TEXT,
    series_id TEXT,
    title TEXT,
    frequency TEXT,
    units TEXT,
    source TEXT,
    notes TEXT,
    PRIMARY KEY (dataset, series_id)
);
"""

VIEW_MONTHLY = """
CREATE OR REPLACE VIEW observations_monthly AS
WITH x AS (
  SELECT *,
         date_trunc('month', date) AS month,
         row_number() OVER (
           PARTITION BY dataset, series_id, date_trunc('month', date)
           ORDER BY date DESC
         ) AS rn
  FROM observations
)
SELECT dataset, series_id, month AS date, value, attributes, updated_at
FROM x
WHERE rn = 1;
"""

def ensure_db(con: duckdb.DuckDBPyConnection):
    con.execute(DDL_OBS)
    con.execute(DDL_SERIES)
    con.execute(VIEW_MONTHLY)

def connect_duckdb(path: str) -> duckdb.DuckDBPyConnection:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    return duckdb.connect(path)

def _empty_obs_df() -> pl.DataFrame:
    return pl.DataFrame({
        "dataset": pl.Series([], dtype=pl.Utf8),
        "series_id": pl.Series([], dtype=pl.Utf8),
        "date": pl.Series([], dtype=pl.Date),
        "value": pl.Series([], dtype=pl.Float64),
        "attributes": pl.Series([], dtype=pl.Utf8),
        "updated_at": pl.Series([], dtype=pl.Datetime)
    })

def _empty_series_df() -> pl.DataFrame:
    return pl.DataFrame({
        "dataset": pl.Series([], dtype=pl.Utf8),
        "series_id": pl.Series([], dtype=pl.Utf8),
        "title": pl.Series([], dtype=pl.Utf8),
        "frequency": pl.Series([], dtype=pl.Utf8),
        "units": pl.Series([], dtype=pl.Utf8),
        "source": pl.Series([], dtype=pl.Utf8),
        "notes": pl.Series([], dtype=pl.Utf8),
    })

def upsert_observations(con: duckdb.DuckDBPyConnection, df: pl.DataFrame):
    if df.is_empty():
        return
    needed = ["dataset", "series_id", "date", "value", "attributes", "updated_at"]
    for col in needed:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")
    # Line 120 (Corrected)
    con.register("obs_new", df.to_pandas())
    con.execute("INSERT INTO observations SELECT * FROM obs_new")
    # Inside upsert_observations function (around line 122)
    con.execute(
        """
        CREATE OR REPLACE TABLE observations AS
        SELECT 
            dataset, series_id, date, value, attributes, updated_at -- Changed this line
        FROM (
        SELECT *,
                row_number() OVER (
                PARTITION BY dataset, series_id, date
                ORDER BY updated_at DESC
                ) AS rn
        FROM observations
        ) WHERE rn = 1;
        """
)

def upsert_series_catalog(con: duckdb.DuckDBPyConnection, cat_df: pl.DataFrame):
    if cat_df.is_empty():
        return
    con.register("series_new", cat_df.to_pandas())
    con.execute("INSERT OR REPLACE INTO series_catalog SELECT * FROM series_new")

def export_parquet(con: duckdb.DuckDBPyConnection, sql: str, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"export_{int(time.time())}.parquet")
    con.execute(f"COPY ({sql}) TO '{out_path}' (FORMAT PARQUET)")
    return out_path

# ---------------------------
# HTTP & caching
# ---------------------------
def build_session(cache_days: int) -> requests.Session:
    expire = timedelta(days=cache_days)
    os.makedirs(".http_cache", exist_ok=True)
    return requests_cache.CachedSession(
        cache_name=".http_cache/finvault",
        backend="sqlite",
        expire_after=expire,
        allowable_codes=(200,),
        stale_if_error=True,
    )

# ---------------------------
# Plugin base & registry
# ---------------------------
class FetchResult(t.TypedDict):
    observations: pl.DataFrame
    series_catalog: pl.DataFrame

class BasePlugin:
    """Plugins should implement .fetch(asof) and return DataFrames with unified schema."""
    def __init__(self, cfg: SourceConfig, session: requests.Session):
        self.cfg = cfg
        self.session = session
    @property
    def dataset(self) -> str:
        return self.cfg.id
    def fetch(self, asof: date) -> FetchResult:  # override per plugin
        raise NotImplementedError

PLUGIN_REGISTRY: dict[str, t.Type[BasePlugin]] = {}
def register_plugin(name: str):
    def deco(cls):
        PLUGIN_REGISTRY[name] = cls
        return cls
    return deco

# ---------------------------
# Stooq plugin (CSV, low friction)
# ---------------------------
@register_plugin("stooq")
class StooqPlugin(BasePlugin):
    """Daily OHLCV from Stooq; we store requested fields and expose monthly view via SQL."""
    def fetch(self, asof: date) -> FetchResult:
        ps = self.cfg.params
        tickers: list[str] = ps.get("tickers", [])
        fields: list[str] = ps.get("fields", ["close"])  # subset of open/high/low/close/volume
        base = "https://stooq.com/q/d/l/"  # ?s={ticker}&i=d

        obs_frames: list[pl.DataFrame] = []
        cat_rows: list[dict] = []

        # Inside StooqPlugin.fetch method
        
        for tk in tickers:
            # --- Start Changes ---
            console.print(f"  Fetching {tk}...", style="dim") # Add this print
            try: # Add try block
                url = f"{base}?s={tk}&i=d"
                r = self.session.get(url, timeout=60)
                r.raise_for_status()
                if not r.text.strip():
                    console.print(f"  Skipping {tk}: No data returned.", style="yellow")
                    time.sleep(0.15) # Add sleep even on skip
                    continue

                # Line 209 (Should already be correct)
                df = pl.read_csv(
                    io.StringIO(r.text),
                    schema_overrides={
                        "Date": pl.Utf8,
                        "Open": pl.Float64,
                        "High": pl.Float64,
                        "Low": pl.Float64,
                        "Close": pl.Float64,
                        "Volume": pl.Float64,
                    },
                )

                if df.is_empty():
                    console.print(f"  Skipping {tk}: Empty DataFrame after read.", style="yellow")
                    time.sleep(0.15) # Add sleep even on skip
                    continue
                
                # --- Keep existing rename/strptime logic ---
                df = df.rename({
                    "Date": "date", "Open": "open", "High": "high",
                    "Low": "low", "Close": "close", "Volume": "volume"
                }).with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d", strict=False))
                # --- Keep existing rename/strptime logic ---
                
                # --- Keep the loop for fields ---
                for f in fields:
                    if f not in df.columns:
                        continue
                    series_id = f"{tk}:{f}"
                    obs = pl.DataFrame({
                        "dataset": pl.Series([self.dataset] * len(df), dtype=pl.Utf8),
                        "series_id": pl.Series([series_id] * len(df), dtype=pl.Utf8),
                        "date": df["date"].cast(pl.Date),
                        "value": df[f].cast(pl.Float64),
                        "attributes": pl.Series([None] * len(df), dtype=pl.Utf8),
                        "updated_at": pl.Series([datetime.now(timezone.utc)] * len(df), dtype=pl.Datetime), # Use timezone-aware UTC now
                    })
                    obs_frames.append(obs)
                    cat_rows.append({
                        "dataset": self.dataset,
                        "series_id": series_id,
                        "title": f"Stooq {tk} {f}",
                        "frequency": "daily",
                        "units": "",
                        "source": "stooq",
                        "notes": "finvault stooq plugin",
                    })
                # --- End loop for fields ---
        # after the for tk in tickers loop:

            except requests.exceptions.RequestException as e: # Add except block
                console.print(f"  [bold red]Error fetching {tk}:[/bold red] {e}", style="red")
            except Exception as e: # Add generic except block
                console.print(f"  [bold red]Error processing {tk}:[/bold red] {e}", style="red")

            time.sleep(0.15) # Add a 0.5 second pause after each ticker
            
                    # After the ticker loop:

        if obs_frames:
            obs_all = pl.concat(obs_frames)
        else:
            obs_all = _empty_obs_df()

        if cat_rows:
            cat_df = pl.from_dicts(cat_rows)
        else:
            cat_df = _empty_series_df()

        return {"observations": obs_all, "series_catalog": cat_df}


# ---------------------------
# FRED plugin (optional; API key required; keep usage minimal & cached)
# ---------------------------
@register_plugin("fred")
class FredPlugin(BasePlugin):
    API = "https://api.stlouisfed.org/fred/series/observations"
    def fetch(self, asof: date) -> FetchResult:
        ps = self.cfg.params
        key_env = ps.get("api_key_env", "FRED_API_KEY")
        api_key = os.getenv(key_env)
        if not api_key:
            typer.secho(f"[fred:{self.dataset}] Skipping — missing {key_env} in environment.", fg=typer.colors.YELLOW)
            return {"observations": _empty_obs_df(), "series_catalog": _empty_series_df()}

        series = ps.get("series", [])
        obs_frames: list[pl.DataFrame] = []
        cat_rows: list[dict] = []

        for sid in series:
            params = {
                "series_id": sid,
                "api_key": api_key,
                "observation_start": "1900-01-01",
                "file_type": "json",
            }
            r = self.session.get(self.API, params=params, timeout=60)
            r.raise_for_status()
            data = r.json()
            obs = data.get("observations", [])
            if not obs:
                continue

            df = pl.DataFrame({
                "date": [o["date"] for o in obs],
                "value": [None if (o["value"] in (".", "")) else float(o["value"]) for o in obs],
            }).with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d", strict=False))

            series_id = sid
            obs_df = pl.DataFrame({
                "dataset": pl.Series([self.dataset] * len(df), dtype=pl.Utf8),
                "series_id": pl.Series([series_id] * len(df), dtype=pl.Utf8),
                "date": df["date"].cast(pl.Date),
                "value": df["value"].cast(pl.Float64),
                "attributes": pl.Series([None] * len(df), dtype=pl.Utf8),
                "updated_at": pl.Series([datetime.now(timezone.utc)] * len(df), dtype=pl.Datetime),
            })
            obs_frames.append(obs_df)
            cat_rows.append({
                "dataset": self.dataset,
                "series_id": series_id,
                "title": f"FRED {sid}",
                "frequency": "auto",
                "units": "",
                "source": "FRED",
                "notes": "finvault fred plugin (cached)",
            })

        obs_all = pl.concat(obs_frames) if obs_frames else _empty_obs_df()
        cat_df = pl.from_dicts(cat_rows) if cat_rows else _empty_series_df()
        return {"observations": obs_all, "series_catalog": cat_df}

# ---------------------------
# Helpers
# ---------------------------
def load_config(path: str) -> AppConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return AppConfig(**raw)

def run_plugins(cfg: AppConfig, asof: date) -> tuple[pl.DataFrame, pl.DataFrame]:
    sess = build_session(cfg.cache_days)
    obs_all: list[pl.DataFrame] = []
    cat_all: list[pl.DataFrame] = []

    for sc in cfg.sources:
        if not sc.enabled:
            continue
        cls = PLUGIN_REGISTRY.get(sc.type)
        if not cls:
            typer.secho(f"Unknown source type: {sc.type}", fg=typer.colors.RED)
            continue
        plugin = cls(sc, sess)
        typer.secho(f"Fetching: {sc.id} ({sc.type})", fg=typer.colors.CYAN)
        res = plugin.fetch(asof)
        if not res:
            continue
        if isinstance(res.get("observations"), pl.DataFrame) and not res["observations"].is_empty():
            obs_all.append(res["observations"])
        if isinstance(res.get("series_catalog"), pl.DataFrame) and not res["series_catalog"].is_empty():
            cat_all.append(res["series_catalog"])

    obs_df = pl.concat(obs_all) if obs_all else _empty_obs_df()
    cat_df = pl.concat(cat_all) if cat_all else _empty_series_df()
    return obs_df, cat_df

# ---------------------------
# CLI commands
# ---------------------------
@app.command()
def init(config: str = typer.Option("config.yaml", help="Path to config YAML")):
    """Initialize DuckDB schema and monthly view."""
    cfg = load_config(config)
    con = connect_duckdb(cfg.duckdb_path)
    ensure_db(con)
    typer.secho(f"Initialized DB at {cfg.duckdb_path}", fg=typer.colors.GREEN)

# Lines 351-374 (After)
@app.command()
def update(
    config: str = typer.Option("config.yaml", help="Path to config YAML"),
    asof: str = typer.Option(None, help="YYYY-MM to label the run (defaults to today)"),
    do_export: bool = typer.Option(True, help="Write a Parquet snapshot of obs_monthly"),
):
    """Fetch from all enabled sources, upsert into DuckDB, refresh views, export Parquet."""
    cfg = load_config(config)
    con = connect_duckdb(cfg.duckdb_path)
    ensure_db(con)

    asof_date = date.today() if not asof else datetime.strptime(asof + "-01", "%Y-%m-%d").date()
    obs_df, cat_df = run_plugins(cfg, asof=asof_date)

    if not obs_df.is_empty():
        upsert_observations(con, obs_df)
        typer.secho(f"Upserted {len(obs_df)} observation rows", fg=typer.colors.GREEN)
    if not cat_df.is_empty():
        upsert_series_catalog(con, cat_df)
        typer.secho(f"Upserted/updated {len(cat_df)} catalog rows", fg=typer.colors.GREEN)

    if do_export:
        sql = "SELECT * FROM observations_monthly"
        out = export_parquet(con, sql, cfg.export_parquet_dir)
        typer.secho(f"Parquet snapshot: {out}", fg=typer.colors.BLUE)

@app.command()
def sql(
    query: str = typer.Argument(..., help="SQL to run against DuckDB"),
    config: str = typer.Option("config.yaml", help="Path to config YAML"),
):
    cfg = load_config(config)
    con = connect_duckdb(cfg.duckdb_path)
    res = con.execute(query).fetch_df()
    with pl.Config(fmt_str_lengths=24):
        print(pl.from_pandas(res).head(50))
    ensure_db(con)


@app.command()
def export(
    format: str = typer.Argument("parquet", help="parquet|csv"),
    out: str = typer.Argument(..., help="Output file or directory"),
    sql_query: str = typer.Option("SELECT * FROM observations_monthly", "--sql", help="SQL to export"),
    config: str = typer.Option("config.yaml", help="Path to config YAML"),
):
    cfg = load_config(config)
    con = connect_duckdb(cfg.duckdb_path)
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    if format == "parquet":
        con.execute(f"COPY ({sql_query}) TO '{out}' (FORMAT PARQUET)")
        typer.secho(f"Wrote {out}", fg=typer.colors.GREEN)
    elif format == "csv":
        con.execute(f"COPY ({sql_query}) TO '{out}' (HEADER, DELIMITER ',')")
        typer.secho(f"Wrote {out}", fg=typer.colors.GREEN)
    else:
        raise typer.BadParameter("format must be parquet or csv")

if __name__ == "__main__":
    app()
