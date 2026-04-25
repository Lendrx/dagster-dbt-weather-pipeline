import os
import socket
from datetime import datetime, timezone
import pandas as pd
import requests
from dagster import AssetCheckResult, asset, asset_check
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

# ------------------------------------------------------------------------------------
# Konfiguration
# ------------------------------------------------------------------------------------

# .env soll lokale Export-Variablen ueberschreiben.
load_dotenv(override=True)

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB")

if not DB_USER or not DB_PASSWORD or not DB_NAME:
    raise ValueError(
        "POSTGRES_USER, POSTGRES_PASSWORD und POSTGRES_DB muessen als Umgebungsvariablen gesetzt sein."
    )

DB_CONN = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# ------------------------------------------------------------------------------------
# Hilfsfunktionen
# ------------------------------------------------------------------------------------

def _resolve_connection_string() -> str:
    # Lokal: falls postgres_dw nicht aufloesbar ist, auf localhost zurueckfallen.
    if DB_HOST != "postgres_dw":
        return DB_CONN

    try:
        socket.gethostbyname(DB_HOST)
        return DB_CONN
    except socket.gaierror:
        fallback_host = "127.0.0.1"
        return f"postgresql://{DB_USER}:{DB_PASSWORD}@{fallback_host}:{DB_PORT}/{DB_NAME}"


# ------------------------------------------------------------------------------------
def _upsert_raw_table(df: pd.DataFrame, table_name: str, engine) -> None:
    # Rohdaten idempotent laden: truncate + append oder initial erstellen.
    inspector = inspect(engine)
    table_exists = inspector.has_table(table_name, schema="public")

    if table_exists:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE public.{table_name}"))
        df.to_sql(table_name, engine, schema="public", if_exists="append", index=False)
    else:
        df.to_sql(table_name, engine, schema="public", if_exists="fail", index=False)


# ------------------------------------------------------------------------------------
def _get_engine():
    return create_engine(_resolve_connection_string())


# ------------------------------------------------------------------------------------
# Ingestion Assets
# ------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------
@asset(group_name="ingestion")
def raw_sales_data():
    # Demo-Verkaufsdaten laden.
    data = {
        "order_id": [101, 102, 103],
        "customer_name": ["Max Mustermann", "Erika Muster", "John Doe"],
        "amount": [250.50, 450.00, 99.99],
        "order_date": ["2024-01-10", "2024-01-11", "2024-01-12"],
    }
    df = pd.DataFrame(data)
    engine = _get_engine()

    # Zielschemas sicherstellen.
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS core;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS marts;"))
        conn.commit()

    _upsert_raw_table(df, "raw_sales", engine)
    return "Extraction complete"


# ------------------------------------------------------------------------------------
@asset(group_name="ingestion")
def raw_weather_hourly_data():
    # Reale Wetterdaten aus Open-Meteo laden.
    response = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": 52.52,
            "longitude": 13.41,
            "hourly": "temperature_2m,precipitation,windspeed_10m",
            "past_days": 2,
            "forecast_days": 1,
            "timezone": "UTC",
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()

    # API-Response in tabellarisches Format bringen.
    hourly = payload["hourly"]
    df = pd.DataFrame(
        {
            "ts_utc": pd.to_datetime(hourly["time"], utc=True),
            "temperature_c": hourly["temperature_2m"],
            "precipitation_mm": hourly["precipitation"],
            "wind_speed_kmh": hourly["windspeed_10m"],
        }
    )
    df["location"] = "berlin"
    df["ingested_at_utc"] = datetime.now(timezone.utc)

    engine = _get_engine()
    _upsert_raw_table(df, "raw_weather_hourly", engine)
    return f"Loaded {len(df)} hourly weather rows"


# ------------------------------------------------------------------------------------
# Asset Checks
# ------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------
@asset_check(asset=raw_weather_hourly_data)
def weather_raw_min_rows_check() -> AssetCheckResult:
    # Mindestmenge an Zeilen pruefen.
    engine = _get_engine()
    with engine.connect() as conn:
        row_count = conn.execute(text("SELECT COUNT(*) FROM public.raw_weather_hourly")).scalar_one()

    return AssetCheckResult(
        passed=row_count >= 24,
        metadata={"row_count": row_count, "required_min_rows": 24},
    )


# ------------------------------------------------------------------------------------
@asset_check(asset=raw_weather_hourly_data)
def weather_raw_freshness_check() -> AssetCheckResult:
    # Frische der Wetterdaten pruefen.
    engine = _get_engine()
    with engine.connect() as conn:
        latest_ts = conn.execute(text("SELECT MAX(ts_utc) FROM public.raw_weather_hourly")).scalar_one()

    if latest_ts is None:
        return AssetCheckResult(
            passed=False,
            metadata={"reason": "No rows found in public.raw_weather_hourly"},
        )

    now_utc = datetime.now(timezone.utc)
    lag_hours = (now_utc - latest_ts).total_seconds() / 3600
    return AssetCheckResult(
        passed=lag_hours <= 6,
        metadata={"latest_ts_utc": str(latest_ts), "lag_hours": round(lag_hours, 2)},
    )
