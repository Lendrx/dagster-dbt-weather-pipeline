# Weather ELT Project!

Kleines End-to-End Projekt mit **Dagster + dbt + Postgres + Docker**.
Die Daten kommen aus der Open-Meteo API und werden in `staging`, `core` und `marts` modelliert.

## Stack

- Dagster (Orchestrierung + Checks)
- dbt (Transformation + Tests)
- Postgres (DWH)
- Docker Compose (lokales Setup)

## Kurzstart

```bash
cp .env.example .env
docker compose up -d postgres_dw
uv sync
set -a; source .env; set +a
uv run python - <<'PY'
from orchestration.assets import raw_sales_data, raw_weather_hourly_data
print(raw_sales_data())
print(raw_weather_hourly_data())
PY
uv run dbt build --project-dir dbt_project
uv run dagster dev
```

## Wichtige Modelle

- `marts.fct_weather_daily_summary`
- `marts.fct_weather_rainy_day_rate_7d`
- `marts.fct_weather_temperature_trend_7d`
- `marts.fct_daily_sales`

## Optional Docker UI

```bash
docker compose up -d --build dagster
```

Dagster UI: [http://127.0.0.1:3000](http://127.0.0.1:3000)
