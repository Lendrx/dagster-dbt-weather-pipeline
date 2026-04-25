-- Staging: Wetterdaten typisieren.
with source_data as (
    select
        ts_utc,
        location,
        temperature_c,
        precipitation_mm,
        wind_speed_kmh,
        ingested_at_utc
    from {{ source('my_source', 'raw_weather_hourly') }}
)
select
    cast(ts_utc as timestamp) as ts_utc,
    cast(ts_utc as date) as weather_date,
    location,
    cast(temperature_c as numeric) as temperature_c,
    cast(precipitation_mm as numeric) as precipitation_mm,
    cast(wind_speed_kmh as numeric) as wind_speed_kmh,
    cast(ingested_at_utc as timestamp) as ingested_at_utc
from source_data
