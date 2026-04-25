-- Mart: Taegliche Wetterzusammenfassung.
with weather_daily as (
    select
        weather_date,
        location,
        avg_temperature_c,
        total_precipitation_mm,
        avg_wind_speed_kmh
    from {{ ref('dim_weather_daily') }}
)
select
    weather_date,
    location,
    avg_temperature_c,
    total_precipitation_mm,
    avg_wind_speed_kmh,
    case
        when total_precipitation_mm > 5 then 'rainy'
        when avg_temperature_c >= 20 then 'warm'
        else 'normal'
    end as weather_label
from weather_daily
