-- Core: Taegliche Wetteraggregation je Standort.
with weather_hourly as (
    select
        weather_date,
        location,
        temperature_c,
        precipitation_mm,
        wind_speed_kmh
    from {{ ref('stg_weather_hourly') }}
)
select
    weather_date,
    location,
    round(avg(temperature_c), 2) as avg_temperature_c,
    max(temperature_c) as max_temperature_c,
    min(temperature_c) as min_temperature_c,
    round(sum(precipitation_mm), 2) as total_precipitation_mm,
    round(avg(wind_speed_kmh), 2) as avg_wind_speed_kmh
from weather_hourly
group by 1, 2
