-- Mart KPI: Anteil regnerischer Tage der letzten 7 Tage.
with weather_7d as (
    select
        location,
        total_precipitation_mm
    from {{ ref('fct_weather_daily_summary') }}
    where weather_date >= current_date - interval '6 day'
)
select
    location,
    round(
        avg(
            case
                when total_precipitation_mm > 0 then 1
                else 0
            end
        )::numeric,
        4
    ) as rainy_day_rate_7d
from weather_7d
group by 1
