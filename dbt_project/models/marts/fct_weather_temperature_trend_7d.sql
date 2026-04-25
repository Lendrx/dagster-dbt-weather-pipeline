-- Mart KPI: Temperaturtrend (Start vs. Ende) der letzten 7 Tage.
with weather_7d as (
    select
        weather_date,
        location,
        avg_temperature_c
    from {{ ref('fct_weather_daily_summary') }}
    where weather_date >= current_date - interval '6 day'
),
ranked AS (
    select
        weather_date,
        location,
        avg_temperature_c,
        row_number() over (partition by location order by weather_date asc) as rn_asc,
        row_number() over (partition by location order by weather_date desc) as rn_desc
    from weather_7d
)
select
    location,
    max(case when rn_asc = 1 then avg_temperature_c end) as temp_start_7d,
    max(case when rn_desc = 1 then avg_temperature_c end) as temp_end_7d,
    round(
        (
            max(case when rn_desc = 1 then avg_temperature_c end) -
            max(case when rn_asc = 1 then avg_temperature_c end)
        )::numeric,
        2
    ) as temp_delta_7d
from ranked
group by 1
