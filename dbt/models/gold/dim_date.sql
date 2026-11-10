-- One row per calendar day covering the Olist period.
with days as (
    select d as date_day
    from unnest(sequence(date '2016-01-01', date '2018-12-31')) as t(d)
)
select
    cast(date_format(cast(date_day as timestamp), '%Y%m%d') as integer) as date_key,
    date_day,
    year(date_day)                                  as year,
    quarter(date_day)                               as quarter,
    month(date_day)                                 as month,
    date_format(cast(date_day as timestamp), '%M')  as month_name,
    day_of_week(date_day)                           as day_of_week,
    day_of_week(date_day) in (6, 7)                 as is_weekend
from days