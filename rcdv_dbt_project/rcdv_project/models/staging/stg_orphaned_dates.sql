with 
/*
    STG ORPHANED DATES

*/
source as (

    select * from {{ ref("date_orphaned_keys") }}

),

renamed as (

select
    cast(date_key as bigint)                     as date_key,
    cast(full_date as date)                      as full_date,
    cast(day_of_month as double precision)       as day_of_month,
    month_of_year                                as month_of_year,
    year_number                                  as year_number,
    day_of_week_name                             as day_of_week_name,
    day_of_week_name_short                       as day_of_week_name_short,
    cast(week_start_date as date)                as week_start_date,
    cast(week_end_date as date)                  as week_end_date,
    week_of_year                                 as week_of_year,
    cast(day_of_week as numeric)                 as day_of_week,
    day_of_week_iso                              as day_of_week_iso,
    cast(iso_week_start_date as date)            as iso_week_start_date,
    cast(iso_week_end_date as date)              as iso_week_end_date,
    iso_week_of_year                             as iso_week_of_year,
    cast(days_in_month as numeric)               as days_in_month,
    cast(day_of_year as double precision)        as day_of_year,
    cast(days_in_year as double precision)       as days_in_year,
    month_name                                   as month_name,
    month_name_short                             as month_name_short,
    cast(month_start_date as date)               as month_start_date,
    cast(month_end_date as date)                 as month_end_date,
    cast(next_date_day as date)                  as next_date_day,
    quarter_of_year                              as quarter_of_year,
    cast(quarter_start_date as date)             as quarter_start_date,
    cast(quarter_end_date as date)               as quarter_end_date,
    cast(year_start_date as date)                as year_start_date,
    cast(year_end_date as date)                  as year_end_date,
    cast(prior_year_date_day as date)            as prior_year_date_day,
    cast(prior_year_over_year_date_day as date)  as prior_year_over_year_date_day,
    cast(prior_date_day as date)                 as prior_date_day,
    cast(prior_year_week_start_date as date)     as prior_year_week_start_date,
    cast(prior_year_week_end_date as date)       as prior_year_week_end_date,
    cast(prior_year_iso_week_start_date as date) as prior_year_iso_week_start_date,
    cast(prior_year_iso_week_end_date as date)   as prior_year_iso_week_end_date,
    prior_year_week_of_year                      as prior_year_week_of_year,
    prior_year_iso_week_of_year                  as prior_year_iso_week_of_year,
    cast(prior_year_month_start_date as date)    as prior_year_month_start_date,
    cast(prior_year_month_end_date as date)      as prior_year_month_end_date,
    cast(case when day_offset then 1 else 0 end as integer) as day_offset,
    cast(case when week_offset then 1 else 0 end as integer) as week_offset,
    cast(case when quarter_offset then 1 else 0 end as numeric) as quarter_offset,
    cast(case when year_offset then 1 else 0 end as numeric) as year_offset,
    is_current_day                               as is_current_day,
    is_current_week                              as is_current_week,
    is_current_month                             as is_current_month,
    is_current_quarter                           as is_current_quarter,
    is_current_year                              as is_current_year,
    is_first_day_of_month                        as is_first_day_of_month,
    is_last_day_of_month                         as is_last_day_of_month,
    is_first_day_of_quarter                      as is_first_day_of_quarter,
    is_last_day_of_quarter                       as is_last_day_of_quarter,
    is_first_day_of_year                         as is_first_day_of_year,
    is_last_day_of_year                          as is_last_day_of_year,
    is_leap_year                                 as is_leap_year
from source

)

select * from renamed