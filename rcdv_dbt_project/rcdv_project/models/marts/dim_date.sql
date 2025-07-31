/*
    Author:      Richard Davis
    Description: Generates a conformed date dimension table based on stg_date_pkg and includes enriched fields, offsets, and orphan handling.
    Notes:       For detailed change history, refer to Git commit history or PR logs.

*/

{{ config(
    materialized = "table",
    sort = "date_key",
    dist = "date_key"
) }}

{% set current_date = var('current_date', run_started_at.strftime('%Y-%m-%d')) %}
{% set current_date_casted = "cast('" ~ current_date ~ "' as date)" %}

with stg_dim_date as (
    select {{ dbt_utils.star(ref("stg_date_pkg")) }} from {{ ref("stg_date_pkg") }}
),

orphaned as (
    select {{ dbt_utils.star(ref("stg_orphaned_dates")) }} from {{ ref("stg_orphaned_dates") }}
),

base_date as (
    select
        c.*,
        extract(dow from c.date_day) as dow,
        extract(day from c.month_end_date) as days_in_month,
        extract(year from c.date_day) as date_year,
        extract(quarter from c.date_day) as date_quarter,
        extract(year from {{ current_date_casted }}) as current_year,
        extract(quarter from {{ current_date_casted }}) as current_quarter
    from stg_dim_date as c
),

enriched_date as (
    select
        row_number() over (order by date_day asc) - 1 as date_key,
        date_day as full_date,
        day_of_month,
        month_of_year,
        year_number,
        day_of_week_name,
        day_of_week_name_short,
        week_start_date,
        week_end_date,
        week_of_year,

        -- sunday = 1 ... saturday = 7
        case dow when 0 then 1 else dow + 1 end as day_of_week,
        day_of_week as day_of_week_iso,

        iso_week_start_date,
        iso_week_end_date,
        iso_week_of_year,
        days_in_month,
        day_of_year,
        max(day_of_year) over (partition by year_number) as days_in_year,
        month_name,
        month_name_short,
        month_start_date,
        month_end_date,
        next_date_day,
        quarter_of_year,
        quarter_start_date,
        quarter_end_date,
        year_start_date,
        year_end_date,
        prior_year_date_day,
        prior_year_over_year_date_day,
        prior_date_day,
        prior_year_week_start_date,
        prior_year_week_end_date,
        prior_year_iso_week_start_date,
        prior_year_iso_week_end_date,
        prior_year_week_of_year,
        prior_year_iso_week_of_year,
        prior_year_month_start_date,
        prior_year_month_end_date,

        -- offsets
        cast(date_day - {{ current_date_casted }} as integer) as day_offset,
        cast((date_day - {{ current_date_casted }}) / 7 as integer) as week_offset,
        ((date_year - current_year) * 4 + (date_quarter - current_quarter)) as quarter_offset,
        (date_year - current_year) as year_offset
    from base_date
),

date_flags as (
    select
        *,
        -- current period flags
        full_date = {{ current_date_casted }} as is_current_day,
        week_of_year = extract(week from {{ current_date_casted }}) and year_number = extract(year from {{ current_date_casted }}) as is_current_week,
        month_of_year = extract(month from {{ current_date_casted }}) and year_number = extract(year from {{ current_date_casted }}) as is_current_month,
        quarter_of_year = extract(quarter from {{ current_date_casted }}) and year_number = extract(year from {{ current_date_casted }}) as is_current_quarter,
        year_number = extract(year from {{ current_date_casted }}) as is_current_year,

        -- date boundaries
        extract(day from month_start_date) = day_of_month as is_first_day_of_month,
        extract(day from month_end_date) = day_of_month as is_last_day_of_month,
        full_date = quarter_start_date as is_first_day_of_quarter,
        full_date = quarter_end_date as is_last_day_of_quarter,
        full_date = year_start_date as is_first_day_of_year,
        full_date = year_end_date as is_last_day_of_year,

        -- leap year check
        (year_number % 4 = 0 and (year_number % 100 != 0 or year_number % 400 = 0)) as is_leap_year
    from enriched_date
)

-- Final result: union of enriched date rows and seeded orphaned rows
select * from orphaned
union all
select * from date_flags