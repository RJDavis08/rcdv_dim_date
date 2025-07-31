/*
    Author: Richard Davis
    Date:   28/04/2023
    Description:
    --------------------------------------------------------------------------------------------------------------------------
    Version     Changed By         JIRA            Description of Change
    --------------------------------------------------------------------------------------------------------------------------
    0.1         Richard Davis      NA              Create a dim_date using the DBT package and then extend it.
    0.2         Richard Davis      NA              Add dummy values -1 Unknown, -2 NA added to the Dimension.
    0.3         Richard Davis      NA              Used seeds to apply orphaned dimension keys to reduce LOC on this model.
    0.4         Richard Davis/CP   NA              Refactored for best practices and compatibility. Current date parameterised.
    0.5         Richard Davis/CP   NA              Code refactoring, bug fixes, style cleanup, logic improvements.
    0.6         Richard Davis      NA              base_date is now following an ANSI SQL standard, allowing for better compatibility.
*/

-- Snowflake specific materialization configuration
{{ config(materialized="table", sort="date_key", dist="date_key") }}

-- Parameterise current date for flexibility in testing and backfills
{% set current_date = var('current_date', run_started_at.strftime('%Y-%m-%d')) %}
{% set current_date_casted = "cast('" ~ current_date ~ "' as date)" %}

with stg_dim_date as (
    select * from {{ ref("stg_date_pkg") }} order by date_day asc
),

orphaned as (
    -- Select orphaned dates to ensure joins always return a match
    select * from {{ ref("stg_orphaned_dates") }}
),

base_date as (
with base as (
  select
    c.*,
    extract(dow from c.date_day) as dow,
    extract(day from c.month_end_date) as days_in_month,
    extract(year from c.date_day) as date_year,
    extract(quarter from c.date_day) as date_quarter,
    extract(year from current_date) as current_year,
    extract(quarter from current_date) as current_quarter
  from stg_dim_date as c
)
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
  -- sunday-based day of week (sunday = 1, saturday = 7)
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
  cast(date_day - current_date as integer) as day_offset,
  cast((date_day - current_date) / 7 as integer) as week_offset,
  ((date_year - current_year) * 4 + (date_quarter - current_quarter)) as quarter_offset,
  (date_year - current_year) as year_offset
from base
),

date_flags as (
    select
        *,
        case
            when full_date = {{ current_date_casted }} then 'Y'
            else 'N'
        end as is_current_day_yn,

        case
            when week_of_year = date_part('week', {{ current_date_casted }})
                 and year_number = extract(year from {{ current_date_casted }})
            then 'Y'
            else 'N'
        end as is_current_week_yn,

        case
            when month_of_year = extract(month from {{ current_date_casted }}) then 'Y'
            else 'N'
        end as is_current_month_yn,

        case
            when quarter_of_year = extract(quarter from {{ current_date_casted }}) then 'Y'
            else 'N'
        end as is_current_quarter_yn,

        case
            when year_number = extract(year from {{ current_date_casted }}) then 'Y'
            else 'N'
        end as is_current_year_yn,

        case
            when extract(day from month_start_date) = day_of_month then 'Y'
            else 'N'
        end as is_first_day_of_month_yn,

        case
            when extract(day from month_end_date) = day_of_month then 'Y'
            else 'N'
        end as is_last_day_of_month_yn,

        case
            when full_date = quarter_start_date then 'Y'
            else 'N'
        end as is_first_day_of_quarter_yn,

        case
            when full_date = quarter_end_date then 'Y'
            else 'N'
        end as is_last_day_of_quarter_yn,

        case
            when full_date = year_start_date then 'Y'
            else 'N'
        end as is_first_day_of_year_yn,

        case
            when full_date = year_end_date then 'Y'
            else 'N'
        end as is_last_day_of_year_yn,

        case
            when year_number % 4 = 0 and (year_number % 100 != 0 or year_number % 400 = 0) then 'Y'
            else 'N'
        end as is_leap_year_yn
    from base_date
)

-- Final select: union orphaned dates with enriched date dimension
-- Union all without column alignment to save writing and maintaining longforms as all columns will always be used from both tables
select * from orphaned
union all
select * from date_flags
