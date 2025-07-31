with 
/*
    STG ORPHANED DATES

*/
source as (

    select * from {{ ref("date_orphaned_keys") }}

),

renamed as (

    SELECT
        date_key,
        full_date,
        day_of_month,
        month_of_year,
        year_number,
        day_of_week_name,
        day_of_week_name_short,
        week_start_date,
        week_end_date,
        week_of_year,
        day_of_week,
        day_of_week_iso,
        iso_week_start_date,
        iso_week_end_date,
        iso_week_of_year,
        days_in_month,
        day_of_year,
        days_in_year,
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
        day_offset,
        week_offset,
        quarter_offset,
        year_offset,
        is_current_day_yn,
        is_current_week_yn,
        is_current_month_yn,
        is_current_quarter_yn,
        is_current_year_yn,
        is_first_day_of_month_yn,
        is_last_day_of_month_yn,
        is_first_day_of_quarter_yn,
        is_last_day_of_quarter_yn,
        is_first_day_of_year_yn,
        is_last_day_of_year_yn,
        is_leap_year_yn
    FROM source
    where date_key is not null

)

select * from renamed