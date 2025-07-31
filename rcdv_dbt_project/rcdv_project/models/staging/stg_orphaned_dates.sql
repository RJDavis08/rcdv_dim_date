with 
/*
    STG ORPHANED DATES

*/
source as (

    select * from {{ ref("date_orphaned_keys") }}

),

renamed as (

SELECT
    CAST(date_key AS BIGINT)                     AS date_key,
    CAST(full_date AS DATE)                      AS full_date,
    CAST(day_of_month AS DOUBLE PRECISION)       AS day_of_month,
    month_of_year                                AS month_of_year,
    year_number                                  AS year_number,
    day_of_week_name                             AS day_of_week_name,
    day_of_week_name_short                       AS day_of_week_name_short,
    CAST(week_start_date AS DATE)                AS week_start_date,
    CAST(week_end_date AS DATE)                  AS week_end_date,
    week_of_year                                 AS week_of_year,
    CAST(day_of_week AS NUMERIC)                 AS day_of_week,
    day_of_week_iso                              AS day_of_week_iso,
    CAST(iso_week_start_date AS DATE)            AS iso_week_start_date,
    CAST(iso_week_end_date AS DATE)              AS iso_week_end_date,
    iso_week_of_year                             AS iso_week_of_year,
    CAST(days_in_month AS NUMERIC)               AS days_in_month,
    CAST(day_of_year AS DOUBLE PRECISION)        AS day_of_year,
    CAST(days_in_year AS DOUBLE PRECISION)       AS days_in_year,
    month_name                                   AS month_name,
    month_name_short                             AS month_name_short,
    CAST(month_start_date AS DATE)               AS month_start_date,
    CAST(month_end_date AS DATE)                 AS month_end_date,
    CAST(next_date_day AS DATE)                  AS next_date_day,
    quarter_of_year                              AS quarter_of_year,
    CAST(quarter_start_date AS DATE)             AS quarter_start_date,
    CAST(quarter_end_date AS DATE)               AS quarter_end_date,
    CAST(year_start_date AS DATE)                AS year_start_date,
    CAST(year_end_date AS DATE)                  AS year_end_date,
    CAST(prior_year_date_day AS DATE)            AS prior_year_date_day,
    CAST(prior_year_over_year_date_day AS DATE)  AS prior_year_over_year_date_day,
    CAST(prior_date_day AS DATE)                 AS prior_date_day,
    CAST(prior_year_week_start_date AS DATE)     AS prior_year_week_start_date,
    CAST(prior_year_week_end_date AS DATE)       AS prior_year_week_end_date,
    CAST(prior_year_iso_week_start_date AS DATE) AS prior_year_iso_week_start_date,
    CAST(prior_year_iso_week_end_date AS DATE)   AS prior_year_iso_week_end_date,
    prior_year_week_of_year                      AS prior_year_week_of_year,
    prior_year_iso_week_of_year                  AS prior_year_iso_week_of_year,
    CAST(prior_year_month_start_date AS DATE)    AS prior_year_month_start_date,
    CAST(prior_year_month_end_date AS DATE)      AS prior_year_month_end_date,
    CAST(CASE WHEN day_offset THEN 1 ELSE 0 END AS INTEGER) AS day_offset,
    CAST(CASE WHEN week_offset THEN 1 ELSE 0 END AS INTEGER) AS week_offset,
    CAST(CASE WHEN quarter_offset THEN 1 ELSE 0 END AS NUMERIC) AS quarter_offset,
    CAST(CASE WHEN year_offset THEN 1 ELSE 0 END AS NUMERIC) AS year_offset,
    is_current_day                               AS is_current_day,
    is_current_week                              AS is_current_week,
    is_current_month                             AS is_current_month,
    is_current_quarter                           AS is_current_quarter,
    is_current_year                              AS is_current_year,
    is_first_day_of_month                        AS is_first_day_of_month,
    is_last_day_of_month                         AS is_last_day_of_month,
    is_first_day_of_quarter                      AS is_first_day_of_quarter,
    is_last_day_of_quarter                       AS is_last_day_of_quarter,
    is_first_day_of_year                         AS is_first_day_of_year,
    is_last_day_of_year                          AS is_last_day_of_year,
    is_leap_year                                 AS is_leap_year
FROM source

)

select * from renamed