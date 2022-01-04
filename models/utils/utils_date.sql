with date_data as (

    select
        *
    from
        UNNEST(GENERATE_DATE_ARRAY('2013-01-01', '2050-01-01', INTERVAL 1 DAY)) as date_stamp

),

utils_date as (

    SELECT
        date_stamp
        
        -- month details
        ,date_trunc(date_stamp, month) as month_start_date
        ,case when date_stamp = last_day(date_stamp, month) then TRUE else FALSE end as is_last_day_of_month
        ,cast(format_date("%b'%g", date_stamp) as string) as formatted_month_name
        ,cast(extract(month from date_stamp) as int) as month_number
        ,cast(format_date('%B', date_stamp) as string) as month_name
        ,cast(extract(day from date_stamp) as int) as day_of_month
        

        -- quarter details
        ,cast(extract(quarter from date_stamp) as int) as quarter_number

        -- year details
        ,cast(extract(year from date_stamp) as int) as year
        ,cast(extract(dayofyear from date_stamp) as int) as day_of_year

        -- week details
        ,cast(extract(week from date_stamp) as int) as week_of_year
        ,cast(format_date('%w', date_stamp) as int) as day_of_week
        ,case when format_date('%A', date_stamp) not in ('Sunday', 'Saturday') then TRUE else FALSE end as is_weekday
        ,cast(format_date('%A', date_stamp) as string) as day_name

FROM 
    date_data

)

select * from utils_date