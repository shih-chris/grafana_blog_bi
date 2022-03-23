with 

citibike_trips as (

    select *
    from {{ ref('stg_citibike_trips') }}

),

utils_date as (

    select *
    from {{ ref('utils_date') }}

),

final as (

    select
        -- ids
        citibike_trips.trip_id
        ,citibike_trips.bike_id

        -- Trip details
        ,utils_date.date_stamp
        ,citibike_trips.trip_started_at
        ,citibike_trips.trip_ended_at
        ,citibike_trips.trip_duration_seconds
        ,round(citibike_trips.trip_duration_seconds / 60, 2) as trip_duration_minutes
        ,citibike_trips.user_type

        -- Date / Time details
        ,utils_date.month_start_date
        ,utils_date.day_name
        ,utils_date.is_weekday

        -- Station ids
        ,citibike_trips.start_station_id
        ,citibike_trips.end_station_id

        -- Rider details
        ,citibike_trips.gender as rider_gender
        ,date_diff(utils_date.date_stamp, parse_date('%Y', citibike_trips.birth_year), year) as rider_age

    from
        citibike_trips
        inner join utils_date on date_trunc(citibike_trips.trip_started_at, day) = utils_date.date_stamp

)

select *
from final
