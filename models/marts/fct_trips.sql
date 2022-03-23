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
        {{ dbt_utils.surrogate_id([
            'citibike_trips.bike_id'
            ,'citibike_trips.start_station_id'
            ,'citibike_trips.trip_started_at'
        ]) }} as trip_id
        ,citibike_trips.bike_id

        -- Trip details
        ,citibike_trips.trip_duration
        ,citibike_trips.trip_started_at
        ,citibike_trips.trip_ended_at

        -- Date / Time details
        ,utils_date.month_start_date
        ,utils_date.is_weekday

        -- Station ids
        ,citibike_trips.start_station_id
        ,citibike_trips.end_station_id

    from
        citibike_trips
        inner join utils_date on date_trunc(citibike_trips.trip_started_at, day) = utils_date.date_stamp


)

select *
from final
