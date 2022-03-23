with

source as (

    select *
    from {{ source('new_york_citibike', 'citibike_trips') }}

    -- limit amount of data stored / processed for our example
    where date(starttime) between '2017-04-01' and '2018-05-31'

),

renamed as (

    select
        -- Bike / User details
        safe_cast(bikeid as int) as bike_id
        ,safe_cast(usertype as string) as user_type
        ,safe_cast(birth_year as int) as birth_year
        ,safe_cast(gender as string) as gender

        -- Trip details
        ,safe_cast(tripduration as int) as trip_duration
        ,safe_cast(starttime as datetime) as trip_started_at
        ,safe_cast(stoptime as datetime) as trip_ended_at

        -- Start station details
        ,safe_cast(start_station_id as int) as start_station_id
        ,safe_cast(start_station_name as string) as start_station_name
        ,safe_cast(start_station_latitude as numeric) as start_station_latitude
        ,safe_cast(start_station_longitude as numeric) as start_station_longitude

        -- End station details
        ,safe_cast(end_station_id as int) as end_station_id
        ,safe_cast(end_station_name as string) as end_station_name
        ,safe_cast(end_station_latitude as numeric) as end_station_latitude
        ,safe_cast(end_station_longitude as numeric) as end_station_longitude

    from
        source

),

final as (

    select
        md5(bike_id || start_station_id || trip_started_at || trip_ended_at) as trip_id
        ,renamed.*
    from
        renamed

)

select *
from final

