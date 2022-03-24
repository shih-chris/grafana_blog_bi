with

fct_trips as (

    select *
    from  {{ ref('fct_trips') }}

),

dim_stations as (

    select *
    from {{ ref('dim_stations') }}

),

utils_date as (

    select *
    from {{ ref('utils_date') }}
    where date_stamp between '2017-04-01' and '2018-05-31'

),

starting_trips as (

    select
        fct_trips.start_station_id
        ,fct_trips.date_stamp

        -- daily trip details
        ,count(distinct fct_trips.trip_id) as total_trips
        ,sum(fct_trips.trip_duration_minutes) as total_trip_duration_minutes
        ,sum(fct_trips.trip_duration_minutes) / count(distinct fct_trips.trip_id) as average_trip_duration_minutes

    from
        fct_trips
    group by
        1,2

),

ending_trips as (

    select
        fct_trips.end_station_id
        ,fct_trips.date_stamp

        -- daily trip details
        ,count(distinct fct_trips.trip_id) as total_trips
        ,sum(fct_trips.trip_duration_minutes) as total_trip_duration_minutes
        ,sum(fct_trips.trip_duration_minutes) / count(distinct fct_trips.trip_id) as average_trip_duration_minutes

    from
        fct_trips
    group by
        1,2

),

final as (

    select
        -- date details
        utils_date.date_stamp
        ,utils_date.day_name
        ,utils_date.is_weekday

        -- station details
        ,dim_stations.station_id
        ,dim_stations.station_name

        ,dim_stations.station_latitude
        ,dim_stations.station_longitude
        ,dim_stations.station_region_name

        ,dim_stations.station_rental_methods
        ,dim_stations.station_capacity

        -- trip metrics
        ,starting_trips.total_trips as starting_trips
        ,starting_trips.total_trip_duration_minutes as starting_trip_duration_minutes
        ,starting_trips.average_trip_duration_minutes as starting_average_trip_duration_minutes

        ,ending_trips.total_trips as ending_trips
        ,ending_trips.total_trip_duration_minutes as ending_trip_duration_minutes
        ,ending_trips.average_trip_duration_minutes as ending_average_trip_duration_minutes

    from
        dim_stations
        cross join utils_date
        left join starting_trips
            on dim_stations.station_id = starting_trips.start_station_id
            and utils_date.date_stamp = starting_trips.date_stamp
        left join ending_trips
            on dim_stations.station_id = ending_trips.end_station_id
            and utils_date.date_stamp = ending_trips.date_stamp

)

select *
from final
