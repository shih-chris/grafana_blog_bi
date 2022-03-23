with

fct_trips as (

    select *
    from  {{ ref('fct_trips') }}

),

dim_stations as (

    select *
    from {{ ref('dim_stations') }}

),

starting_trips as (

    select
        fct_trips.start_station_id
        ,fct_trips.date_stamp

        -- daily trip details
        ,count(distinct fct_trips.trip_id) as total_trips
        ,sum(fct_trips.trip_duration) as total_trip_duration

    from
        fct_trips
    group by
        1,2

),

final as (


)

select *
from final
