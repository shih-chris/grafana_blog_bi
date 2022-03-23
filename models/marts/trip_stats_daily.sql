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
        utils_date.date_stamp
        ,count(distinct trip_id) as total_trips
        ,sum(trip_duration_seconds) / 60 as total_trip_duration_minutes
        ,1.0 * sum(trip_duration_seconds) / count(distinct trip_id) / 60 as average_trip_duration_minutes
    from
        citibike_trips
        inner join utils_date on date_trunc(citibike_trips.trip_started_at, day) = utils_date.date_stamp
    group by 1

)

select *
from final
