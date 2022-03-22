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
        -- date details
        utils_date.date_stamp
        ,utils_date.day_name
        ,utils_date.is_weekday

        -- daily trip stats
        ,count(distinct citibike_trips.trip_id) as total_trips
        ,sum(citibike_trips.trip_duration) as total_trip_duration
        ,1.0 * sum(citibike_trips.trip_duration) / count(distinct citibike_trips.trip_id) as average_trip_duration

        -- am trip stats
        ,count(distinct case when extract(citibike_trips.trip_started_at, hour) < 12 then citibike_trips.trip_id) as am_trips
        ,sum(distinct case when extract(citibike_trips.trip_started_at, hour) < 12 then citibike_trips.trip_duration) as am_trip_duration
        ,1.0 * sum(distinct case when extract(citibike_trips.trip_started_at, hour) < 12 then citibike_trips.trip_duration) /
            count(distinct case when extract(citibike_trips.trip_started_at, hour) < 12 then citibike_trips.trip_id)
            as am_average_trip_duration

        -- pm trip stats
        ,count(distinct case when extract(citibike_trips.trip_started_at, hour) >= 12 then citibike_trips.trip_id) as pm_trips
        ,sum(distinct case when extract(citibike_trips.trip_started_at, hour) >= 12 then citibike_trips.trip_duration) as pm_trip_duration
        ,1.0 * sum(distinct case when extract(citibike_trips.trip_started_at, hour) >= 12 then citibike_trips.trip_duration) /
            count(distinct case when extract(citibike_trips.trip_started_at, hour) >= 12 then citibike_trips.trip_id)
            as pm_average_trip_duration

    from
        citibike_trips
        inner join utils_date on date_trunc(citibike_trips.trip_started_at, day) = utils_date.date_stamp
    group by 1,2,3

)

select *
from final
