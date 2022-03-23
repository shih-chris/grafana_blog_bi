with 

fct_trips as (

    select *
    from {{ ref('fct_trips') }}

),

final as (

    select
        -- date details
        fct_trips.date_stamp
        ,fct_trips.day_name
        ,fct_trips.is_weekday

        ,fct_trips.user_type

        -- daily trip stats
        ,count(distinct fct_trips.trip_id) as total_trips
        ,count(distinct fct_trips.bike_id) as total_unique_bikes
        ,sum(fct_trips.trip_duration) as total_trip_duration
        ,1.0 * sum(fct_trips.trip_duration) / count(distinct fct_trips.trip_id) as average_trip_duration

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
        fct_trips
    group by 1,2,3,4

)

select *
from final
