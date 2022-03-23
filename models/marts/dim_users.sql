with

citibike_trips as (

    select *
    from {{ ref('stg_new_york_citibike') }}

),

final as (

    select
        bike_id
        ,birth_year
        ,gender
        ,trip_started_at

        -- lets us pull the most recent data on a bike_id's birth_year and gender
        ,row_number() over (partition by bike_id order by trip_started_at desc) as row_num
    from
        citibike_trips

)

select *
from final
where row_num = 1
