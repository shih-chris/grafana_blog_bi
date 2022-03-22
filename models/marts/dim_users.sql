with

citibike_trips as (

    select *
    from {{ ref('stg_new_york_citibike') }}

),

final as (

    select

    from
        citibike_trips

)

select *
from final
