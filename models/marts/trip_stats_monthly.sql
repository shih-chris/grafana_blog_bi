with 

citibike_trips as (

    select *
    from {{ ref('stg_citibike_trips') }}

),

final as (

    select
        
    from
        citibike_trips

)

select *
from final
