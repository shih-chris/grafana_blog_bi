with

citibike_stations as (

    select *
    from {{ ref('stg_citibike_stations') }}

),

missing_region_ids as (

    select *
    from {{ ref('seed_missing_station_region_ids')}}

),

region_names as (

    select *
    from {{ ref('seed_region_names')}}

),

final as (

    select
        citibike_stations.station_id
        ,citibike_stations.station_name
        ,citibike_stations.station_short_name

        ,citibike_stations.station_latitude
        ,citibike_stations.station_longitude
        ,coalesce(citibike_stations.station_region_id, missing_region_ids.region_id) as station_region_id
        ,region_names.region_name as station_region_name

        ,citibike_stations.station_rental_methods
        ,citibike_stations.station_capacity

    from
        citibike_stations
        left join missing_region_ids
            on citibike_stations.station_id = missing_region_ids.station_id
        left join region_names
            on coalesce(citibike_stations.station_region_id, missing_region_ids.region_id) = region_names.region_id

)

select *
from final
