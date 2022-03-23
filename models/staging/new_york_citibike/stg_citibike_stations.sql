with

source as (

    select *
    from {{ source('new_york_citibike', 'citibike_stations') }}

),

renamed as (

    select
        -- Station ID / Names
        safe_cast(station_id as int) as station_id
        ,safe_cast(name as string) as station_name
        ,safe_cast(short_name as string) as station_short_name
        
        -- Station location
        ,safe_cast(latitude as numeric) as station_latitude
        ,safe_cast(longitude as numeric) as station_longitude
        ,safe_cast(region_id as int) as station_region_id

        -- Station details
        ,safe_cast(rental_methods as string) as station_rental_methods
        ,safe_cast(capacity as int) as station_capacity

    from
        source

)

select *
from renamed
