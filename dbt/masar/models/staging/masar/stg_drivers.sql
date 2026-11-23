with source as (
    select
        *,
        -- bronze holds each driver once per load; keep only the latest load
        row_number() over (partition by driver_id order by _ingested_at desc) as _rn
    from {{ source('bronze', 'drivers') }}
    where driver_id is not null
)
select
    cast(driver_id as string)       as driver_id,
    lower(trim(vehicle_type))       as vehicle_type,
    cast(driver_rating as double)   as driver_rating,
    initcap(trim(city))             as city
from source
where _rn = 1
