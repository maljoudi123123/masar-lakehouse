with pings as (
    select
        distinct(trip_id),
        driver_id,
        city,
        try_cast(event_ts as timestamp) as event_at,
        lat as latitude,
        lon as longitude,
        _ingested_at,
        _source_file
    from {{ source('x_bronze', 'gps_pings') }}
)
select *
from pings
where trip_id is not null
  and event_at is not null
  and latitude between -90 and 90
  and longitude between -180 and 180
