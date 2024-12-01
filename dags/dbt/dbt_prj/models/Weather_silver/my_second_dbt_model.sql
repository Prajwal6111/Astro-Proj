
-- Use the `ref` function to select from other models

select *
from  {{ source('WEATHER_RAW', 'weather_raw_data') }}

