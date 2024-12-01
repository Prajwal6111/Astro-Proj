{{ config(materialized='table',
alias = 'weather_api') }}

WITH base_data AS (
    SELECT 
        id,
        parse_json(DATA) AS json_data
    FROM 
        {{ source('WEATHER_RAW', 'weather_raw_data') }}
),
flattened_weather AS (
    SELECT 
          MD5(CONCAT(
            json_data:main.temp::STRING, '-', 
            json_data:main.feels_like::STRING, '-', 
            json_data:main.pressure::STRING)) AS id,
        json_data:base::STRING AS base,
        -- Main weather metrics
        json_data:main.temp::FLOAT AS temperature,
        json_data:main.feels_like::FLOAT AS feels_like_temperature,
        -- json_data:main.temp_min::FLOAT AS min_temperature,
        -- json_data:main.temp_max::FLOAT AS max_temperature,
        json_data:main.pressure::INT AS pressure,
        json_data:main.humidity::INT AS humidity,
        -- json_data:main.sea_level::INT AS sea_level_pressure,
        -- json_data:main.grnd_level::INT AS ground_level_pressure,

        -- -- Visibility and wind information
        -- json_data:visibility::INT AS visibility,
        -- json_data:wind.speed::FLOAT AS wind_speed,
        -- json_data:wind.deg::INT AS wind_direction,
        -- json_data:wind.gust::FLOAT AS wind_gust,

        -- -- Cloud cover
        -- json_data:clouds.all::INT AS cloud_coverage,

        -- -- Date and system information
        -- json_data:dt::INT AS datetime,
        -- json_data:sys.country::STRING AS country,
        -- json_data:sys.sunrise::INT AS sunrise,
        -- json_data:sys.sunset::INT AS sunset,

        -- -- Other metadata
        -- json_data:timezone::INT AS timezone_offset,
        -- json_data:id::INT AS location_id,
        -- json_data:name::STRING AS location_name,
        -- json_data:cod::INT AS response_code,
        -- CURRENT_TIMESTAMP AS inserted_at

    FROM 
        base_data base
        -- LATERAL FLATTEN(input => base.json_data:weather) weather,
        -- LATERAL FLATTEN(input => base.json_data:main) main,
        -- LATERAL FLATTEN(input => base.json_data:wind) wind,
        -- LATERAL FLATTEN(input => base.json_data:clouds) clouds,
        -- LATERAL FLATTEN(input => base.json_data:sys) sys
)
SELECT * FROM flattened_weather
