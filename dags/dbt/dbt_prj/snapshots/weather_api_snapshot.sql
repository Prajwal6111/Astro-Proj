{% snapshot weather_api_snapshot %}

    {{
        config(
            materialized='snapshot',
          target_database='silver',
          target_schema='snapshots',
          strategy='check',
          unique_key='id',
          check_cols=['temperature']
        )
    }}

    select source.id, 
            source.temperature, 
            source.base,
            source.feels_like_temperature,
            source.pressure,
            source.humidity,
           CASE
            WHEN source.id IS NULL THEN CURRENT_TIMESTAMP
            ELSE NULL
            END AS expired_at from {{ref('weather_api') }} as source

{% endsnapshot %}