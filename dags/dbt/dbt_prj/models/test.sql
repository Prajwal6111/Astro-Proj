{{ config(materialized='table',
alias = 'test') }}
select *
from WEATHER_SILVER.WEATHER_API