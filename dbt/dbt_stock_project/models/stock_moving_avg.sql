{{ config(materialized='table') }}

SELECT 
    Symbol,
    Date,
    Close,
    AVG(Close) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma_7day
FROM {{ source('raw', 'market_data') }}
WHERE Close IS NOT NULL