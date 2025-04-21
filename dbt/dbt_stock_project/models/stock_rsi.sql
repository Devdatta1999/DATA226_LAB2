{{ config(materialized='table') }}

WITH base AS (
  SELECT 
    Symbol,
    Date,
    Close,
    LAG(Close) OVER (PARTITION BY Symbol ORDER BY Date) AS prev_close
  FROM {{ source('GRIZZLY_RAW', 'market_data') }}
),
deltas AS (
  SELECT *,
    CASE WHEN Close - prev_close > 0 THEN Close - prev_close ELSE 0 END AS gain,
    CASE WHEN Close - prev_close < 0 THEN ABS(Close - prev_close) ELSE 0 END AS loss
  FROM base
),
avg_deltas AS (
  SELECT *,
    AVG(gain) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain,
    AVG(loss) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss
  FROM deltas
)
SELECT *,
  100 - (100 / (1 + (avg_gain / NULLIF(avg_loss, 0)))) AS rsi_14
FROM avg_deltas;
