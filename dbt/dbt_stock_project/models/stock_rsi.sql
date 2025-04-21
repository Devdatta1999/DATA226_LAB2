{{ config(materialized='table') }}

WITH base AS (
  SELECT 
    symbol,
    date,
    close,
    LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
  FROM {{ source('raw', 'market_data') }}
),

deltas AS (
  SELECT 
    *,
    CASE WHEN close - prev_close > 0 THEN close - prev_close ELSE 0 END AS gain,
    CASE WHEN close - prev_close < 0 THEN ABS(close - prev_close) ELSE 0 END AS loss
  FROM base
),

avg_deltas AS (
  SELECT 
    *,
    AVG(gain) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain,
    AVG(loss) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss
  FROM deltas
)

SELECT 
  symbol,
  date,
  close,
  ROUND(100 - (100 / (1 + (avg_gain / NULLIF(avg_loss, 0)))), 2) AS rsi_14
FROM avg_deltas
