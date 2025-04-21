{% snapshot snapshot_stock_moving_avg %}

{{
  config(
    target_schema='GRIZZLY_ANALYTICS',
    unique_key='symbol || \'-\' || to_varchar(date)',
    strategy='timestamp',
    updated_at='date',
    invalidate_hard_deletes=True
  )
}}

SELECT
  symbol,
  date,
  close,
  ma_7day
FROM {{ ref('stock_moving_avg') }}

{% endsnapshot %}
