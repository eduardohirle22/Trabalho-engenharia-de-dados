-- models/staging/stg_catracas.sql
-- Lê Parquet Silver de catracas e padroniza tipos

{{ config(materialized='view') }}

SELECT
    event_id,
    gate_id,
    station_id,
    direction,
    card_type,
    card_hash,
    CAST(fare_paid    AS DECIMAL(6,2)) AS fare_paid,
    CAST(timestamp    AS TIMESTAMPTZ)  AS event_ts,
    DATE_TRUNC('day', CAST(timestamp AS TIMESTAMPTZ)) AS event_date,
    _processed_at,
    _source
FROM read_parquet('/home/claude/poc/silver/catracas_clean/**/*.parquet')
WHERE event_id    IS NOT NULL
  AND station_id  IS NOT NULL
  AND card_hash   IS NOT NULL
  AND direction IN ('ENTRY', 'EXIT')