-- models/staging/stg_gps_onibus.sql
-- Lê Parquet Silver de GPS e padroniza colunas para a camada intermediária

{{ config(materialized='view') }}

SELECT
    vehicle_id,
    line_id,
    direction,
    CAST(lat          AS DOUBLE)    AS lat,
    CAST(lon          AS DOUBLE)    AS lon,
    CAST(speed_kmh    AS DOUBLE)    AS speed_kmh,
    CAST(occupancy_pct AS INTEGER)  AS occupancy_pct,
    CAST(is_outlier   AS BOOLEAN)   AS is_outlier,
    status,
    CAST(timestamp    AS TIMESTAMPTZ) AS event_ts,
    DATE_TRUNC('day', CAST(timestamp AS TIMESTAMPTZ)) AS event_date,
    tipo                            AS vehicle_type,
    CAST(capacidade   AS INTEGER)   AS vehicle_capacity,
    _processed_at,
    _source
FROM read_parquet('/home/claude/poc/silver/gps_onibus_clean/**/*.parquet')
WHERE vehicle_id IS NOT NULL
  AND timestamp  IS NOT NULL