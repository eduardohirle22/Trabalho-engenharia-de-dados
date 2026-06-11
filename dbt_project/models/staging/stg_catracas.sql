-- models/staging/stg_catracas.sql
-- Staging: Catracas Silver → tipagem e aliases padronizados

{{
  config(
    materialized = 'view',
    description  = 'Eventos de validação de bilhetes nas estações do VLT'
  )
}}

WITH fonte AS (
    SELECT *
    FROM read_parquet(
        '{{ var("silver_dir") }}/catracas_clean/**/*.parquet'
    )
),

tipado AS (
    SELECT
        event_id::VARCHAR          AS event_id,
        gate_id::VARCHAR           AS gate_id,
        station_id::VARCHAR        AS station_id,
        direction::VARCHAR         AS direction,
        card_type::VARCHAR         AS card_type,
        card_hash::VARCHAR         AS card_hash,
        fare_paid::DOUBLE          AS fare_paid_brl,
        timestamp::TIMESTAMPTZ     AS evento_ts,
        _processed_at::TIMESTAMPTZ AS processado_em,

        CAST(timestamp AS DATE)    AS data_evento,
        EXTRACT(hour FROM timestamp) AS hora_do_dia,
        CASE
            WHEN EXTRACT(hour FROM timestamp) BETWEEN 6  AND 9  THEN 'manha_pico'
            WHEN EXTRACT(hour FROM timestamp) BETWEEN 17 AND 20 THEN 'tarde_pico'
            WHEN EXTRACT(hour FROM timestamp) BETWEEN 10 AND 16 THEN 'entrepico'
            ELSE 'noturno'
        END AS periodo_dia,

        -- Flag de gratuidade
        CASE WHEN fare_paid = 0 THEN TRUE ELSE FALSE END AS eh_gratuidade
    FROM fonte
    WHERE event_id IS NOT NULL
      AND direction IN ('ENTRY', 'EXIT')
)

SELECT * FROM tipado
