-- models/staging/stg_gps_onibus.sql
-- Staging: GPS Silver → tipagem e aliases padronizados

{{
  config(
    materialized = 'view',
    description  = 'Eventos GPS dos ônibus vindos da camada Silver (Parquet)'
  )
}}

WITH fonte AS (
    SELECT *
    FROM read_parquet(
        '{{ var("silver_dir") }}/gps_onibus_clean/**/*.parquet'
    )
),

tipado AS (
    SELECT
        vehicle_id::VARCHAR            AS vehicle_id,
        line_id::VARCHAR               AS line_id,
        direction::VARCHAR             AS direction,
        lat::DOUBLE                    AS lat,
        lon::DOUBLE                    AS lon,
        speed_kmh::DOUBLE              AS speed_kmh,
        occupancy_pct::INTEGER         AS occupancy_pct,
        engine_on::BOOLEAN             AS engine_on,
        door_open::BOOLEAN             AS door_open,
        status::VARCHAR                AS status,
        is_outlier::BOOLEAN            AS is_outlier,
        tipo::VARCHAR                  AS tipo_veiculo,
        capacidade::INTEGER            AS capacidade_passageiros,
        timestamp::TIMESTAMPTZ         AS evento_ts,
        _processed_at::TIMESTAMPTZ     AS processado_em,
        _source::VARCHAR               AS fonte,

        -- Dimensões de tempo
        DATE_TRUNC('hour', timestamp)  AS hora_evento,
        CAST(timestamp AS DATE)        AS data_evento,
        EXTRACT(hour FROM timestamp)   AS hora_do_dia,
        CASE
            WHEN EXTRACT(hour FROM timestamp) BETWEEN 6 AND 9   THEN 'manha_pico'
            WHEN EXTRACT(hour FROM timestamp) BETWEEN 17 AND 20 THEN 'tarde_pico'
            WHEN EXTRACT(hour FROM timestamp) BETWEEN 10 AND 16 THEN 'entrepico'
            ELSE 'noturno'
        END AS periodo_dia
    FROM fonte
    WHERE vehicle_id IS NOT NULL
      AND lat BETWEEN -90 AND 90
      AND lon BETWEEN -180 AND 180
)

SELECT * FROM tipado
