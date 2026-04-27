-- models/marts/kpi_operacional_diario.sql
-- KPIs diários de operação: OTP, headway, taxa de ocupação
-- Tabela Gold consumida pelo Superset e DuckDB ad-hoc

{{ config(materialized='table') }}

WITH gps AS (
    SELECT
        event_date,
        line_id,
        vehicle_id,
        vehicle_type,
        vehicle_capacity,
        AVG(speed_kmh)          AS velocidade_media_kmh,
        AVG(occupancy_pct)      AS ocupacao_media_pct,
        MAX(occupancy_pct)      AS ocupacao_maxima_pct,
        COUNT(*)                AS leituras_gps,
        SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) AS leituras_outlier,
        -- OTP simplificado: veículos com velocidade média > 10 km/h estão "em operação"
        SUM(CASE WHEN speed_kmh > 10 AND status != 'out_of_service' THEN 1 ELSE 0 END)
            AS leituras_em_operacao
    FROM {{ ref('stg_gps_onibus') }}
    WHERE NOT is_outlier  -- exclui outliers do KPI principal
    GROUP BY 1, 2, 3, 4, 5
),

kpi_por_linha AS (
    SELECT
        event_date,
        line_id,
        vehicle_type,
        COUNT(DISTINCT vehicle_id)                  AS veiculos_ativos,
        AVG(velocidade_media_kmh)                   AS velocidade_media_linha_kmh,
        AVG(ocupacao_media_pct)                      AS ocupacao_media_linha_pct,
        MAX(ocupacao_maxima_pct)                     AS ocupacao_pico_pct,
        SUM(leituras_gps)                           AS total_leituras,
        SUM(leituras_outlier)                       AS total_outliers,
        -- OTP: % de leituras onde o veículo estava operando normalmente
        ROUND(
            100.0 * SUM(leituras_em_operacao) / NULLIF(SUM(leituras_gps), 0),
            2
        )                                           AS otp_pct
    FROM gps
    GROUP BY 1, 2, 3
)

SELECT
    event_date                              AS data,
    line_id                                 AS linha,
    vehicle_type                            AS tipo_veiculo,
    veiculos_ativos,
    ROUND(velocidade_media_linha_kmh, 1)    AS velocidade_media_kmh,
    ROUND(ocupacao_media_linha_pct, 1)      AS ocupacao_media_pct,
    ROUND(ocupacao_pico_pct, 1)             AS ocupacao_pico_pct,
    total_leituras,
    total_outliers,
    otp_pct,
    CASE
        WHEN otp_pct >= 90 THEN 'BOM'
        WHEN otp_pct >= 75 THEN 'REGULAR'
        ELSE 'CRITICO'
    END                                     AS classificacao_otp,
    CURRENT_TIMESTAMP                       AS _dbt_updated_at
FROM kpi_por_linha
ORDER BY data DESC, linha