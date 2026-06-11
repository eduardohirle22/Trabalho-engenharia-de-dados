-- models/marts/kpi_operacional_diario.sql
-- KPI operacional diário por linha de ônibus
-- Metrics: OTP, velocidade média, ocupação média, outliers

{{
  config(
    materialized = 'table',
    description  = 'KPIs operacionais diários por linha — principal modelo Gold'
  )
}}

WITH base AS (
    SELECT
        data_evento,
        line_id,
        direction,
        periodo_dia,
        COUNT(*)                             AS total_eventos,
        ROUND(AVG(speed_kmh), 2)             AS velocidade_media_kmh,
        ROUND(MAX(speed_kmh), 2)             AS velocidade_maxima_kmh,
        ROUND(AVG(occupancy_pct), 1)         AS ocupacao_media_pct,
        ROUND(MAX(occupancy_pct), 1)         AS ocupacao_maxima_pct,
        COUNT(DISTINCT vehicle_id)           AS veiculos_ativos,
        SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) AS eventos_outlier,
        SUM(CASE WHEN status = 'delayed'     THEN 1 ELSE 0 END) AS eventos_atrasados,
        SUM(CASE WHEN status = 'on_route'    THEN 1 ELSE 0 END) AS eventos_em_rota,
        SUM(CASE WHEN door_open             THEN 1 ELSE 0 END) AS paradas_porta_aberta
    FROM {{ ref('stg_gps_onibus') }}
    GROUP BY data_evento, line_id, direction, periodo_dia
),

com_otp AS (
    SELECT
        *,
        -- OTP: % de eventos on_route + at_stop (no horário)
        ROUND(
            100.0 * (eventos_em_rota) / NULLIF(total_eventos - eventos_outlier, 0),
            1
        ) AS otp_pct,

        -- Taxa de outliers
        ROUND(
            100.0 * eventos_outlier / NULLIF(total_eventos, 0),
            2
        ) AS taxa_outlier_pct,

        -- Classificação de performance
        CASE
            WHEN ROUND(100.0 * eventos_em_rota / NULLIF(total_eventos - eventos_outlier, 0), 1) >= 90
                THEN 'VERDE'
            WHEN ROUND(100.0 * eventos_em_rota / NULLIF(total_eventos - eventos_outlier, 0), 1) >= 70
                THEN 'AMARELO'
            ELSE 'VERMELHO'
        END AS semaforo_otp,

        -- Data e hora de processamento
        CURRENT_TIMESTAMP AS _calculado_em
    FROM base
)

SELECT * FROM com_otp
ORDER BY data_evento DESC, line_id, direction
