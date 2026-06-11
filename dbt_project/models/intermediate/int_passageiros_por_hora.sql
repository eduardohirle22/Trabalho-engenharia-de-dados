-- models/intermediate/int_passageiros_por_hora.sql
-- Agrega entradas de passageiros por estação × hora

{{
  config(
    materialized = 'view',
    description  = 'Passageiros agregados por estação e hora (base para Gold)'
  )
}}

WITH entradas AS (
    SELECT
        station_id,
        data_evento,
        hora_do_dia,
        periodo_dia,
        COUNT(*)            AS total_eventos,
        SUM(CASE WHEN direction = 'ENTRY' THEN 1 ELSE 0 END) AS entradas,
        SUM(CASE WHEN direction = 'EXIT'  THEN 1 ELSE 0 END) AS saidas,
        SUM(fare_paid_brl)  AS receita_brl,
        SUM(CASE WHEN eh_gratuidade THEN 1 ELSE 0 END) AS gratuidades
    FROM {{ ref('stg_catracas') }}
    GROUP BY station_id, data_evento, hora_do_dia, periodo_dia
)

SELECT
    station_id,
    data_evento,
    hora_do_dia,
    periodo_dia,
    total_eventos,
    entradas,
    saidas,
    receita_brl,
    gratuidades,
    ROUND(100.0 * gratuidades / NULLIF(total_eventos, 0), 1) AS pct_gratuidades
FROM entradas
