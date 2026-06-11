-- models/marts/agg_demanda_por_hora.sql
-- Demanda consolidada por estação × hora × período do dia

{{
  config(
    materialized = 'table',
    description  = 'Demanda de passageiros por estação e hora — base para dashboards'
  )
}}

WITH demanda AS (
    SELECT
        station_id,
        data_evento,
        hora_do_dia,
        periodo_dia,
        SUM(entradas)        AS total_entradas,
        SUM(saidas)          AS total_saidas,
        SUM(receita_brl)     AS receita_total_brl,
        SUM(gratuidades)     AS total_gratuidades,
        AVG(pct_gratuidades) AS media_pct_gratuidades
    FROM {{ ref('int_passageiros_por_hora') }}
    GROUP BY station_id, data_evento, hora_do_dia, periodo_dia
),

com_rank AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY data_evento ORDER BY total_entradas DESC) AS rank_entradas_dia,
        CURRENT_TIMESTAMP AS _calculado_em
    FROM demanda
)

SELECT * FROM com_rank
ORDER BY data_evento DESC, rank_entradas_dia
