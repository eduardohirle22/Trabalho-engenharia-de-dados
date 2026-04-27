-- models/marts/agg_demanda_por_hora.sql
-- Demanda de passageiros por estação × hora × dia da semana
-- Alimenta o mapa de calor de demanda no Superset

{{ config(materialized='table') }}

SELECT
    station_id                              AS estacao,
    event_date                              AS data,
    hora_do_dia,
    DAYOFWEEK(event_date)                   AS dia_da_semana,  -- 0=Dom, 6=Sab
    CASE DAYOFWEEK(event_date)
        WHEN 0 THEN 'Domingo'
        WHEN 1 THEN 'Segunda'
        WHEN 2 THEN 'Terça'
        WHEN 3 THEN 'Quarta'
        WHEN 4 THEN 'Quinta'
        WHEN 5 THEN 'Sexta'
        WHEN 6 THEN 'Sábado'
    END                                     AS nome_dia,
    SUM(total_eventos)                      AS total_validacoes,
    SUM(CASE WHEN direction = 'ENTRY' THEN total_eventos ELSE 0 END) AS entradas,
    SUM(CASE WHEN direction = 'EXIT'  THEN total_eventos ELSE 0 END) AS saidas,
    SUM(receita_total)                      AS receita_total,
    SUM(passageiros_unicos)                 AS passageiros_unicos,
    CASE
        WHEN hora_do_dia BETWEEN 6  AND 9  THEN 'pico_manha'
        WHEN hora_do_dia BETWEEN 17 AND 20 THEN 'pico_tarde'
        WHEN hora_do_dia BETWEEN 10 AND 16 THEN 'fora_pico'
        ELSE 'noturno'
    END                                     AS periodo,
    CURRENT_TIMESTAMP                       AS _dbt_updated_at
FROM {{ ref('int_passageiros_por_hora') }}
GROUP BY 1, 2, 3, 4, 5
ORDER BY data DESC, estacao, hora_do_dia