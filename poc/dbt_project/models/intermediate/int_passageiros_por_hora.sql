-- models/intermediate/int_passageiros_por_hora.sql
-- Agrega eventos de catraca por estação e hora do dia

{{ config(materialized='view') }}

SELECT
    station_id,
    event_date,
    DATE_PART('hour', event_ts)   AS hora_do_dia,
    direction,
    card_type,
    COUNT(*)                      AS total_eventos,
    SUM(fare_paid)                AS receita_total,
    AVG(fare_paid)                AS ticket_medio,
    COUNT(DISTINCT card_hash)     AS passageiros_unicos
FROM {{ ref('stg_catracas') }}
GROUP BY 1, 2, 3, 4, 5