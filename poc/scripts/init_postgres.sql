-- UrbanFlow — Inicialização do banco legado de bilhetagem
-- Executado automaticamente pelo Docker na primeira inicialização

CREATE TABLE IF NOT EXISTS trips (
    trip_id        VARCHAR(36)  PRIMARY KEY,
    modal          VARCHAR(20)  NOT NULL CHECK (modal IN ('onibus', 'metro', 'bicicleta')),
    line_id        VARCHAR(20),
    origin_stop_id VARCHAR(20),
    dest_stop_id   VARCHAR(20),
    card_hash      VARCHAR(64),
    fare_paid      DECIMAL(6,2),
    trip_date      DATE         NOT NULL,
    departure_ts   TIMESTAMP,
    arrival_ts     TIMESTAMP,
    duration_min   SMALLINT,
    vehicle_id     VARCHAR(20),
    created_at     TIMESTAMP    DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trips_date  ON trips(trip_date);
CREATE INDEX IF NOT EXISTS idx_trips_modal ON trips(modal);
CREATE INDEX IF NOT EXISTS idx_trips_line  ON trips(line_id);

-- Inserir dados sintéticos de exemplo (últimos 3 dias)
INSERT INTO trips (trip_id, modal, line_id, origin_stop_id, dest_stop_id, card_hash, fare_paid, trip_date, departure_ts, arrival_ts, duration_min, vehicle_id)
SELECT
    gen_random_uuid()::varchar,
    (ARRAY['onibus','metro','bicicleta'])[floor(random()*3+1)],
    'L' || LPAD((floor(random()*120+1))::text, 3, '0'),
    'STOP-' || LPAD((floor(random()*200+1))::text, 4, '0'),
    'STOP-' || LPAD((floor(random()*200+1))::text, 4, '0'),
    md5(random()::text),
    ROUND((random()*8+2)::numeric, 2),
    CURRENT_DATE - (serie.n % 3),
    NOW() - (random()*interval '20 hours'),
    NOW() - (random()*interval '18 hours'),
    floor(random()*60+5)::smallint,
    'BUS-' || LPAD((floor(random()*850+1))::text, 4, '0')
FROM generate_series(1, 5000) AS serie(n);

COMMENT ON TABLE trips IS 'Registros de viagens — sistema legado de bilhetagem UrbanFlow';