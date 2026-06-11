-- ─── UrbanFlow — Banco Legado PostgreSQL ────────────────────────────────────
-- Simula o sistema de bilhetagem legado da UrbanFlow Mobilidade S.A.
-- Executado automaticamente no primeiro start do container Postgres
-- ─────────────────────────────────────────────────────────────────────────────

\c urbanflow_legado

-- ─── Tabelas ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS linhas (
    line_id       VARCHAR(10)  PRIMARY KEY,
    nome_linha    VARCHAR(100) NOT NULL,
    modal         VARCHAR(20)  NOT NULL CHECK (modal IN ('onibus', 'vlt', 'bicicleta')),
    num_veiculos  INT          DEFAULT 0,
    ativa         BOOLEAN      DEFAULT TRUE,
    criado_em     TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS viagens (
    viagem_id       SERIAL       PRIMARY KEY,
    vehicle_id      VARCHAR(20)  NOT NULL,
    line_id         VARCHAR(10)  REFERENCES linhas(line_id),
    data_viagem     DATE         NOT NULL,
    hora_inicio     TIME         NOT NULL,
    hora_fim        TIME,
    passageiros     INT          NOT NULL DEFAULT 0,
    receita_brl     NUMERIC(10,2) DEFAULT 0.00,
    km_percorridos  NUMERIC(8,2),
    no_horario      BOOLEAN,
    criado_em       TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS estacoes (
    station_id   VARCHAR(10)  PRIMARY KEY,
    nome_estacao VARCHAR(100) NOT NULL,
    modal        VARCHAR(20)  NOT NULL,
    lat          NUMERIC(10,6),
    lon          NUMERIC(10,6),
    ativa        BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS bilhetagem_diaria (
    id              SERIAL       PRIMARY KEY,
    data_ref        DATE         NOT NULL,
    station_id      VARCHAR(10)  REFERENCES estacoes(station_id),
    total_entradas  INT          DEFAULT 0,
    total_saidas    INT          DEFAULT 0,
    receita_brl     NUMERIC(10,2) DEFAULT 0.00,
    criado_em       TIMESTAMP    DEFAULT NOW()
);

-- ─── Linhas ──────────────────────────────────────────────────────────────────
INSERT INTO linhas (line_id, nome_linha, modal, num_veiculos) VALUES
  ('L001', 'Centro — Aeroporto',       'onibus', 12),
  ('L002', 'Norte — Shopping',         'onibus', 8),
  ('L003', 'Sul — Universidade',       'onibus', 10),
  ('L004', 'Leste — Industrial',       'onibus', 7),
  ('L005', 'Oeste — Hospital',         'onibus', 9),
  ('L010', 'Circular Centro',          'onibus', 5),
  ('VLT-A', 'VLT Linha A — Norte/Sul', 'vlt', 6),
  ('VLT-B', 'VLT Linha B — Leste/Oeste', 'vlt', 4),
  ('BIKE-Z1', 'Zona 1 Bicicletas',     'bicicleta', 0),
  ('BIKE-Z2', 'Zona 2 Bicicletas',     'bicicleta', 0)
ON CONFLICT DO NOTHING;

-- ─── Estações ────────────────────────────────────────────────────────────────
INSERT INTO estacoes (station_id, nome_estacao, modal, lat, lon) VALUES
  ('EST-01', 'Central',       'vlt',  -16.678, -49.254),
  ('EST-02', 'Norte A',       'vlt',  -16.640, -49.270),
  ('EST-03', 'Norte B',       'vlt',  -16.620, -49.280),
  ('EST-04', 'Sul A',         'vlt',  -16.710, -49.240),
  ('EST-05', 'Sul B',         'vlt',  -16.730, -49.230),
  ('EST-06', 'Aeroporto',     'vlt',  -16.632, -49.221),
  ('EST-07', 'Shopping Norte','vlt',  -16.615, -49.260),
  ('EST-08', 'Universidade',  'vlt',  -16.720, -49.250),
  ('EST-09', 'Hospital',      'vlt',  -16.688, -49.290),
  ('EST-10', 'Centro 2',      'vlt',  -16.675, -49.248),
  ('EST-11', 'Industrial A',  'vlt',  -16.665, -49.210),
  ('EST-12', 'Industrial B',  'vlt',  -16.660, -49.200),
  ('EST-13', 'Parque',        'vlt',  -16.695, -49.265),
  ('EST-14', 'Rodoviária',    'vlt',  -16.683, -49.256),
  ('EST-15', 'Mercado',       'vlt',  -16.679, -49.249),
  ('EST-16', 'Palácio',       'vlt',  -16.672, -49.251),
  ('EST-17', 'Setor Sul',     'vlt',  -16.705, -49.244),
  ('EST-18', 'Setor Norte',   'vlt',  -16.651, -49.267)
ON CONFLICT DO NOTHING;

-- ─── Viagens sintéticas (últimos 30 dias) ────────────────────────────────────
-- Gera ~5.000 registros de viagens para os últimos 30 dias
DO $$
DECLARE
    v_line_ids  VARCHAR[] := ARRAY['L001','L002','L003','L004','L005','L010','VLT-A','VLT-B'];
    v_line_id   VARCHAR;
    v_day       DATE;
    v_vehicle   VARCHAR;
    v_hora_ini  TIME;
    v_duracao   INT;
    v_pass      INT;
    i           INT;
    j           INT;
BEGIN
    FOR j IN 0..29 LOOP
        v_day := CURRENT_DATE - j;
        FOREACH v_line_id IN ARRAY v_line_ids LOOP
            -- 18 viagens por linha por dia
            FOR i IN 1..18 LOOP
                v_vehicle  := 'BUS-' || LPAD((FLOOR(RANDOM()*850)+1)::TEXT, 4, '0');
                v_hora_ini := (TIME '05:30' + (i * INTERVAL '45 minutes'));
                v_duracao  := 40 + FLOOR(RANDOM()*30)::INT;
                v_pass     := FLOOR(RANDOM()*80+5)::INT;

                INSERT INTO viagens (vehicle_id, line_id, data_viagem, hora_inicio, hora_fim,
                                     passageiros, receita_brl, km_percorridos, no_horario)
                VALUES (
                    v_vehicle,
                    v_line_id,
                    v_day,
                    v_hora_ini,
                    v_hora_ini + (v_duracao || ' minutes')::INTERVAL,
                    v_pass,
                    ROUND((v_pass * 5.5 * (RANDOM()*0.4+0.8))::NUMERIC, 2),
                    ROUND((15 + RANDOM()*25)::NUMERIC, 2),
                    RANDOM() > 0.15   -- 85% no horário
                );
            END LOOP;
        END LOOP;
    END LOOP;
END$$;

-- ─── Bilhetagem diária por estação (últimos 30 dias) ─────────────────────────
DO $$
DECLARE
    v_station    VARCHAR;
    v_day        DATE;
    v_entradas   INT;
    station_ids  VARCHAR[] := ARRAY['EST-01','EST-02','EST-03','EST-04','EST-05',
                                    'EST-06','EST-07','EST-08','EST-09','EST-10',
                                    'EST-11','EST-12','EST-13','EST-14','EST-15',
                                    'EST-16','EST-17','EST-18'];
BEGIN
    FOR j IN 0..29 LOOP
        v_day := CURRENT_DATE - j;
        FOREACH v_station IN ARRAY station_ids LOOP
            -- Pico de manhã e tarde
            v_entradas := 200 + FLOOR(RANDOM()*400)::INT;
            INSERT INTO bilhetagem_diaria (data_ref, station_id, total_entradas, total_saidas, receita_brl)
            VALUES (
                v_day, v_station,
                v_entradas,
                v_entradas + FLOOR(RANDOM()*20-10)::INT,
                ROUND((v_entradas * 5.5 * 0.6)::NUMERIC, 2)  -- ~60% pagam tarifa cheia
            );
        END LOOP;
    END LOOP;
END$$;

-- ─── Índices ─────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_viagens_data    ON viagens(data_viagem);
CREATE INDEX IF NOT EXISTS idx_viagens_line    ON viagens(line_id);
CREATE INDEX IF NOT EXISTS idx_viagens_vehicle ON viagens(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_bilhet_data     ON bilhetagem_diaria(data_ref);
CREATE INDEX IF NOT EXISTS idx_bilhet_station  ON bilhetagem_diaria(station_id);

-- ─── View de monitoramento ────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_resumo_diario AS
SELECT
    data_viagem,
    COUNT(*)                      AS total_viagens,
    SUM(passageiros)              AS total_passageiros,
    ROUND(SUM(receita_brl), 2)    AS receita_total_brl,
    ROUND(AVG(km_percorridos), 2) AS km_medio,
    ROUND(100.0 * SUM(CASE WHEN no_horario THEN 1 ELSE 0 END) / COUNT(*), 1) AS otp_pct
FROM viagens
GROUP BY data_viagem
ORDER BY data_viagem DESC;

-- Confirma
SELECT COUNT(*) AS viagens_inseridas FROM viagens;
SELECT COUNT(*) AS bilhetagem_inserida FROM bilhetagem_diaria;
