"""
main.py — UrbanFlow Serving API
FastAPI + DuckDB lendo Parquet do Silver/Gold

Endpoints:
  GET /health                    → status da API
  GET /kpis/operacional          → KPIs diários por linha
  GET /kpis/demanda              → demanda por estação e hora
  GET /frota/status              → status atual da frota (GPS Silver)
  GET /estacoes/ocupacao         → ocupação atual das estações do VLT
  GET /resumo/dashboard          → payload agregado para o dashboard
"""

import os
import logging
from pathlib import Path
from datetime import date, datetime
from typing import Optional, List

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("urbanflow-api")

# ─── Config ───────────────────────────────────────────────────────────────────
SILVER_DIR = Path(os.getenv("SILVER_DIR", "/data/silver"))
GOLD_DIR   = Path(os.getenv("GOLD_DIR",   "/data/gold"))
GOLD_DB    = GOLD_DIR / "urbanflow.duckdb"

# ─── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="UrbanFlow Data API",
    description="API de serving da plataforma de dados UrbanFlow Mobilidade S.A.",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ─── Helpers ─────────────────────────────────────────────────────────────────
def _conn() -> duckdb.DuckDBPyConnection:
    """Retorna conexão DuckDB. Usa banco Gold se existir, senão lê Parquet diretamente."""
    if GOLD_DB.exists():
        return duckdb.connect(str(GOLD_DB), read_only=True)
    return duckdb.connect()


def _parquet_exists(subdir: str) -> bool:
    return bool(list((SILVER_DIR / subdir).rglob("*.parquet"))) if (SILVER_DIR / subdir).exists() else False


def _query(sql: str) -> list:
    try:
        conn   = _conn()
        result = conn.execute(sql).fetchdf()
        conn.close()
        return result.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Query error: {e}\nSQL: {sql}")
        raise HTTPException(status_code=500, detail=f"Erro ao executar query: {str(e)}")

# ─── Modelos de resposta ──────────────────────────────────────────────────────
class HealthResponse(BaseModel):
    status:       str
    timestamp:    str
    gold_db:      bool
    silver_gps:   bool
    silver_catracas: bool
    versao_api:   str

# ─── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["Infra"])
def health():
    """Verifica saúde da API e disponibilidade dos dados."""
    return {
        "status":          "ok",
        "timestamp":       datetime.utcnow().isoformat() + "Z",
        "gold_db":         GOLD_DB.exists(),
        "silver_gps":      _parquet_exists("gps_onibus_clean"),
        "silver_catracas": _parquet_exists("catracas_clean"),
        "versao_api":      "2.0.0",
    }


@app.get("/kpis/operacional", tags=["KPIs"])
def kpis_operacional(
    data_inicio: Optional[str] = Query(None, description="YYYY-MM-DD"),
    data_fim:    Optional[str] = Query(None, description="YYYY-MM-DD"),
    line_id:     Optional[str] = Query(None, description="Ex: L001"),
    limit:       int           = Query(100, ge=1, le=1000),
):
    """
    KPIs operacionais diários por linha de ônibus.
    Retorna OTP, velocidade média, ocupação e semáforo de performance.
    """
    silver_path = str(SILVER_DIR / "gps_onibus_clean/**/*.parquet")

    where_clauses = []
    if data_inicio:
        where_clauses.append(f"data_evento >= '{data_inicio}'::DATE")
    if data_fim:
        where_clauses.append(f"data_evento <= '{data_fim}'::DATE")
    if line_id:
        where_clauses.append(f"line_id = '{line_id}'")

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    sql = f"""
        WITH base AS (
            SELECT
                CAST(timestamp AS DATE)    AS data_evento,
                line_id,
                direction,
                CASE
                    WHEN EXTRACT(hour FROM timestamp::TIMESTAMPTZ) BETWEEN 6  AND 9  THEN 'manha_pico'
                    WHEN EXTRACT(hour FROM timestamp::TIMESTAMPTZ) BETWEEN 17 AND 20 THEN 'tarde_pico'
                    WHEN EXTRACT(hour FROM timestamp::TIMESTAMPTZ) BETWEEN 10 AND 16 THEN 'entrepico'
                    ELSE 'noturno'
                END AS periodo_dia,
                speed_kmh::DOUBLE    AS speed_kmh,
                occupancy_pct::INT   AS occupancy_pct,
                vehicle_id,
                CASE WHEN speed_kmh::DOUBLE > 120 THEN TRUE ELSE FALSE END AS is_outlier,
                CASE WHEN status = 'on_route' THEN 1 ELSE 0 END AS em_rota
            FROM read_parquet('{silver_path}')
        ),
        agg AS (
            SELECT
                data_evento,
                line_id,
                direction,
                periodo_dia,
                COUNT(*)                              AS total_eventos,
                ROUND(AVG(speed_kmh), 2)              AS velocidade_media_kmh,
                ROUND(AVG(occupancy_pct::DOUBLE), 1)  AS ocupacao_media_pct,
                COUNT(DISTINCT vehicle_id)            AS veiculos_ativos,
                SUM(is_outlier::INT)                  AS outliers,
                ROUND(100.0 * SUM(em_rota) / NULLIF(COUNT(*) - SUM(is_outlier::INT), 0), 1) AS otp_pct
            FROM base
            {where_sql}
            GROUP BY data_evento, line_id, direction, periodo_dia
        )
        SELECT *,
            CASE
                WHEN otp_pct >= 90 THEN 'VERDE'
                WHEN otp_pct >= 70 THEN 'AMARELO'
                ELSE 'VERMELHO'
            END AS semaforo_otp
        FROM agg
        ORDER BY data_evento DESC, line_id
        LIMIT {limit}
    """
    return _query(sql)


@app.get("/kpis/demanda", tags=["KPIs"])
def kpis_demanda(
    data_inicio: Optional[str] = Query(None),
    data_fim:    Optional[str] = Query(None),
    station_id:  Optional[str] = Query(None),
    limit:       int           = Query(200, ge=1, le=2000),
):
    """Demanda de passageiros por estação × hora × período do dia."""
    silver_path = str(SILVER_DIR / "catracas_clean/**/*.parquet")

    where_parts = []
    if data_inicio:
        where_parts.append(f"data_evento >= '{data_inicio}'::DATE")
    if data_fim:
        where_parts.append(f"data_evento <= '{data_fim}'::DATE")
    if station_id:
        where_parts.append(f"station_id = '{station_id}'")

    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    sql = f"""
        WITH fonte AS (
            SELECT
                station_id::VARCHAR                              AS station_id,
                CAST(timestamp AS DATE)                         AS data_evento,
                EXTRACT(hour FROM timestamp::TIMESTAMPTZ)::INT  AS hora_do_dia,
                CASE
                    WHEN EXTRACT(hour FROM timestamp::TIMESTAMPTZ) BETWEEN 6  AND 9  THEN 'manha_pico'
                    WHEN EXTRACT(hour FROM timestamp::TIMESTAMPTZ) BETWEEN 17 AND 20 THEN 'tarde_pico'
                    WHEN EXTRACT(hour FROM timestamp::TIMESTAMPTZ) BETWEEN 10 AND 16 THEN 'entrepico'
                    ELSE 'noturno'
                END AS periodo_dia,
                direction::VARCHAR   AS direction,
                fare_paid::DOUBLE    AS fare_paid
            FROM read_parquet('{silver_path}')
        )
        SELECT
            station_id,
            data_evento,
            hora_do_dia,
            periodo_dia,
            SUM(CASE WHEN direction = 'ENTRY' THEN 1 ELSE 0 END) AS entradas,
            SUM(CASE WHEN direction = 'EXIT'  THEN 1 ELSE 0 END) AS saidas,
            ROUND(SUM(fare_paid), 2)                              AS receita_brl,
            RANK() OVER (PARTITION BY data_evento ORDER BY SUM(CASE WHEN direction='ENTRY' THEN 1 ELSE 0 END) DESC) AS rank_dia
        FROM fonte
        {where_sql}
        GROUP BY station_id, data_evento, hora_do_dia, periodo_dia
        ORDER BY data_evento DESC, entradas DESC
        LIMIT {limit}
    """
    return _query(sql)


@app.get("/frota/status", tags=["Frota"])
def frota_status(limit: int = Query(50, ge=1, le=500)):
    """Status mais recente da frota (último evento GPS por veículo)."""
    silver_path = str(SILVER_DIR / "gps_onibus_clean/**/*.parquet")

    sql = f"""
        WITH ranked AS (
            SELECT
                vehicle_id::VARCHAR   AS vehicle_id,
                line_id::VARCHAR      AS line_id,
                status::VARCHAR       AS status,
                speed_kmh::DOUBLE     AS speed_kmh,
                occupancy_pct::INT    AS occupancy_pct,
                lat::DOUBLE           AS lat,
                lon::DOUBLE           AS lon,
                timestamp::TIMESTAMPTZ AS ultimo_evento,
                ROW_NUMBER() OVER (
                    PARTITION BY vehicle_id
                    ORDER BY timestamp DESC
                ) AS rn
            FROM read_parquet('{silver_path}')
        )
        SELECT
            vehicle_id, line_id, status,
            ROUND(speed_kmh, 1)   AS speed_kmh,
            occupancy_pct,
            ROUND(lat, 6)         AS lat,
            ROUND(lon, 6)         AS lon,
            ultimo_evento
        FROM ranked
        WHERE rn = 1
        ORDER BY ultimo_evento DESC
        LIMIT {limit}
    """
    return _query(sql)


@app.get("/estacoes/ocupacao", tags=["Estações"])
def estacoes_ocupacao(data: Optional[str] = Query(None)):
    """Ocupação das estações do VLT para uma data específica (default: hoje)."""
    silver_path = str(SILVER_DIR / "catracas_clean/**/*.parquet")
    data_ref    = data or date.today().isoformat()

    sql = f"""
        SELECT
            station_id::VARCHAR                              AS station_id,
            COUNT(*)                                        AS total_eventos,
            SUM(CASE WHEN direction='ENTRY' THEN 1 ELSE 0 END) AS entradas,
            SUM(CASE WHEN direction='EXIT'  THEN 1 ELSE 0 END) AS saidas,
            ROUND(SUM(fare_paid::DOUBLE), 2)                AS receita_brl,
            MIN(timestamp::TIMESTAMPTZ)                     AS primeiro_evento,
            MAX(timestamp::TIMESTAMPTZ)                     AS ultimo_evento
        FROM read_parquet('{silver_path}')
        WHERE CAST(timestamp AS DATE) = '{data_ref}'::DATE
        GROUP BY station_id
        ORDER BY entradas DESC
    """
    return _query(sql)


@app.get("/resumo/dashboard", tags=["Dashboard"])
def resumo_dashboard():
    """
    Payload agregado para o dashboard — resumo executivo do dia.
    Retorna métricas consolidadas de frota, passageiros e receita.
    """
    silver_gps  = str(SILVER_DIR / "gps_onibus_clean/**/*.parquet")
    silver_cat  = str(SILVER_DIR / "catracas_clean/**/*.parquet")

    hoje = date.today().isoformat()

    # Frota hoje
    sql_frota = f"""
        SELECT
            COUNT(DISTINCT vehicle_id)  AS veiculos_rastreados,
            COUNT(DISTINCT line_id)     AS linhas_ativas,
            ROUND(AVG(speed_kmh::DOUBLE), 1) AS velocidade_media_kmh,
            ROUND(AVG(occupancy_pct::INT), 1) AS ocupacao_media_pct,
            SUM(CASE WHEN status='delayed' THEN 1 ELSE 0 END) AS eventos_atrasados,
            COUNT(*) AS total_eventos_gps
        FROM read_parquet('{silver_gps}')
        WHERE CAST(timestamp AS DATE) = '{hoje}'::DATE
    """

    # Passageiros hoje
    sql_pass = f"""
        SELECT
            COUNT(DISTINCT station_id) AS estacoes_ativas,
            SUM(CASE WHEN direction='ENTRY' THEN 1 ELSE 0 END) AS total_entradas,
            ROUND(SUM(fare_paid::DOUBLE), 2) AS receita_total_brl
        FROM read_parquet('{silver_cat}')
        WHERE CAST(timestamp AS DATE) = '{hoje}'::DATE
    """

    frota      = _query(sql_frota)
    passageiros = _query(sql_pass)

    return {
        "data_referencia":   hoje,
        "atualizado_em":     datetime.utcnow().isoformat() + "Z",
        "frota":             frota[0] if frota else {},
        "passageiros":       passageiros[0] if passageiros else {},
        "status_pipeline":   "ok" if _parquet_exists("gps_onibus_clean") else "sem_dados",
    }
