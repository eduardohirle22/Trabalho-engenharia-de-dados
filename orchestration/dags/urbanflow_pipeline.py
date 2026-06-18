"""
urbanflow_pipeline.py — DAG principal UrbanFlow
Orquestra o ciclo completo: Ingestão → Bronze → Silver → Gold (dbt)

Schedule: a cada hora (simuladores) + diário (batch Postgres)

Estrutura:
  1. ingestao_simuladores  — GPS, catracas, bikes → Bronze (MinIO)
  2. batch_postgres        — viagens e bilhetagem do banco legado → Bronze
  3. bronze_to_silver      — ETL pandas com validação de qualidade
  4. dbt_run               — modelos Silver → Gold (DuckDB)
  5. dbt_test              — 26+ testes de qualidade automáticos
  6. notifica_sucesso      — log de conclusão + métricas
"""

from __future__ import annotations

import os
import json
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log

# ─── Configuração ─────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "urbanflow",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
    "email_on_retry":   False,
}

INGESTAO_DIR = "/opt/airflow/ingestao"
DBT_DIR      = "/opt/airflow/dbt_project"
SILVER_DIR   = os.getenv("SILVER_DIR", "/opt/airflow/data/silver")
GOLD_DIR     = os.getenv("GOLD_DIR", "/opt/airflow/data/gold")
MINIO_EP     = os.getenv("MINIO_ENDPOINT", "http://minio:9000")

# ─── Tasks Python ─────────────────────────────────────────────────────────────

def task_simular_gps(**context):
    """Gera dados GPS e publica no MinIO Bronze."""
    sys.path.insert(0, INGESTAO_DIR)
    from simulador_gps import gerar_evento_gps, publicar_minio, VEICULOS, LINHAS
    from datetime import timezone

    logger.info("🚌 Iniciando simulador GPS...")
    ts     = datetime.now(timezone.utc)
    n_veic = int(Variable.get("gps_veiculos_por_ciclo", default_var=300))

    eventos = [
        gerar_evento_gps(v, LINHAS[i % len(LINHAS)], ts)
        for i, v in enumerate(VEICULOS[:n_veic])
    ]

    try:
        path = publicar_minio(eventos, ts)
        logger.info(f"  ✅ {len(eventos)} eventos GPS → {path}")
        context["ti"].xcom_push(key="gps_count", value=len(eventos))
    except Exception as e:
        logger.warning(f"  ⚠️  MinIO indisponível, publicando local: {e}")
        from simulador_gps import publicar_local
        Path("/opt/airflow/data/bronze").mkdir(parents=True, exist_ok=True)
        path = publicar_local(eventos, ts, base_dir="/opt/airflow/data/bronze")
        context["ti"].xcom_push(key="gps_count", value=len(eventos))


def task_simular_catracas(**context):
    """Gera eventos de catracas e publica no MinIO Bronze."""
    sys.path.insert(0, INGESTAO_DIR)
    from simulador_catracas import gerar_evento_catraca, publicar_minio, publicar_local
    from datetime import timezone

    logger.info("🎫 Iniciando simulador catracas...")
    ts     = datetime.now(timezone.utc)
    n_evt  = int(Variable.get("catracas_eventos_por_ciclo", default_var=200))
    eventos = [gerar_evento_catraca(ts) for _ in range(n_evt)]

    try:
        path = publicar_minio(eventos, ts)
        logger.info(f"  ✅ {len(eventos)} eventos catracas → {path}")
    except Exception as e:
        logger.warning(f"  ⚠️  Fallback local: {e}")
        Path("/opt/airflow/data/bronze").mkdir(parents=True, exist_ok=True)
        publicar_local(eventos, ts, base_dir="/opt/airflow/data/bronze")

    context["ti"].xcom_push(key="catracas_count", value=len(eventos))


def task_simular_bikes(**context):
    """Gera status das estações de bikes e publica no MinIO Bronze."""
    sys.path.insert(0, INGESTAO_DIR)
    from simulador_bikes import ESTACOES_BIKE, gerar_status_estacao, publicar_minio, publicar_local
    from datetime import timezone

    logger.info("🚲 Iniciando simulador bikes...")
    ts      = datetime.now(timezone.utc)
    eventos = [gerar_status_estacao(e, ts) for e in ESTACOES_BIKE]

    try:
        path = publicar_minio(eventos, ts)
        logger.info(f"  ✅ {len(eventos)} estações bikes → {path}")
    except Exception as e:
        logger.warning(f"  ⚠️  Fallback local: {e}")
        Path("/opt/airflow/data/bronze").mkdir(parents=True, exist_ok=True)
        publicar_local(eventos, ts, base_dir="/opt/airflow/data/bronze")


def task_batch_postgres(**context):
    """
    Extrai viagens e bilhetagem do banco legado e publica no MinIO Bronze.
    Executa diariamente para o dia anterior.
    """
    import psycopg2
    import json
    import boto3
    from datetime import date, timedelta

    data_ref = (date.today() - timedelta(days=1)).isoformat()
    logger.info(f"📦 Extraindo batch Postgres para data_ref={data_ref}")

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_LEGADO_HOST", "postgres"),
        dbname=os.getenv("POSTGRES_LEGADO_DB", "urbanflow_legado"),
        user=os.getenv("POSTGRES_LEGADO_USER", "urbanflow"),
        password=os.getenv("POSTGRES_LEGADO_PASSWORD", "urbanflow123"),
    )

    # Viagens
    with conn.cursor() as cur:
        cur.execute("""
            SELECT viagem_id, vehicle_id, line_id, data_viagem::text,
                   hora_inicio::text, hora_fim::text, passageiros,
                   CAST(receita_brl AS FLOAT), CAST(km_percorridos AS FLOAT),
                   no_horario
            FROM viagens
            WHERE data_viagem = %s
        """, (data_ref,))
        cols = [d[0] for d in cur.description]
        viagens = [dict(zip(cols, row)) for row in cur.fetchall()]

    # Bilhetagem
    with conn.cursor() as cur:
        cur.execute("""
            SELECT b.id, b.data_ref::text, b.station_id, e.nome_estacao,
                   b.total_entradas, b.total_saidas, CAST(b.receita_brl AS FLOAT)
            FROM bilhetagem_diaria b
            JOIN estacoes e ON e.station_id = b.station_id
            WHERE b.data_ref = %s
        """, (data_ref,))
        cols = [d[0] for d in cur.description]
        bilhetagem = [dict(zip(cols, row)) for row in cur.fetchall()]

    conn.close()
    logger.info(f"  📊 Viagens: {len(viagens)} | Bilhetagem: {len(bilhetagem)}")

    # Publica no MinIO
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_EP,
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        region_name="us-east-1",
    )
    ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    ano, mes, dia = data_ref.split("-")

    for payload, prefix in [(viagens, "viagens_postgres"), (bilhetagem, "bilhetagem_postgres")]:
        if payload:
            key = f"{prefix}/ano={ano}/mes={mes}/dia={dia}/{prefix}_{ts_str}.json"
            s3.put_object(
                Bucket="urbanflow-bronze", Key=key,
                Body=json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8"),
                ContentType="application/json",
            )
            logger.info(f"  ✅ {len(payload)} registros → s3://urbanflow-bronze/{key}")

    context["ti"].xcom_push(key="viagens_extraidas", value=len(viagens))


def task_bronze_to_silver(**context):
    """Executa pipeline pandas Bronze → Silver com validação de qualidade."""
    sys.path.insert(0, INGESTAO_DIR)
    from bronze_to_silver import processar_gps, processar_catracas, processar_bikes
    from pathlib import Path

    logger.info("🔄 Pipeline Bronze → Silver iniciado")
    silver = Path(SILVER_DIR)
    silver.mkdir(parents=True, exist_ok=True)
    bronze = Path("/opt/airflow/data/bronze")

    metricas = {}
    for fn, nome in [
        (processar_gps, "gps"),
        (processar_catracas, "catracas"),
        (processar_bikes, "bikes"),
    ]:
        try:
            pipeline_log = fn(bronze_dir=bronze, silver_dir=silver, source="minio")
            metricas[nome] = {
                "lidas": pipeline_log.lidas,
                "escritas": pipeline_log.escritas,
                "taxa": pipeline_log.taxa_qualidade,
            }
            # Alerta se taxa de qualidade < 99%
            if pipeline_log.lidas > 0 and pipeline_log.taxa_qualidade < 99.0:
                logger.warning(
                    f"  ⚠️  Taxa de qualidade {nome}: {pipeline_log.taxa_qualidade:.1f}% < 99%"
                )
        except Exception as e:
            logger.error(f"  ❌ Erro ao processar {nome}: {e}")

    context["ti"].xcom_push(key="silver_metricas", value=json.dumps(metricas))
    logger.info(f"  ✅ Bronze → Silver concluído: {metricas}")


def task_notifica_sucesso(**context):
    """Consolida métricas do pipeline e loga resultado final."""
    ti = context["ti"]

    gps_count    = ti.xcom_pull(task_ids="simular_gps",    key="gps_count")      or 0
    catracas_cnt = ti.xcom_pull(task_ids="simular_catracas",key="catracas_count") or 0
    viagens_cnt  = ti.xcom_pull(task_ids="batch_postgres",  key="viagens_extraidas") or 0

    metricas_raw = ti.xcom_pull(task_ids="bronze_to_silver", key="silver_metricas")
    metricas     = json.loads(metricas_raw) if metricas_raw else {}

    logger.info("=" * 60)
    logger.info("  ✅ UrbanFlow Pipeline — Ciclo Concluído")
    logger.info(f"  GPS publicados     : {gps_count}")
    logger.info(f"  Catracas publicadas: {catracas_cnt}")
    logger.info(f"  Viagens Postgres   : {viagens_cnt}")
    logger.info(f"  Silver métricas    : {metricas}")
    logger.info("=" * 60)


# ─── DAG ──────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="urbanflow_pipeline",
    description="Pipeline completo UrbanFlow: Ingestão → Bronze → Silver → Gold",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",   # a cada hora
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=["urbanflow", "producao", "pipeline"],
    doc_md="""
## UrbanFlow — Pipeline Principal

Pipeline de engenharia de dados para a plataforma de mobilidade urbana.

### Fluxo
```
GPS/Catracas/Bikes → MinIO Bronze → pandas ETL → Silver → dbt → Gold → API/Serving
```

### Variáveis Airflow configuráveis
- `gps_veiculos_por_ciclo` (default: 300)
- `catracas_eventos_por_ciclo` (default: 200)
""",
) as dag:

    # ── Simuladores (em paralelo)
    sim_gps = PythonOperator(
        task_id="simular_gps",
        python_callable=task_simular_gps,
    )
    sim_catracas = PythonOperator(
        task_id="simular_catracas",
        python_callable=task_simular_catracas,
    )
    sim_bikes = PythonOperator(
        task_id="simular_bikes",
        python_callable=task_simular_bikes,
    )

    # ── Batch Postgres (diário)
    batch_pg = PythonOperator(
        task_id="batch_postgres",
        python_callable=task_batch_postgres,
    )

    # ── Bronze → Silver
    b2s = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=task_bronze_to_silver,
    )

    # ── dbt run
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"URBANFLOW_SILVER_DIR={SILVER_DIR} "
            f"URBANFLOW_GOLD_DIR={GOLD_DIR} "
            f"dbt run --profiles-dir {DBT_DIR} --project-dir {DBT_DIR} "
            f"--target prod 2>&1"
        ),
    )

    # ── dbt test
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"URBANFLOW_SILVER_DIR={SILVER_DIR} "
            f"URBANFLOW_GOLD_DIR={GOLD_DIR} "
            f"dbt test --profiles-dir {DBT_DIR} --project-dir {DBT_DIR} "
            f"--target prod 2>&1"
        ),
    )

    # ── Notificação de sucesso
    notifica = PythonOperator(
        task_id="notifica_sucesso",
        python_callable=task_notifica_sucesso,
        trigger_rule="all_done",
    )

    # ─── Dependências ─────────────────────────────────────────────────────────
    # Simuladores em paralelo → B2S → dbt run → dbt test → notifica
    [sim_gps, sim_catracas, sim_bikes, batch_pg] >> b2s >> dbt_run >> dbt_test >> notifica
