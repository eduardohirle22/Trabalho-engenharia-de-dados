"""
urbanflow_healthcheck.py -- DAG de monitoramento continuo dos servicos UrbanFlow

Executa a cada 5 minutos e verifica:
  - MinIO (object storage Bronze/Silver/Gold)
  - PostgreSQL Airflow e PostgreSQL legado
  - API FastAPI de serving (endpoint /health)
  - Espaco em disco no volume airflow-data
  - Freshness dos dados no Silver (< 2 horas)

Se qualquer servico critico cair, alem da task ficar VERMELHA no Airflow UI,
um alerta e enviado pelos canais configurados (webhook/e-mail) via urbanflow_notify,
com fallback para log.critical. Assim a equipe e avisada mesmo sem olhar a UI.

Se uma task ficar vermelha: docker compose ps / docker compose logs <servico>
"""

from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import urbanflow_notify as notify

log      = logging.getLogger("urbanflow.healthcheck")
MINIO_EP = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
API_URL  = os.getenv("API_HEALTH_URL", "http://serving:8000/health")


def _callback_falha_health(context):
    """Notifica externamente quando uma checagem de saude falha."""
    ti = context["task_instance"]
    notify.enviar_alerta(
        titulo=f"Servico indisponivel — {ti.task_id}",
        corpo=(
            f"A checagem '{ti.task_id}' do healthcheck FALHOU.\n"
            f"Excecao: {context.get('exception', 'sem detalhes')}\n"
            f"Log URL: {ti.log_url}\n"
            f"Acao: docker compose ps / docker compose logs <servico>"
        ),
        nivel=notify.CRITICAL,
    )


DEFAULT_ARGS = {
    "owner":              "urbanflow",
    "retries":            0,
    "email_on_failure":   False,
    "on_failure_callback": _callback_falha_health,
}


def checar_minio(**context):
    """
    Verifica conectividade com MinIO e lista os buckets.
    Task fica VERMELHA no Airflow UI se MinIO estiver offline.
    """
    import boto3
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_EP,
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
            region_name="us-east-1",
        )
        buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
        log.info(f"MinIO ONLINE -- buckets: {buckets}")
        return {"status": "ONLINE", "buckets": buckets, "ts": datetime.now().isoformat()}
    except Exception as e:
        msg = f"MinIO OFFLINE: {e}"
        log.critical(msg)
        raise RuntimeError(msg)


def checar_postgres(**context):
    """Verifica conectividade com PostgreSQL (banco Airflow)."""
    import psycopg2
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            dbname="airflow",
            user=os.getenv("POSTGRES_USER", "airflow"),
            password=os.getenv("POSTGRES_PASSWORD", "airflow"),
            connect_timeout=5,
        )
        cur = conn.cursor()
        cur.execute("SELECT version()")
        version = cur.fetchone()[0].split(",")[0]
        conn.close()
        log.info(f"PostgreSQL ONLINE -- {version}")
        return {"status": "ONLINE", "version": version}
    except Exception as e:
        msg = f"PostgreSQL OFFLINE: {e}"
        log.critical(msg)
        raise RuntimeError(msg)


def checar_postgres_legado(**context):
    """Verifica conectividade com o banco urbanflow_legado."""
    import psycopg2
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_LEGADO_HOST", "postgres"),
            dbname=os.getenv("POSTGRES_LEGADO_DB", "urbanflow_legado"),
            user=os.getenv("POSTGRES_LEGADO_USER", "airflow"),
            password=os.getenv("POSTGRES_LEGADO_PASSWORD", "airflow"),
            connect_timeout=5,
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'")
        n_tabelas = cur.fetchone()[0]
        conn.close()
        log.info(f"PostgreSQL Legado ONLINE -- {n_tabelas} tabelas")
        return {"status": "ONLINE", "tabelas": n_tabelas}
    except Exception as e:
        msg = f"PostgreSQL Legado OFFLINE: {e}"
        log.critical(msg)
        raise RuntimeError(msg)


def checar_api(**context):
    """
    Verifica se a API FastAPI de serving esta respondendo no endpoint /health.
    Task fica VERMELHA (e dispara alerta) se a API estiver fora do ar.
    """
    import urllib.request
    try:
        with urllib.request.urlopen(API_URL, timeout=10) as resp:
            if 200 <= resp.status < 300:
                log.info(f"API FastAPI ONLINE -- {API_URL} ({resp.status})")
                return {"status": "ONLINE", "endpoint": API_URL, "http": resp.status}
            raise RuntimeError(f"API respondeu HTTP {resp.status}")
    except Exception as e:
        msg = f"API FastAPI OFFLINE ({API_URL}): {e}"
        log.critical(msg)
        raise RuntimeError(msg)


def checar_disco(**context):
    """
    Monitora uso do disco no volume airflow-data.
    WARNING se > 80%, CRITICAL se > 95%. Nao falha o pipeline.
    """
    import shutil
    path = "/opt/airflow/data"
    try:
        total, used, free = shutil.disk_usage(path)
        pct    = (used / total) * 100
        gb_liv = free / (1024 ** 3)
        gb_tot = total / (1024 ** 3)
        resultado = {"pct_usado": round(pct, 2), "gb_livre": round(gb_liv, 2), "gb_total": round(gb_tot, 2)}
        if pct > 95:
            log.critical(f"DISCO CRITICO: {pct:.1f}% -- apenas {gb_liv:.1f}GB livres!")
        elif pct > 80:
            log.warning(f"DISCO ALTO: {pct:.1f}% -- {gb_liv:.1f}GB livres de {gb_tot:.1f}GB")
        else:
            log.info(f"Disco OK -- {pct:.1f}% usado, {gb_liv:.1f}GB livres")
        return resultado
    except Exception as e:
        log.error(f"Nao foi possivel verificar disco: {e}")
        return {"erro": str(e)}


def checar_silver_freshness(**context):
    """
    Verifica se os dados Silver foram atualizados nas ultimas 2 horas.
    Detecta pipeline parado mesmo que servicos estejam online.
    """
    import boto3
    from datetime import timezone
    try:
        s3    = boto3.client(
            "s3", endpoint_url=MINIO_EP,
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
            region_name="us-east-1",
        )
        agora     = datetime.now(timezone.utc)
        resultado = {}
        for prefixo in ["gps_onibus_clean/", "catracas_clean/", "bikes_clean/"]:
            resp    = s3.list_objects_v2(Bucket="urbanflow-silver", Prefix=prefixo)
            objetos = resp.get("Contents", [])
            if not objetos:
                log.warning(f"Silver vazio: {prefixo}")
                resultado[prefixo] = "VAZIO"
                continue
            mais_recente = max(o["LastModified"] for o in objetos)
            horas        = (agora - mais_recente).total_seconds() / 3600
            status       = "OK" if horas < 2 else "DESATUALIZADO"
            if status == "DESATUALIZADO":
                log.warning(f"Silver {prefixo}: ultimo arquivo ha {horas:.1f}h (> 2h)")
            else:
                log.info(f"Silver {prefixo}: OK -- ha {horas:.1f}h")
            resultado[prefixo] = {"status": status, "horas_atras": round(horas, 2)}
        return resultado
    except Exception as e:
        log.warning(f"Nao foi possivel verificar Silver freshness: {e}")
        return {"erro": str(e)}


# --- DAG ---
with DAG(
    dag_id="urbanflow_healthcheck",
    description="Monitoramento continuo dos servicos UrbanFlow (a cada 5 minutos)",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=["urbanflow", "monitoramento", "healthcheck"],
) as dag:

    t_minio   = PythonOperator(task_id="checar_minio",           python_callable=checar_minio)
    t_pg      = PythonOperator(task_id="checar_postgres",        python_callable=checar_postgres)
    t_pg_leg  = PythonOperator(task_id="checar_postgres_legado", python_callable=checar_postgres_legado)
    t_api     = PythonOperator(task_id="checar_api",             python_callable=checar_api)
    t_disco   = PythonOperator(task_id="checar_disco",           python_callable=checar_disco)
    t_fresh   = PythonOperator(task_id="checar_silver_freshness",python_callable=checar_silver_freshness)

    # Todas as tasks rodam em paralelo -- independentes
    [t_minio, t_pg, t_pg_leg, t_api, t_disco, t_fresh]
