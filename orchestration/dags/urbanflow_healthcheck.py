"""
urbanflow_healthcheck.py -- DAG de monitoramento continuo dos servicos UrbanFlow

Executa a cada 5 minutos e verifica:
  - MinIO (object storage Bronze/Silver/Gold)
  - PostgreSQL Airflow e PostgreSQL legado
  - Espaco em disco no volume airflow-data
  - Freshness dos dados no Silver (< 2 horas)

Se uma task ficar vermelha: docker compose ps / docker compose logs <servico>
"""

from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

log      = logging.getLogger("urbanflow.healthcheck")
MINIO_EP = os.getenv("MINIO_ENDPOINT", "http://minio:9000")

DEFAULT_ARGS = {
    "owner":            "urbanflow",
    "retries":          0,
    "email_on_failure": False,
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
    t_disco   = PythonOperator(task_id="checar_disco",           python_callable=checar_disco)
    t_fresh   = PythonOperator(task_id="checar_silver_freshness",python_callable=checar_silver_freshness)

    # Todas as tasks rodam em paralelo -- independentes
    [t_minio, t_pg, t_pg_leg, t_disco, t_fresh]
