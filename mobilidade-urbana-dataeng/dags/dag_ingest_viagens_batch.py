"""
UrbanFlow — DAG de Ingestão Batch
Extrai dados históricos de viagens do PostgreSQL legado e salva no Bronze (MinIO).

Executa diariamente às 01h00.
"""

from datetime import datetime, timedelta
import json
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Configuração padrão da DAG
# ─────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "urbanflow-data-team",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-team@urbanflow.com"],
}

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BRONZE_BUCKET = "urbanflow-bronze"


def extract_viagens_from_postgres(**context):
    """
    Extrai registros de viagens do PostgreSQL legado
    para o dia anterior (T-1) e salva no MinIO Bronze.
    """
    import boto3
    import pandas as pd
    from io import StringIO

    execution_date = context["execution_date"]
    target_date = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")
    year, month, day = target_date.split("-")

    logger.info(f"Extraindo viagens do dia {target_date}...")

    # Conexão com PostgreSQL via Airflow Connection
    pg_hook = PostgresHook(postgres_conn_id="urbanflow_postgres_legacy")

    query = f"""
        SELECT
            trip_id,
            modal,
            origin_stop_id,
            destination_stop_id,
            card_id,
            fare_paid,
            trip_date,
            duration_minutes,
            created_at
        FROM trips
        WHERE trip_date = '{target_date}'
        ORDER BY created_at
    """

    df = pg_hook.get_pandas_df(query)
    logger.info(f"Extraídos {len(df)} registros de viagens.")

    if df.empty:
        logger.warning(f"Nenhum dado encontrado para {target_date}. Encerrando.")
        return 0

    # Serializa para CSV
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()

    # Salva no MinIO Bronze
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    s3_key = f"viagens/ano={year}/mes={month}/dia={day}/viagens_{target_date}.csv"

    s3_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=s3_key,
        Body=csv_content.encode("utf-8"),
        ContentType="text/csv",
        Metadata={
            "source": "postgres_legacy",
            "extraction_date": execution_date.isoformat(),
            "record_count": str(len(df)),
        }
    )

    logger.info(f"✅ Arquivo salvo em s3://{BRONZE_BUCKET}/{s3_key}")
    logger.info(f"   Registros: {len(df)}")

    # Puxa métricas para XCom (monitoramento)
    context["ti"].xcom_push(key="record_count", value=len(df))
    context["ti"].xcom_push(key="s3_key", value=s3_key)

    return len(df)


def validate_extraction(**context):
    """Valida se a extração foi bem-sucedida e alerta se poucos registros."""
    record_count = context["ti"].xcom_pull(
        task_ids="extract_viagens", key="record_count"
    )
    s3_key = context["ti"].xcom_pull(
        task_ids="extract_viagens", key="s3_key"
    )

    logger.info(f"Validando extração: {record_count} registros em {s3_key}")

    # Alerta se muito poucos registros (possível problema na fonte)
    MIN_EXPECTED_RECORDS = 50_000
    if record_count < MIN_EXPECTED_RECORDS:
        logger.warning(
            f"⚠️  Apenas {record_count} registros extraídos. "
            f"Esperado mínimo: {MIN_EXPECTED_RECORDS}. "
            f"Verificar fonte PostgreSQL!"
        )

    logger.info("✅ Validação concluída.")


# ─────────────────────────────────────────────
# Definição da DAG
# ─────────────────────────────────────────────
with DAG(
    dag_id="dag_ingest_viagens_batch",
    description="Extrai viagens do PostgreSQL legado → Bronze (MinIO)",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 1 * * *",  # Diariamente às 01h00
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestão", "batch", "viagens", "bronze"],
) as dag:

    task_extract = PythonOperator(
        task_id="extract_viagens",
        python_callable=extract_viagens_from_postgres,
        doc_md="Extrai viagens do PostgreSQL legado para o Bronze no MinIO.",
    )

    task_validate = PythonOperator(
        task_id="validate_extraction",
        python_callable=validate_extraction,
        doc_md="Valida contagem de registros extraídos e alerta anomalias.",
    )

    # Fluxo: extrai → valida
    task_extract >> task_validate
