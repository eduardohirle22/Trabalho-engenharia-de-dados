"""
urbanflow_pipeline.py — DAG principal UrbanFlow
Orquestra o ciclo completo: Ingestão → Bronze → Silver → Gold (dbt)

Estrutura:
  1. verificar_servicos    — pré-voo: checa MinIO e PostgreSQL
  2. ingestao_simuladores  — GPS, catracas, bikes → Bronze (MinIO)
  3. batch_postgres        — viagens e bilhetagem do banco legado → Bronze
  4. bronze_to_silver      — ETL pandas com validação de qualidade
  5. verificar_qualidade   — valida parquets Silver antes do dbt
  6. dbt_run               — modelos Silver → Gold (DuckDB)
  7. dbt_test              — 26+ testes de qualidade automáticos
  8. notifica_sucesso      — log de conclusão + métricas consolidadas

Monitoramento:
  - on_failure_callback: notificação externa (webhook/e-mail) em qualquer falha de task
  - sla_miss_callback: notificação quando task ultrapassa 2h
  - verificar_servicos: bloqueia pipeline se MinIO ou Postgres offline
  - verificar_qualidade: valida volume, nulos, duplicatas e regras de negócio
    Em caso de reprovação de qualidade, dispara alerta WARNING via urbanflow_notify.

Canais de notificação (ver urbanflow_notify.py e .env.example):
  - ALERT_WEBHOOK_URL  → Slack / Discord / Teams
  - SMTP_*             → e-mail
  - Sem configuração   → fallback para log.critical (auditável)
"""

from __future__ import annotations

import os
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin

# Modulo compartilhado de notificacao (webhook/e-mail com fallback para log).
# Esta no mesmo diretorio de DAGs, portanto importavel diretamente pelo Airflow.
import urbanflow_notify as notify

logger = LoggingMixin().log
log    = logging.getLogger("urbanflow.pipeline")

# ─── Configuração ─────────────────────────────────────────────────────────────
INGESTAO_DIR = "/opt/airflow/ingestao"
DBT_DIR      = "/opt/airflow/dbt_project"
SILVER_DIR   = os.getenv("SILVER_DIR", "/opt/airflow/data/silver")
GOLD_DIR     = os.getenv("GOLD_DIR",   "/opt/airflow/data/gold")
MINIO_EP     = os.getenv("MINIO_ENDPOINT", "http://minio:9000")

# ─── Callbacks de monitoramento ───────────────────────────────────────────────

def _callback_falha(context):
    """
    Disparado automaticamente quando QUALQUER task falha.
    Monta um alerta CRITICAL com contexto completo e envia pelos canais
    configurados (webhook/e-mail), com fallback para log.critical.
    """
    ti        = context["task_instance"]
    dag_id    = context["dag"].dag_id
    task_id   = ti.task_id
    exec_date = context["execution_date"]
    exception = context.get("exception", "sem detalhes")
    log_url   = ti.log_url

    corpo = (
        f"DAG      : {dag_id}\n"
        f"Task     : {task_id}\n"
        f"Data/Hora: {exec_date}\n"
        f"Tentativa: {ti.try_number}\n"
        f"Excecao  : {exception}\n"
        f"Log URL  : {log_url}"
    )
    notify.enviar_alerta(
        titulo="Falha no pipeline UrbanFlow",
        corpo=corpo,
        nivel=notify.CRITICAL,
    )


def _callback_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Disparado quando uma task excede o SLA de 2 horas.
    Indica que o pipeline esta mais lento que o esperado.
    """
    tarefas = [sla.task_id for sla in slas]
    notify.enviar_alerta(
        titulo="SLA violado no pipeline UrbanFlow",
        corpo=f"DAG: {dag.dag_id}\nTasks que excederam 2h: {tarefas}",
        nivel=notify.WARNING,
    )


DEFAULT_ARGS = {
    "owner":                     "urbanflow",
    "depends_on_past":           False,
    "retries":                   2,
    "retry_delay":               timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "email_on_failure":          False,
    "email_on_retry":            False,
    "on_failure_callback":       _callback_falha,
    "sla":                       timedelta(hours=2),
}

# ─── Task: Pre-voo — verificacao de servicos ──────────────────────────────────

def task_verificar_servicos(**context):
    """
    Verifica conectividade de MinIO e PostgreSQL ANTES de iniciar o pipeline.
    Falha imediata e descritiva se qualquer servico estiver offline.
    Evita que tasks subsequentes falhem com mensagens obscuras.
    """
    import boto3
    import psycopg2

    erros  = []
    status = {}

    # MinIO
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_EP,
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
            region_name="us-east-1",
        )
        buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
        log.info(f"  MinIO: ONLINE — buckets: {buckets}")
        status["minio"] = {"ok": True, "buckets": buckets}
    except Exception as e:
        erros.append(f"MinIO indisponivel: {e}")
        status["minio"] = {"ok": False, "erro": str(e)}
        log.error(f"  MinIO OFFLINE: {e}")

    # PostgreSQL Legado
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_LEGADO_HOST", "postgres"),
            dbname=os.getenv("POSTGRES_LEGADO_DB", "urbanflow_legado"),
            user=os.getenv("POSTGRES_LEGADO_USER", "airflow"),
            password=os.getenv("POSTGRES_LEGADO_PASSWORD", "airflow"),
            connect_timeout=5,
        )
        conn.close()
        log.info("  PostgreSQL Legado: ONLINE")
        status["postgres"] = {"ok": True}
    except Exception as e:
        erros.append(f"PostgreSQL indisponivel: {e}")
        status["postgres"] = {"ok": False, "erro": str(e)}
        log.error(f"  PostgreSQL OFFLINE: {e}")

    context["ti"].xcom_push(key="servicos_status", value=json.dumps(status))

    if erros:
        raise RuntimeError(
            "Servicos indisponiveis — pipeline abortado:\n" + "\n".join(erros)
        )

    log.info("  Todos os servicos operacionais. Pipeline autorizado.")
    return status


# ─── Task: Verificacao de qualidade Silver ────────────────────────────────────

def task_verificar_qualidade(**context):
    """
    Valida os parquets Silver apos o bronze_to_silver:
    - Existencia de arquivos por dataset
    - Volume minimo de registros
    - Nulos em campos criticos
    - Duplicatas
    - Regras de negocio (coordenadas, direction, valores negativos)
    """
    import pandas as pd

    silver  = Path(SILVER_DIR)
    alertas = []
    resultados = {}

    CONFIG_QUALIDADE = {
        "gps_onibus_clean": {
            "min_registros":   50,
            "campos_criticos": ["vehicle_id", "lat", "lon", "speed_kmh", "timestamp"],
            "regras": {
                "lat_valida":   lambda df: df["lat"].between(-90, 90).all(),
                "lon_valida":   lambda df: df["lon"].between(-180, 180).all(),
                "speed_valida": lambda df: (df["speed_kmh"] >= 0).all(),
            },
        },
        "catracas_clean": {
            "min_registros":   20,
            "campos_criticos": ["event_id", "station_id", "direction", "timestamp"],
            "regras": {
                "direction_valida": lambda df: df["direction"].isin(["ENTRY", "EXIT"]).all(),
            },
        },
        "bikes_clean": {
            "min_registros":   5,
            "campos_criticos": ["station_id", "bikes_available", "docks_available"],
            "regras": {
                "bikes_nao_negativo": lambda df: (df["bikes_available"] >= 0).all(),
                "docks_nao_negativo": lambda df: (df["docks_available"] >= 0).all(),
            },
        },
    }

    for dataset, cfg in CONFIG_QUALIDADE.items():
        parquets = list(silver.glob(f"{dataset}/**/*.parquet"))
        if not parquets:
            alertas.append(f"ALERTA {dataset}: nenhum arquivo parquet encontrado")
            resultados[dataset] = {"status": "AUSENTE"}
            continue

        df    = pd.concat([pd.read_parquet(p) for p in parquets], ignore_index=True)
        total = len(df)

        checks = {
            "total_registros": total,
            "min_ok":    total >= cfg["min_registros"],
            "nulos":     {c: int(df[c].isnull().sum()) for c in cfg["campos_criticos"] if c in df.columns},
            "duplicatas": int(df.duplicated().sum()),
            "regras":    {},
        }

        if not checks["min_ok"]:
            alertas.append(f"ALERTA {dataset}: {total} registros < minimo {cfg['min_registros']}")

        for campo, n_nulos in checks["nulos"].items():
            if n_nulos > 0:
                alertas.append(f"ALERTA {dataset}.{campo}: {n_nulos} nulos")

        for nome_regra, fn_regra in cfg.get("regras", {}).items():
            try:
                ok = bool(fn_regra(df))
                checks["regras"][nome_regra] = ok
                if not ok:
                    alertas.append(f"ALERTA {dataset}: regra '{nome_regra}' REPROVADA")
            except Exception as e:
                checks["regras"][nome_regra] = f"ERRO: {e}"

        checks["status"] = "ALERTA" if any(dataset in a for a in alertas) else "OK"
        resultados[dataset] = checks
        log.info(f"  {dataset}: {total} registros | duplicatas: {checks['duplicatas']} | {checks['status']}")

    for a in alertas:
        log.warning(a)

    # Se houve qualquer alerta de qualidade, notifica externamente (WARNING).
    # Decisao de design: NAO interrompe o pipeline (nao perde dado operacional),
    # mas registra e notifica para acao humana. A barreira rigida sao os testes dbt.
    if alertas:
        notify.enviar_alerta(
            titulo=f"Qualidade de dados — {len(alertas)} alerta(s) no Silver",
            corpo="\n".join(alertas),
            nivel=notify.WARNING,
        )

    log.info(f"  Qualidade verificada — {len(alertas)} alerta(s)")
    context["ti"].xcom_push(key="qualidade_resultados", value=json.dumps(resultados, default=str))
    return resultados


# ─── Tasks de simulacao e ingestion ──────────────────────────────────────────

def task_simular_gps(**context):
    """Gera dados GPS e publica no MinIO Bronze."""
    sys.path.insert(0, INGESTAO_DIR)
    from simulador_gps import gerar_evento_gps, publicar_minio, VEICULOS, LINHAS
    from datetime import timezone

    logger.info("Iniciando simulador GPS...")
    ts     = datetime.now(timezone.utc)
    n_veic = int(Variable.get("gps_veiculos_por_ciclo", default_var=300))
    eventos = [
        gerar_evento_gps(v, LINHAS[i % len(LINHAS)], ts)
        for i, v in enumerate(VEICULOS[:n_veic])
    ]
    try:
        path = publicar_minio(eventos, ts)
        logger.info(f"  {len(eventos)} eventos GPS -> {path}")
    except Exception as e:
        logger.warning(f"  MinIO indisponivel, publicando local: {e}")
        from simulador_gps import publicar_local
        Path("/opt/airflow/data/bronze").mkdir(parents=True, exist_ok=True)
        publicar_local(eventos, ts, base_dir="/opt/airflow/data/bronze")
    context["ti"].xcom_push(key="gps_count", value=len(eventos))


def task_simular_catracas(**context):
    """Gera eventos de catracas e publica no MinIO Bronze."""
    sys.path.insert(0, INGESTAO_DIR)
    from simulador_catracas import gerar_evento_catraca, publicar_minio, publicar_local
    from datetime import timezone

    logger.info("Iniciando simulador catracas...")
    ts     = datetime.now(timezone.utc)
    n_evt  = int(Variable.get("catracas_eventos_por_ciclo", default_var=200))
    eventos = [gerar_evento_catraca(ts) for _ in range(n_evt)]
    try:
        path = publicar_minio(eventos, ts)
        logger.info(f"  {len(eventos)} eventos catracas -> {path}")
    except Exception as e:
        logger.warning(f"  Fallback local: {e}")
        Path("/opt/airflow/data/bronze").mkdir(parents=True, exist_ok=True)
        publicar_local(eventos, ts, base_dir="/opt/airflow/data/bronze")
    context["ti"].xcom_push(key="catracas_count", value=len(eventos))


def task_simular_bikes(**context):
    """Gera status das estacoes de bikes e publica no MinIO Bronze."""
    sys.path.insert(0, INGESTAO_DIR)
    from simulador_bikes import ESTACOES_BIKE, gerar_status_estacao, publicar_minio, publicar_local
    from datetime import timezone

    logger.info("Iniciando simulador bikes...")
    ts      = datetime.now(timezone.utc)
    eventos = [gerar_status_estacao(e, ts) for e in ESTACOES_BIKE]
    try:
        path = publicar_minio(eventos, ts)
        logger.info(f"  {len(eventos)} estacoes bikes -> {path}")
    except Exception as e:
        logger.warning(f"  Fallback local: {e}")
        Path("/opt/airflow/data/bronze").mkdir(parents=True, exist_ok=True)
        publicar_local(eventos, ts, base_dir="/opt/airflow/data/bronze")


def task_batch_postgres(**context):
    """Extrai viagens e bilhetagem do banco legado -> MinIO Bronze."""
    import psycopg2
    import boto3
    from datetime import date, timedelta

    data_ref = (date.today() - timedelta(days=1)).isoformat()
    logger.info(f"Extraindo batch Postgres para data_ref={data_ref}")

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_LEGADO_HOST", "postgres"),
        dbname=os.getenv("POSTGRES_LEGADO_DB", "urbanflow_legado"),
        user=os.getenv("POSTGRES_LEGADO_USER", "airflow"),
        password=os.getenv("POSTGRES_LEGADO_PASSWORD", "airflow"),
    )
    with conn.cursor() as cur:
        cur.execute("""
            SELECT viagem_id, vehicle_id, line_id, data_viagem::text,
                   hora_inicio::text, hora_fim::text, passageiros,
                   CAST(receita_brl AS FLOAT), CAST(km_percorridos AS FLOAT), no_horario
            FROM viagens WHERE data_viagem = %s
        """, (data_ref,))
        cols    = [d[0] for d in cur.description]
        viagens = [dict(zip(cols, row)) for row in cur.fetchall()]
    with conn.cursor() as cur:
        cur.execute("""
            SELECT b.id, b.data_ref::text, b.station_id, e.nome_estacao,
                   b.total_entradas, b.total_saidas, CAST(b.receita_brl AS FLOAT)
            FROM bilhetagem_diaria b
            JOIN estacoes e ON e.station_id = b.station_id
            WHERE b.data_ref = %s
        """, (data_ref,))
        cols       = [d[0] for d in cur.description]
        bilhetagem = [dict(zip(cols, row)) for row in cur.fetchall()]
    conn.close()
    logger.info(f"  Viagens: {len(viagens)} | Bilhetagem: {len(bilhetagem)}")

    s3 = boto3.client(
        "s3", endpoint_url=MINIO_EP,
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        region_name="us-east-1",
    )
    ts_str        = datetime.now().strftime("%Y%m%d_%H%M%S")
    ano, mes, dia = data_ref.split("-")
    for payload, prefix in [(viagens, "viagens_postgres"), (bilhetagem, "bilhetagem_postgres")]:
        if payload:
            key = f"{prefix}/ano={ano}/mes={mes}/dia={dia}/{prefix}_{ts_str}.json"
            s3.put_object(
                Bucket="urbanflow-bronze", Key=key,
                Body=json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8"),
                ContentType="application/json",
            )
            logger.info(f"  {len(payload)} registros -> s3://urbanflow-bronze/{key}")
    context["ti"].xcom_push(key="viagens_extraidas", value=len(viagens))


def task_bronze_to_silver(**context):
    """Executa pipeline pandas Bronze -> Silver com validacao de qualidade."""
    sys.path.insert(0, INGESTAO_DIR)
    from bronze_to_silver import processar_gps, processar_catracas, processar_bikes

    logger.info("Pipeline Bronze -> Silver iniciado")
    silver = Path(SILVER_DIR)
    silver.mkdir(parents=True, exist_ok=True)
    bronze = Path("/opt/airflow/data/bronze")

    metricas = {}
    for fn, nome in [(processar_gps, "gps"), (processar_catracas, "catracas"), (processar_bikes, "bikes")]:
        try:
            pipeline_log = fn(bronze_dir=bronze, silver_dir=silver, source="minio")
            metricas[nome] = {
                "lidas":   pipeline_log.lidas,
                "escritas": pipeline_log.escritas,
                "taxa":    pipeline_log.taxa_qualidade,
            }
            if pipeline_log.lidas > 0 and pipeline_log.taxa_qualidade < 99.0:
                logger.warning(f"  Taxa qualidade {nome}: {pipeline_log.taxa_qualidade:.1f}% < meta 99%")
        except Exception as e:
            logger.error(f"  Erro ao processar {nome}: {e}")
            raise
    context["ti"].xcom_push(key="silver_metricas", value=json.dumps(metricas))
    logger.info(f"  Bronze -> Silver concluido: {metricas}")


def task_notifica_sucesso(**context):
    """Consolida todas as metricas do ciclo e loga resumo final."""
    ti = context["ti"]
    gps_count    = ti.xcom_pull(task_ids="simular_gps",        key="gps_count")        or 0
    catracas_cnt = ti.xcom_pull(task_ids="simular_catracas",    key="catracas_count")   or 0
    viagens_cnt  = ti.xcom_pull(task_ids="batch_postgres",      key="viagens_extraidas") or 0
    servicos_raw = ti.xcom_pull(task_ids="verificar_servicos",  key="servicos_status")
    metricas_raw = ti.xcom_pull(task_ids="bronze_to_silver",    key="silver_metricas")
    qualidade_raw= ti.xcom_pull(task_ids="verificar_qualidade", key="qualidade_resultados")

    servicos  = json.loads(servicos_raw)  if servicos_raw  else {}
    metricas  = json.loads(metricas_raw)  if metricas_raw  else {}
    qualidade = json.loads(qualidade_raw) if qualidade_raw else {}

    log.info("=" * 60)
    log.info("  UrbanFlow Pipeline - Ciclo Concluido")
    log.info(f"  GPS publicados      : {gps_count}")
    log.info(f"  Catracas publicadas : {catracas_cnt}")
    log.info(f"  Viagens Postgres    : {viagens_cnt}")
    log.info(f"  Silver metricas     : {metricas}")
    log.info(f"  Qualidade Silver    : {qualidade}")
    log.info(f"  Servicos (inicio)   : {servicos}")
    log.info("=" * 60)


# ─── DAG ──────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="urbanflow_pipeline",
    description="Pipeline UrbanFlow: Ingestion -> Bronze -> Silver -> Gold com monitoramento",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=_callback_sla_miss,
    tags=["urbanflow", "producao", "pipeline"],
) as dag:

    check_servicos = PythonOperator(task_id="verificar_servicos", python_callable=task_verificar_servicos)
    sim_gps  = PythonOperator(task_id="simular_gps",      python_callable=task_simular_gps)
    sim_cat  = PythonOperator(task_id="simular_catracas",  python_callable=task_simular_catracas)
    sim_bik  = PythonOperator(task_id="simular_bikes",     python_callable=task_simular_bikes)
    batch_pg = PythonOperator(task_id="batch_postgres",    python_callable=task_batch_postgres)
    b2s      = PythonOperator(task_id="bronze_to_silver",  python_callable=task_bronze_to_silver)
    check_qual = PythonOperator(task_id="verificar_qualidade", python_callable=task_verificar_qualidade)
    dbt_run  = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"URBANFLOW_SILVER_DIR={SILVER_DIR} URBANFLOW_GOLD_DIR={GOLD_DIR} "
            f"dbt run --profiles-dir {DBT_DIR} --project-dir {DBT_DIR} --target prod 2>&1"
        ),
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"URBANFLOW_SILVER_DIR={SILVER_DIR} URBANFLOW_GOLD_DIR={GOLD_DIR} "
            f"dbt test --profiles-dir {DBT_DIR} --project-dir {DBT_DIR} --target prod 2>&1"
        ),
    )
    notifica = PythonOperator(
        task_id="notifica_sucesso",
        python_callable=task_notifica_sucesso,
        trigger_rule="all_done",
    )

    # Dependencias
    check_servicos >> [sim_gps, sim_cat, sim_bik, batch_pg]
    [sim_gps, sim_cat, sim_bik, batch_pg] >> b2s
    b2s >> check_qual >> dbt_run >> dbt_test >> notifica
