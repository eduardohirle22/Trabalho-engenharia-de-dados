#!/usr/bin/env python3
"""
watchdog_airflow.py — Vigia EXTERNO do Airflow (roda FORA do Airflow).

Por que existe:
  As DAGs urbanflow_pipeline e urbanflow_healthcheck so rodam se o scheduler do
  Airflow estiver vivo. Se o proprio Airflow cair, nenhuma DAG executa e, portanto,
  nenhum alerta interno e disparado. Este script resolve esse ponto cego: ele roda
  por fora (cron do host ou container separado) e bate no endpoint de saude do
  Airflow. Se o Airflow nao responder, ELE envia o alerta.

Como rodar (exemplos):
  # Manual / teste
  python watchdog_airflow.py

  # Cron do host, a cada 2 minutos (crontab -e):
  */2 * * * * cd /caminho/projeto && /usr/bin/python3 orchestration/watchdog_airflow.py

Variaveis de ambiente:
  AIRFLOW_HEALTH_URL  default http://localhost:8080/health
  (canais de alerta: ver urbanflow_notify.py — ALERT_WEBHOOK_URL / SMTP_*)
"""

from __future__ import annotations

import os
import sys
import json
import urllib.request

# Reusa exatamente o mesmo modulo de notificacao das DAGs.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "dags"))
import urbanflow_notify as notify  # noqa: E402

AIRFLOW_HEALTH_URL = os.getenv("AIRFLOW_HEALTH_URL", "http://localhost:8080/health")


def verificar_airflow() -> bool:
    """
    Consulta o endpoint /health do Airflow.
    O endpoint retorna o estado de 'metadatabase' e 'scheduler'.
    Retorna True se ambos estiverem 'healthy'.
    """
    try:
        with urllib.request.urlopen(AIRFLOW_HEALTH_URL, timeout=10) as resp:
            dados = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        notify.enviar_alerta(
            titulo="Airflow INACESSIVEL",
            corpo=(
                f"Nao foi possivel contatar o Airflow em {AIRFLOW_HEALTH_URL}.\n"
                f"Erro: {e}\n"
                f"Provavel queda do webserver/scheduler. "
                f"Verifique: docker compose ps && docker compose logs airflow-scheduler"
            ),
            nivel=notify.CRITICAL,
        )
        return False

    meta      = dados.get("metadatabase", {}).get("status")
    scheduler = dados.get("scheduler", {}).get("status")

    if meta == "healthy" and scheduler == "healthy":
        print(f"Airflow OK — metadatabase={meta}, scheduler={scheduler}")
        return True

    notify.enviar_alerta(
        titulo="Airflow DEGRADADO",
        corpo=(
            f"Endpoint respondeu, mas componentes nao estao saudaveis:\n"
            f"  metadatabase: {meta}\n"
            f"  scheduler   : {scheduler}\n"
            f"Acao: docker compose logs airflow-scheduler"
        ),
        nivel=notify.CRITICAL,
    )
    return False


if __name__ == "__main__":
    ok = verificar_airflow()
    sys.exit(0 if ok else 1)
