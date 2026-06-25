"""
urbanflow_notify.py — Modulo compartilhado de notificacao de alertas.

Centraliza o envio de alertas para canais externos, de forma que tanto a DAG
principal (urbanflow_pipeline) quanto a DAG de monitoramento (urbanflow_healthcheck)
usem exatamente o mesmo caminho de notificacao.

Canais suportados (ativados por variavel de ambiente, opt-in):
  - Webhook (Slack / Discord / Microsoft Teams / Mattermost) via ALERT_WEBHOOK_URL
  - E-mail via SMTP                                          via SMTP_HOST etc.

Comportamento de fallback:
  Se NENHUM canal estiver configurado, o alerta e gravado em log.critical.
  O pipeline NUNCA quebra por causa de uma falha de notificacao — qualquer
  excecao ao notificar e capturada e apenas logada. Notificar e best-effort.

Variaveis de ambiente (todas opcionais — ver .env.example):
  ALERT_WEBHOOK_URL   URL completa do webhook (Slack/Discord/Teams)
  ALERT_WEBHOOK_KIND  "slack" (default) | "discord" | "teams"
  SMTP_HOST           host do servidor SMTP (ex: smtp.gmail.com)
  SMTP_PORT           porta SMTP (default 587, STARTTLS)
  SMTP_USER           usuario/login SMTP
  SMTP_PASSWORD       senha/app-password SMTP
  ALERT_EMAIL_FROM    remetente (default = SMTP_USER)
  ALERT_EMAIL_TO      destinatarios separados por virgula
"""

from __future__ import annotations

import os
import json
import logging
import smtplib
import urllib.request
from email.mime.text import MIMEText
from datetime import datetime, timezone

log = logging.getLogger("urbanflow.notify")

# Niveis padronizados de severidade
INFO     = "INFO"
WARNING  = "WARNING"
CRITICAL = "CRITICAL"

_EMOJI = {INFO: "🟢", WARNING: "🟡", CRITICAL: "🔴"}


def _enviar_webhook(titulo: str, corpo: str, nivel: str) -> bool:
    """Envia o alerta para um webhook (Slack/Discord/Teams). Retorna True se enviado."""
    url = os.getenv("ALERT_WEBHOOK_URL", "").strip()
    if not url:
        return False

    kind  = os.getenv("ALERT_WEBHOOK_KIND", "slack").strip().lower()
    emoji = _EMOJI.get(nivel, "")
    texto = f"{emoji} *{titulo}*\n```\n{corpo}\n```"

    # Cada plataforma espera um campo diferente no JSON.
    if kind == "discord":
        payload = {"content": f"{emoji} **{titulo}**\n```\n{corpo}\n```"}
    elif kind == "teams":
        payload = {"title": titulo, "text": corpo}
    else:  # slack (default) / mattermost
        payload = {"text": texto}

    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            if 200 <= resp.status < 300:
                log.info(f"Alerta enviado via webhook ({kind}).")
                return True
            log.warning(f"Webhook respondeu status {resp.status}.")
            return False
    except Exception as e:
        log.warning(f"Falha ao enviar webhook: {e}")
        return False


def _enviar_email(titulo: str, corpo: str) -> bool:
    """Envia o alerta por e-mail via SMTP/STARTTLS. Retorna True se enviado."""
    host = os.getenv("SMTP_HOST", "").strip()
    to   = os.getenv("ALERT_EMAIL_TO", "").strip()
    if not host or not to:
        return False

    port      = int(os.getenv("SMTP_PORT", "587"))
    user      = os.getenv("SMTP_USER", "").strip()
    password  = os.getenv("SMTP_PASSWORD", "").strip()
    remetente = os.getenv("ALERT_EMAIL_FROM", user).strip()
    destinos  = [d.strip() for d in to.split(",") if d.strip()]

    msg = MIMEText(corpo, "plain", "utf-8")
    msg["Subject"] = f"[UrbanFlow] {titulo}"
    msg["From"]    = remetente
    msg["To"]      = ", ".join(destinos)

    try:
        with smtplib.SMTP(host, port, timeout=15) as servidor:
            servidor.starttls()
            if user and password:
                servidor.login(user, password)
            servidor.sendmail(remetente, destinos, msg.as_string())
        log.info(f"Alerta enviado por e-mail para {destinos}.")
        return True
    except Exception as e:
        log.warning(f"Falha ao enviar e-mail: {e}")
        return False


def enviar_alerta(titulo: str, corpo: str, nivel: str = CRITICAL) -> dict:
    """
    Ponto de entrada unico para notificacao.

    Tenta todos os canais configurados (webhook + e-mail). Se nenhum canal estiver
    configurado, faz fallback para log. NUNCA lanca excecao — notificar e best-effort
    e jamais deve derrubar o pipeline.

    Retorna um dict indicando quais canais receberam o alerta, util para testes/logs.
    """
    ts        = datetime.now(timezone.utc).isoformat()
    corpo_ts  = f"{corpo}\n\n(timestamp UTC: {ts})"
    resultado = {"webhook": False, "email": False, "log": False}

    try:
        resultado["webhook"] = _enviar_webhook(titulo, corpo_ts, nivel)
    except Exception as e:
        log.warning(f"Erro inesperado no canal webhook: {e}")

    try:
        resultado["email"] = _enviar_email(titulo, corpo_ts)
    except Exception as e:
        log.warning(f"Erro inesperado no canal e-mail: {e}")

    # Fallback: se nada saiu, garante registro auditavel em log.
    if not (resultado["webhook"] or resultado["email"]):
        nivel_log = log.critical if nivel == CRITICAL else log.warning
        nivel_log(
            f"\n{'='*60}\n  ALERTA {nivel} — {titulo}\n{'='*60}\n{corpo_ts}\n{'='*60}"
        )
        resultado["log"] = True

    return resultado
