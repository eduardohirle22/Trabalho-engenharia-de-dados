"""
simulador_catracas.py — UrbanFlow Prova de Conceito
Simula eventos de validação de bilhetes nas 18 estações do metrô.

Uso: python simulador_catracas.py --local --ciclos 3
"""

import json
import hashlib
import random
import argparse
from datetime import datetime, timezone
from pathlib import Path

ESTACOES   = [f"EST-{i:02d}" for i in range(1, 19)]    # 18 estações
CATRACAS_POR_ESTACAO = 4
CARD_TYPES = ["SINGLE", "MONTHLY", "STUDENT", "SENIOR"]
CARD_PESOS = [0.30, 0.45, 0.15, 0.10]
TARIFAS    = {"SINGLE": 5.50, "MONTHLY": 0.00, "STUDENT": 2.75, "SENIOR": 0.00}

def card_hash(card_id: str) -> str:
    """SHA-256 com salt fixo de ambiente — pseudoanonimização LGPD."""
    salt = "urbanflow-poc-salt-2026"
    return hashlib.sha256(f"{salt}{card_id}".encode()).hexdigest()

def gerar_evento_catraca(ts: datetime) -> dict:
    estacao  = random.choice(ESTACOES)
    gate_num = random.randint(1, CATRACAS_POR_ESTACAO)
    tipo     = random.choices(CARD_TYPES, CARD_PESOS)[0]
    card_id  = f"CARD-{random.randint(100000, 999999)}"
    return {
        "event_id":   f"EVT-{ts.strftime('%Y%m%d%H%M%S')}-{random.randint(1000,9999)}",
        "gate_id":    f"GATE-{estacao}-{gate_num:02d}",
        "station_id": estacao,
        "direction":  random.choice(["ENTRY", "EXIT"]),
        "card_type":  tipo,
        "card_hash":  card_hash(card_id),   # card_id NUNCA armazenado
        "fare_paid":  TARIFAS[tipo],
        "timestamp":  ts.isoformat(),
        "schema_version": "1.0",
    }

def publicar_local(eventos: list, ts: datetime, base_dir: str = "bronze") -> str:
    pasta = Path(base_dir) / "catracas" / f"ano={ts.year}" / f"mes={ts.month:02d}" / f"dia={ts.day:02d}"
    pasta.mkdir(parents=True, exist_ok=True)
    arquivo = pasta / f"catracas_{ts.strftime('%Y%m%d_%H%M%S')}.json"
    with open(arquivo, "w") as f:
        json.dump(eventos, f, ensure_ascii=False)
    return str(arquivo)

def main():
    parser = argparse.ArgumentParser(description="Simulador Catracas UrbanFlow")
    parser.add_argument("--local",  action="store_true", default=True)
    parser.add_argument("--ciclos", type=int, default=1)
    parser.add_argument("--dir",    type=str, default="bronze")
    parser.add_argument("--eventos-por-ciclo", type=int, default=200)
    args = parser.parse_args()

    print(f"🎫 UrbanFlow — Simulador Catracas Metrô")
    print(f"   Estações: {len(ESTACOES)}  |  Eventos/ciclo: {args.eventos_por_ciclo}")
    print()

    for ciclo in range(1, args.ciclos + 1):
        ts = datetime.now(timezone.utc)
        eventos = [gerar_evento_catraca(ts) for _ in range(args.eventos_por_ciclo)]

        caminho = publicar_local(eventos, ts, base_dir=args.dir)
        print(f"[Ciclo {ciclo}] ✅ {len(eventos)} eventos → {caminho}")
        print(f"  Exemplo: {json.dumps(eventos[0], indent=2, ensure_ascii=False)[:200]}...")

if __name__ == "__main__":
    main()