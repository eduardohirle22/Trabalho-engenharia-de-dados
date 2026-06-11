"""
simulador_catracas.py — UrbanFlow Ingestão
Simula eventos de validação de bilhetes nas 18 estações do metrô leve (VLT).

Privacidade LGPD: card_id nunca armazenado — apenas SHA-256 com salt de ambiente.

Uso:
    python simulador_catracas.py --local --ciclos 2 --eventos-por-ciclo 200
    python simulador_catracas.py --minio --ciclos 1
"""

import json
import hashlib
import os
import random
import argparse
from datetime import datetime, timezone
from pathlib import Path

ESTACOES            = [f"EST-{i:02d}" for i in range(1, 19)]
CATRACAS_POR_ESTACAO = 4
CARD_TYPES          = ["SINGLE", "MONTHLY", "STUDENT", "SENIOR"]
CARD_PESOS          = [0.30, 0.45, 0.15, 0.10]
TARIFAS             = {"SINGLE": 5.50, "MONTHLY": 0.00, "STUDENT": 2.75, "SENIOR": 0.00}

def card_hash(card_id: str) -> str:
    salt = os.getenv("CARD_HASH_SALT", "urbanflow-prod-salt-2026")
    return hashlib.sha256(f"{salt}{card_id}".encode()).hexdigest()

def gerar_evento_catraca(ts: datetime) -> dict:
    estacao  = random.choice(ESTACOES)
    gate_num = random.randint(1, CATRACAS_POR_ESTACAO)
    tipo     = random.choices(CARD_TYPES, CARD_PESOS)[0]
    card_id  = f"CARD-{random.randint(100000, 999999)}"
    return {
        "event_id":        f"EVT-{ts.strftime('%Y%m%d%H%M%S')}-{random.randint(1000,9999)}",
        "gate_id":         f"GATE-{estacao}-{gate_num:02d}",
        "station_id":      estacao,
        "direction":       random.choice(["ENTRY", "EXIT"]),
        "card_type":       tipo,
        "card_hash":       card_hash(card_id),
        "fare_paid":       TARIFAS[tipo],
        "timestamp":       ts.isoformat(),
        "schema_version":  "1.1",
    }

def publicar_local(eventos: list, ts: datetime, base_dir: str = "bronze") -> str:
    pasta = (Path(base_dir) / "catracas"
             / f"ano={ts.year}" / f"mes={ts.month:02d}" / f"dia={ts.day:02d}")
    pasta.mkdir(parents=True, exist_ok=True)
    arquivo = pasta / f"catracas_{ts.strftime('%Y%m%d_%H%M%S')}.json"
    with open(arquivo, "w") as f:
        json.dump(eventos, f, ensure_ascii=False)
    return str(arquivo)

def publicar_minio(eventos: list, ts: datetime) -> str:
    import boto3
    from botocore.exceptions import ClientError

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        region_name="us-east-1",
    )
    bucket = "urbanflow-bronze"
    key = (f"catracas/ano={ts.year}/mes={ts.month:02d}"
           f"/dia={ts.day:02d}/catracas_{ts.strftime('%Y%m%d_%H%M%S')}.json")
    try:
        s3.put_object(
            Bucket=bucket, Key=key,
            Body=json.dumps(eventos, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
        return f"s3://{bucket}/{key}"
    except ClientError as e:
        print(f"  ⚠️  Erro MinIO: {e}. Fallback para local.")
        return publicar_local(eventos, ts)

def main():
    parser = argparse.ArgumentParser(description="Simulador Catracas UrbanFlow")
    parser.add_argument("--local",  action="store_true", default=False)
    parser.add_argument("--minio",  action="store_true", default=False)
    parser.add_argument("--ciclos", type=int, default=2)
    parser.add_argument("--eventos-por-ciclo", type=int, default=150)
    parser.add_argument("--dir",    type=str, default="bronze")
    args = parser.parse_args()

    if not args.local and not args.minio:
        args.local = True

    print(f"🎫 UrbanFlow — Simulador Catracas Metrô")
    print(f"   Estações: {len(ESTACOES)}  |  Eventos/ciclo: {args.eventos_por_ciclo}")

    total = 0
    for ciclo in range(args.ciclos):
        ts     = datetime.now(timezone.utc)
        eventos = [gerar_evento_catraca(ts) for _ in range(args.eventos_por_ciclo)]
        path    = publicar_minio(eventos, ts) if args.minio else publicar_local(eventos, ts, args.dir)
        total  += len(eventos)
        print(f"  Ciclo {ciclo+1}/{args.ciclos}: {len(eventos)} eventos → {path}")

    print(f"\n  ✅ Total: {total} eventos de catraca")

if __name__ == "__main__":
    main()
