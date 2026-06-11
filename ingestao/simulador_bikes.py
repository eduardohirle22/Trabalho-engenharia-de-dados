"""
simulador_bikes.py — UrbanFlow Ingestão
Simula status de estações de bicicletas compartilhadas (30 estações, ciclo de 30 min).
"""

import json
import os
import random
import argparse
from datetime import datetime, timezone
from pathlib import Path

ESTACOES_BIKE = [
    {"id": f"BIKE-{i:02d}", "lat": round(-15.78 + random.uniform(-0.1, 0.1), 6),
     "lon": round(-48.03 + random.uniform(-0.1, 0.1), 6), "capacidade": random.choice([10, 15, 20])}
    for i in range(1, 31)
]

def gerar_status_estacao(estacao: dict, ts: datetime) -> dict:
    cap  = estacao["capacidade"]
    disp = random.randint(0, cap)
    return {
        "station_id":       estacao["id"],
        "lat":              estacao["lat"],
        "lon":              estacao["lon"],
        "bikes_available":  disp,
        "docks_available":  cap - disp,
        "capacity":         cap,
        "is_renting":       disp > 0,
        "is_returning":     (cap - disp) > 0,
        "last_reported":    ts.isoformat(),
        "schema_version":   "1.0",
    }

def publicar_local(eventos: list, ts: datetime, base_dir: str = "bronze") -> str:
    pasta = (Path(base_dir) / "bikes"
             / f"ano={ts.year}" / f"mes={ts.month:02d}" / f"dia={ts.day:02d}")
    pasta.mkdir(parents=True, exist_ok=True)
    arquivo = pasta / f"bikes_{ts.strftime('%Y%m%d_%H%M%S')}.json"
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
    key = (f"bikes/ano={ts.year}/mes={ts.month:02d}"
           f"/dia={ts.day:02d}/bikes_{ts.strftime('%Y%m%d_%H%M%S')}.json")
    try:
        s3.put_object(Bucket=bucket, Key=key,
                      Body=json.dumps(eventos, ensure_ascii=False).encode("utf-8"),
                      ContentType="application/json")
        return f"s3://{bucket}/{key}"
    except ClientError as e:
        print(f"  ⚠️  Erro MinIO: {e}. Fallback local.")
        return publicar_local(eventos, ts)

def main():
    parser = argparse.ArgumentParser(description="Simulador Bikes UrbanFlow")
    parser.add_argument("--local",  action="store_true", default=False)
    parser.add_argument("--minio",  action="store_true", default=False)
    parser.add_argument("--ciclos", type=int, default=1)
    parser.add_argument("--dir",    type=str, default="bronze")
    args = parser.parse_args()
    if not args.local and not args.minio:
        args.local = True

    print(f"🚲 UrbanFlow — Simulador Bicicletas Compartilhadas")
    print(f"   Estações: {len(ESTACOES_BIKE)}  |  Ciclos: {args.ciclos}")

    for ciclo in range(args.ciclos):
        ts      = datetime.now(timezone.utc)
        eventos = [gerar_status_estacao(e, ts) for e in ESTACOES_BIKE]
        path    = publicar_minio(eventos, ts) if args.minio else publicar_local(eventos, ts, args.dir)
        print(f"  Ciclo {ciclo+1}/{args.ciclos}: {len(eventos)} estações → {path}")

    print(f"\n  ✅ Simulador de bikes concluído")

if __name__ == "__main__":
    main()
