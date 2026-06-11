"""
simulador_gps.py — UrbanFlow Ingestão
Simula telemetria GPS de 850 ônibus e publica eventos JSON no MinIO ou localmente.

Uso local (POC sem Docker):
    python simulador_gps.py --local --ciclos 3

Uso com MinIO rodando:
    python simulador_gps.py --minio --ciclos 1

Variáveis de ambiente (MinIO):
    MINIO_ENDPOINT   http://localhost:9000
    MINIO_ACCESS_KEY minioadmin
    MINIO_SECRET_KEY minioadmin123
"""

import json
import os
import random
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ─── Configuração ──────────────────────────────────────────────────────────────
NUM_VEICULOS   = 850
NUM_LINHAS     = 120
INTERVALO_SEG  = 30

# Bounding box de Brasília
LAT_MIN, LAT_MAX = -15.95, -15.55
LON_MIN, LON_MAX = -48.30, -47.75

LINHAS   = [f"L{i:03d}" for i in range(1, NUM_LINHAS + 1)]
VEICULOS = [f"BUS-{i:04d}" for i in range(1, NUM_VEICULOS + 1)]
STATUS   = ["on_route", "at_stop", "delayed", "out_of_service"]
STATUS_PESOS = [0.75, 0.15, 0.08, 0.02]

# ─── Gerador de evento ────────────────────────────────────────────────────────
def gerar_evento_gps(vehicle_id: str, line_id: str, ts: datetime) -> dict:
    speed = round(random.gauss(35, 15), 1)
    speed = max(0.0, min(speed, 90.0))
    is_outlier = random.random() < 0.005   # 0.5% de outliers
    if is_outlier:
        speed = round(random.uniform(100, 140), 1)

    return {
        "vehicle_id":     vehicle_id,
        "line_id":        line_id,
        "direction":      random.choice(["IDA", "VOLTA"]),
        "lat":            round(random.uniform(LAT_MIN, LAT_MAX), 6),
        "lon":            round(random.uniform(LON_MIN, LON_MAX), 6),
        "speed_kmh":      speed,
        "occupancy_pct":  random.randint(0, 100),
        "engine_on":      True,
        "door_open":      random.random() < 0.10,
        "status":         random.choices(STATUS, STATUS_PESOS)[0],
        "timestamp":      ts.isoformat(),
        "schema_version": "1.1",
    }

# ─── Publicação local ─────────────────────────────────────────────────────────
def publicar_local(eventos: list, ts: datetime, base_dir: str = "bronze") -> str:
    pasta = (Path(base_dir) / "gps_onibus"
             / f"ano={ts.year}" / f"mes={ts.month:02d}"
             / f"dia={ts.day:02d}" / f"hora={ts.hour:02d}")
    pasta.mkdir(parents=True, exist_ok=True)
    arquivo = pasta / f"gps_{ts.strftime('%Y%m%d_%H%M%S')}.json"
    with open(arquivo, "w") as f:
        json.dump(eventos, f, ensure_ascii=False)
    return str(arquivo)

# ─── Publicação MinIO ────────────────────────────────────────────────────────
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
    key = (f"gps_onibus/ano={ts.year}/mes={ts.month:02d}"
           f"/dia={ts.day:02d}/hora={ts.hour:02d}"
           f"/gps_{ts.strftime('%Y%m%d_%H%M%S')}.json")
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(eventos, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
        return f"s3://{bucket}/{key}"
    except ClientError as e:
        print(f"  ⚠️  Erro MinIO: {e}. Fallback para local.")
        return publicar_local(eventos, ts)

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Simulador GPS UrbanFlow")
    parser.add_argument("--local",  action="store_true", default=False)
    parser.add_argument("--minio",  action="store_true", default=False)
    parser.add_argument("--ciclos", type=int, default=3)
    parser.add_argument("--veiculos", type=int, default=300)
    parser.add_argument("--dir",    type=str, default="bronze")
    args = parser.parse_args()

    if not args.local and not args.minio:
        args.local = True   # default local

    print(f"🚌 UrbanFlow — Simulador GPS Ônibus")
    print(f"   Veículos: {args.veiculos}  |  Ciclos: {args.ciclos}")
    print(f"   Destino: {'MinIO' if args.minio else 'local'}")

    total = 0
    for ciclo in range(args.ciclos):
        ts = datetime.now(timezone.utc)
        veiculos_selecionados = VEICULOS[:args.veiculos]
        eventos = [
            gerar_evento_gps(v, LINHAS[i % len(LINHAS)], ts)
            for i, v in enumerate(veiculos_selecionados)
        ]

        if args.minio:
            path = publicar_minio(eventos, ts)
        else:
            path = publicar_local(eventos, ts, base_dir=args.dir)

        total += len(eventos)
        print(f"  Ciclo {ciclo+1}/{args.ciclos}: {len(eventos)} eventos → {path}")

    print(f"\n  ✅ Total publicado: {total} eventos GPS")

if __name__ == "__main__":
    main()
