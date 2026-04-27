"""
simulador_gps.py — UrbanFlow Prova de Conceito
Simula telemetria GPS de 850 ônibus e publica eventos JSON no storage.

Uso local (POC sem Docker):
    python simulador_gps.py --local --ciclos 3

Uso com MinIO rodando:
    python simulador_gps.py --minio

Saída local: bronze/gps_onibus/ano=YYYY/mes=MM/dia=DD/hora=HH/
"""

import json
import os
import random
import hashlib
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ─── Configuração ──────────────────────────────────────────────────────────────
NUM_VEICULOS   = 850
NUM_LINHAS     = 120
INTERVALO_SEG  = 30  # segundos entre ciclos em produção

# Bounding box aproximada de uma cidade média brasileira (Goiânia como referência)
LAT_MIN, LAT_MAX = -16.80, -16.55
LON_MIN, LON_MAX = -49.40, -49.15

LINHAS   = [f"L{i:03d}" for i in range(1, NUM_LINHAS + 1)]
VEICULOS = [f"BUS-{i:04d}" for i in range(1, NUM_VEICULOS + 1)]
STATUS   = ["on_route", "at_stop", "delayed", "out_of_service"]
STATUS_PESOS = [0.75, 0.15, 0.08, 0.02]

# ─── Gerador de evento ────────────────────────────────────────────────────────
def gerar_evento_gps(vehicle_id: str, line_id: str, ts: datetime) -> dict:
    speed = round(random.gauss(35, 15), 1)
    speed = max(0.0, min(speed, 90.0))          # clamp [0, 90]
    is_outlier = random.random() < 0.005         # 0.5% de outliers reais
    if is_outlier:
        speed = round(random.uniform(100, 140), 1)

    return {
        "vehicle_id":    vehicle_id,
        "line_id":       line_id,
        "direction":     random.choice(["IDA", "VOLTA"]),
        "lat":           round(random.uniform(LAT_MIN, LAT_MAX), 6),
        "lon":           round(random.uniform(LON_MIN, LON_MAX), 6),
        "speed_kmh":     speed,
        "occupancy_pct": random.randint(0, 100),
        "engine_on":     True,
        "door_open":     random.random() < 0.10,
        "status":        random.choices(STATUS, STATUS_PESOS)[0],
        "timestamp":     ts.isoformat(),
        "schema_version": "1.0",
    }

# ─── Publicação ───────────────────────────────────────────────────────────────
def publicar_local(eventos: list, ts: datetime, base_dir: str = "bronze") -> str:
    """Salva eventos JSON em estrutura de pastas local (simula MinIO)."""
    pasta = Path(base_dir) / "gps_onibus" / f"ano={ts.year}" / f"mes={ts.month:02d}" / f"dia={ts.day:02d}" / f"hora={ts.hour:02d}"
    pasta.mkdir(parents=True, exist_ok=True)
    arquivo = pasta / f"gps_{ts.strftime('%Y%m%d_%H%M%S')}.json"
    with open(arquivo, "w") as f:
        json.dump(eventos, f, ensure_ascii=False)
    return str(arquivo)

def publicar_minio(eventos: list, ts: datetime):
    """Publica eventos no MinIO via boto3."""
    import boto3
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    )
    key = f"gps_onibus/ano={ts.year}/mes={ts.month:02d}/dia={ts.day:02d}/hora={ts.hour:02d}/gps_{ts.strftime('%Y%m%d_%H%M%S')}.json"
    s3.put_object(
        Bucket="urbanflow-bronze",
        Key=key,
        Body=json.dumps(eventos, ensure_ascii=False),
        ContentType="application/json",
    )
    return f"s3://urbanflow-bronze/{key}"

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Simulador GPS UrbanFlow")
    parser.add_argument("--local",  action="store_true", help="Salva localmente em ./bronze/")
    parser.add_argument("--minio",  action="store_true", help="Publica no MinIO")
    parser.add_argument("--ciclos", type=int, default=1, help="Número de ciclos a gerar")
    parser.add_argument("--dir",    type=str, default="bronze", help="Diretório base (modo local)")
    args = parser.parse_args()

    if not args.local and not args.minio:
        args.local = True  # default: modo local

    print(f"🚌 UrbanFlow — Simulador GPS")
    print(f"   Veículos: {NUM_VEICULOS}  |  Linhas: {NUM_LINHAS}")
    print(f"   Modo: {'local' if args.local else 'MinIO'}  |  Ciclos: {args.ciclos}")
    print()

    for ciclo in range(1, args.ciclos + 1):
        ts = datetime.now(timezone.utc)
        print(f"[Ciclo {ciclo}/{args.ciclos}] {ts.strftime('%Y-%m-%d %H:%M:%S')} UTC")

        # Gera eventos para todos os veículos
        eventos = []
        for vid, lid in zip(VEICULOS, LINHAS * (NUM_VEICULOS // NUM_LINHAS + 1)):
            eventos.append(gerar_evento_gps(vid, lid[:len(LINHAS)-1] if len(LINHAS) else lid, ts))
        # Sorteia um subset (em operação real, não rodam 100% dos ônibus 24h)
        ativos = random.sample(eventos, k=int(NUM_VEICULOS * 0.85))

        if args.local:
            caminho = publicar_local(ativos, ts, base_dir=args.dir)
            print(f"  ✅ {len(ativos)} eventos → {caminho}")
        if args.minio:
            caminho = publicar_minio(ativos, ts)
            print(f"  ✅ {len(ativos)} eventos → {caminho}")

    print(f"\n📊 Resumo:")
    print(f"   Total de eventos gerados: {args.ciclos * int(NUM_VEICULOS * 0.85)}")
    print(f"   Schema: vehicle_id, line_id, direction, lat, lon, speed_kmh, occupancy_pct, status, timestamp")

if __name__ == "__main__":
    main()