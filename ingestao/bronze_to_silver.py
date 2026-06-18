"""
bronze_to_silver.py — UrbanFlow Transformação
Pipeline pandas: Bronze (JSON/MinIO) → Silver (Parquet validado/MinIO)

Etapas:
  - Leitura de JSON do Bronze (local ou MinIO)
  - Deduplicação por chave composta
  - Normalização de timestamps para UTC ISO 8601
  - Remoção de registros com campos obrigatórios nulos (→ quarentena)
  - Sinalização de outliers (is_outlier=True — não deleta)
  - Enriquecimento com dimensão de veículos
  - Exportação em Parquet comprimido (Snappy) no Silver

Uso:
    python bronze_to_silver.py --source local --bronze-dir bronze --silver-dir silver
    python bronze_to_silver.py --source minio
"""

import json
import sys
import io
import os
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger("bronze_to_silver")

# ─── Schemas ─────────────────────────────────────────────────────────────────
SCHEMA_GPS = {
    "obrigatorios": ["vehicle_id", "line_id", "lat", "lon", "speed_kmh", "timestamp"],
    "chave_dedup":  ["vehicle_id", "timestamp"],
    "outlier_col":  "speed_kmh",
    "outlier_max":  120.0,
}
SCHEMA_CATRACAS = {
    "obrigatorios": ["event_id", "station_id", "direction", "card_hash", "timestamp"],
    "chave_dedup":  ["event_id"],
    "outlier_col":  None,
}
SCHEMA_BIKES = {
    "obrigatorios": ["station_id", "bikes_available", "docks_available", "last_reported"],
    "chave_dedup":  ["station_id", "last_reported"],
    "outlier_col":  None,
}

# ─── Dimensão de veículos ─────────────────────────────────────────────────────
DIM_VEICULOS = pd.DataFrame([
    {"vehicle_id": f"BUS-{i:04d}",
     "tipo": "onibus",
     "capacidade": [60, 80, 100][(i - 1) % 3],
     "linha_principal": f"L{((i-1)%120)+1:03d}"}
    for i in range(1, 851)
])

# ─── Log de pipeline ─────────────────────────────────────────────────────────
class PipelineLog:
    def __init__(self, fonte: str):
        self.fonte               = fonte
        self.lidas               = 0
        self.rejeitadas_nulos    = 0
        self.rejeitadas_dedup    = 0
        self.quarentena          = 0
        self.outliers            = 0
        self.escritas            = 0
        self.inicio              = datetime.now()

    @property
    def taxa_qualidade(self) -> float:
        return (self.escritas / self.lidas * 100) if self.lidas > 0 else 0.0

    def resumo(self) -> str:
        dur = (datetime.now() - self.inicio).total_seconds()
        return (
            f"\n{'─'*55}\n"
            f"  Fonte             : {self.fonte}\n"
            f"  Lidas             : {self.lidas:>6,}\n"
            f"  Rej. nulos        : {self.rejeitadas_nulos:>6,}\n"
            f"  Rej. dedup        : {self.rejeitadas_dedup:>6,}\n"
            f"  Quarentena        : {self.quarentena:>6,}\n"
            f"  Outliers flag     : {self.outliers:>6,}\n"
            f"  Escritas Silver   : {self.escritas:>6,}\n"
            f"  Taxa qualidade    : {self.taxa_qualidade:>5.1f}%\n"
            f"  Duração           : {dur:.2f}s\n"
            f"{'─'*55}"
        )

# ─── Helper S3/MinIO ─────────────────────────────────────────────────────────
def _get_s3_client():
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        region_name="us-east-1",
    )

def _listar_objetos_minio(prefixo: str, bucket: str = "urbanflow-bronze") -> list:
    s3 = _get_s3_client()
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefixo)
    return [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".json")]

def _ler_json_minio(key: str, bucket: str = "urbanflow-bronze") -> list:
    s3   = _get_s3_client()
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    dados = json.loads(body)
    return dados if isinstance(dados, list) else [dados]

def _escrever_parquet_minio(df: pd.DataFrame, key: str, bucket: str = "urbanflow-silver"):
    buf = io.BytesIO()
    df.to_parquet(buf, compression="snappy", index=False, engine="pyarrow")
    buf.seek(0)
    s3 = _get_s3_client()
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue(),
                  ContentType="application/octet-stream")

def _escrever_quarentena_minio(df: pd.DataFrame, key: str):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    s3 = _get_s3_client()
    s3.put_object(Bucket="urbanflow-quarentena", Key=key, Body=buf.getvalue())

# ─── Pipeline GPS ─────────────────────────────────────────────────────────────
def processar_gps(bronze_dir: Path = None, silver_dir: Path = None,
                  source: str = "local") -> PipelineLog:
    pipeline = PipelineLog("gps_onibus")
    dfs = []

    if source == "minio":
        keys = _listar_objetos_minio("gps_onibus/")
        if not keys:
            log.warning("Nenhum objeto GPS no MinIO.")
            return pipeline
        for k in keys:
            dfs.append(pd.DataFrame(_ler_json_minio(k)))
        log.info(f"  📡 {len(keys)} objeto(s) lido(s) do MinIO")
    else:
        arquivos = list(bronze_dir.glob("gps_onibus/**/*.json"))
        if not arquivos:
            log.warning("Nenhum arquivo GPS no Bronze local.")
            return pipeline
        for arq in arquivos:
            with open(arq) as f:
                dados = json.load(f)
            dfs.append(pd.DataFrame(dados if isinstance(dados, list) else [dados]))
        log.info(f"  📂 {len(arquivos)} arquivo(s) lido(s)")

    df = pd.concat(dfs, ignore_index=True)
    pipeline.lidas = len(df)

    # 1. Quarentena — campos obrigatórios nulos
    mask_nulos = df[SCHEMA_GPS["obrigatorios"]].isnull().any(axis=1)
    quarentena_df = df[mask_nulos].copy()
    quarentena_df["_motivo_rejeicao"] = "campo_obrigatorio_nulo"
    pipeline.quarentena = len(quarentena_df)
    df = df[~mask_nulos]
    pipeline.rejeitadas_nulos = pipeline.quarentena

    if len(quarentena_df) > 0:
        ts_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        if source == "minio":
            _escrever_quarentena_minio(quarentena_df, f"gps_nulos_{ts_str}.parquet")
        else:
            q_dir = bronze_dir.parent / "quarentena" / "gps_nulos"
            q_dir.mkdir(parents=True, exist_ok=True)
            quarentena_df.to_parquet(q_dir / f"{ts_str}.parquet", index=False)

    # 2. Deduplicação
    antes = len(df)
    df = df.drop_duplicates(subset=SCHEMA_GPS["chave_dedup"])
    pipeline.rejeitadas_dedup = antes - len(df)

    # 3. Timestamps UTC
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    # 4. Outliers — não deleta, sinaliza
    df["is_outlier"] = df[SCHEMA_GPS["outlier_col"]] > SCHEMA_GPS["outlier_max"]
    pipeline.outliers = int(df["is_outlier"].sum())

    # 5. Coordenadas válidas
    df = df[df["lat"].between(-90, 90) & df["lon"].between(-180, 180)]

    # 6. Enriquecimento com dimensão de veículos
    df = df.merge(DIM_VEICULOS[["vehicle_id", "tipo", "capacidade"]], on="vehicle_id", how="left")

    # 7. Metadados de pipeline
    df["_processed_at"] = pd.Timestamp.now(tz="UTC")
    df["_source"]       = "bronze/gps_onibus"
    df["_pipeline_ver"] = "2.0"

    # 8. Escrita Silver
    data_str = df["timestamp"].dt.date.iloc[0].isoformat() if len(df) > 0 else "2026-01-01"
    pipeline.escritas = len(df)

    if source == "minio":
        key = f"gps_onibus_clean/data={data_str}/part-00000.snappy.parquet"
        _escrever_parquet_minio(df, key)
        log.info(f"  ✅ {pipeline.escritas} registros → s3://urbanflow-silver/{key}")
    if silver_dir is not None:
        saida = silver_dir / "gps_onibus_clean" / f"data={data_str}"
        saida.mkdir(parents=True, exist_ok=True)
        df.to_parquet(saida / "part-00000.snappy.parquet",
                      compression="snappy", index=False, engine="pyarrow")
        log.info(f"  ✅ {pipeline.escritas} registros → {saida}/part-00000.snappy.parquet")

    print(pipeline.resumo())
    return pipeline

# ─── Pipeline Catracas ────────────────────────────────────────────────────────
def processar_catracas(bronze_dir: Path = None, silver_dir: Path = None,
                       source: str = "local") -> PipelineLog:
    pipeline = PipelineLog("catracas")
    dfs = []

    if source == "minio":
        keys = _listar_objetos_minio("catracas/")
        if not keys:
            log.warning("Nenhum objeto catracas no MinIO.")
            return pipeline
        for k in keys:
            dfs.append(pd.DataFrame(_ler_json_minio(k)))
    else:
        arquivos = list(bronze_dir.glob("catracas/**/*.json"))
        if not arquivos:
            log.warning("Nenhum arquivo catracas no Bronze.")
            return pipeline
        for arq in arquivos:
            with open(arq) as f:
                dados = json.load(f)
            dfs.append(pd.DataFrame(dados if isinstance(dados, list) else [dados]))

    df = pd.concat(dfs, ignore_index=True)
    pipeline.lidas = len(df)

    # Qualidade
    mask_nulos = df[SCHEMA_CATRACAS["obrigatorios"]].isnull().any(axis=1)
    pipeline.rejeitadas_nulos = int(mask_nulos.sum())
    df = df[~mask_nulos]

    antes = len(df)
    df = df.drop_duplicates(subset=SCHEMA_CATRACAS["chave_dedup"])
    pipeline.rejeitadas_dedup = antes - len(df)

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df[df["direction"].isin(["ENTRY", "EXIT"])]

    if "fare_paid" in df.columns:
        df = df[df["fare_paid"] >= 0]

    df["_processed_at"] = pd.Timestamp.now(tz="UTC")
    df["_source"]       = "bronze/catracas"
    df["_pipeline_ver"] = "2.0"

    data_str = df["timestamp"].dt.date.iloc[0].isoformat() if len(df) > 0 else "2026-01-01"
    pipeline.escritas = len(df)

    if source == "minio":
        key = f"catracas_clean/data={data_str}/part-00000.snappy.parquet"
        _escrever_parquet_minio(df, key)
        log.info(f"  ✅ {pipeline.escritas} registros catracas → MinIO Silver")
    if silver_dir is not None:
        saida = silver_dir / "catracas_clean" / f"data={data_str}"
        saida.mkdir(parents=True, exist_ok=True)
        df.to_parquet(saida / "part-00000.snappy.parquet",
                      compression="snappy", index=False, engine="pyarrow")
        log.info(f"  ✅ {pipeline.escritas} registros → {saida}")

    print(pipeline.resumo())
    return pipeline

# ─── Pipeline Bikes ───────────────────────────────────────────────────────────
def processar_bikes(bronze_dir: Path = None, silver_dir: Path = None,
                    source: str = "local") -> PipelineLog:
    pipeline = PipelineLog("bikes")
    dfs = []

    if source == "minio":
        keys = _listar_objetos_minio("bikes/")
        if not keys:
            return pipeline
        for k in keys:
            dfs.append(pd.DataFrame(_ler_json_minio(k)))
    else:
        arquivos = list(bronze_dir.glob("bikes/**/*.json"))
        if not arquivos:
            return pipeline
        for arq in arquivos:
            with open(arq) as f:
                dados = json.load(f)
            dfs.append(pd.DataFrame(dados if isinstance(dados, list) else [dados]))

    df = pd.concat(dfs, ignore_index=True)
    pipeline.lidas = len(df)
    df = df.dropna(subset=SCHEMA_BIKES["obrigatorios"])
    df = df.drop_duplicates(subset=SCHEMA_BIKES["chave_dedup"])
    df["last_reported"] = pd.to_datetime(df["last_reported"], utc=True)
    df["_processed_at"] = pd.Timestamp.now(tz="UTC")
    df["_source"]       = "bronze/bikes"
    pipeline.escritas   = len(df)

    data_str = df["last_reported"].dt.date.iloc[0].isoformat() if len(df) > 0 else "2026-01-01"
    if source == "minio":
        key = f"bikes_clean/data={data_str}/part-00000.snappy.parquet"
        _escrever_parquet_minio(df, key)
    if silver_dir is not None:
        saida = silver_dir / "bikes_clean" / f"data={data_str}"
        saida.mkdir(parents=True, exist_ok=True)
        df.to_parquet(saida / "part-00000.snappy.parquet",
                      compression="snappy", index=False, engine="pyarrow")

    log.info(f"  ✅ {pipeline.escritas} estações bikes → Silver")
    print(pipeline.resumo())
    return pipeline

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Pipeline Bronze → Silver UrbanFlow")
    parser.add_argument("--source",     choices=["local", "minio"], default="local")
    parser.add_argument("--bronze-dir", type=str, default="bronze")
    parser.add_argument("--silver-dir", type=str, default="silver")
    args = parser.parse_args()

    bronze = Path(args.bronze_dir)
    silver = Path(args.silver_dir)
    silver.mkdir(parents=True, exist_ok=True)

    print("=" * 55)
    print("  UrbanFlow — Pipeline Bronze → Silver v2.0")
    print("=" * 55)

    logs = []
    for fn, nome in [(processar_gps, "GPS"), (processar_catracas, "Catracas"), (processar_bikes, "Bikes")]:
        print(f"\n🔄 Processando {nome}...")
        try:
            logs.append(fn(bronze_dir=bronze, silver_dir=silver, source=args.source))
        except Exception as e:
            log.error(f"Erro ao processar {nome}: {e}")

    total_lidas   = sum(l.lidas for l in logs)
    total_escritas = sum(l.escritas for l in logs)
    taxa = (total_escritas / total_lidas * 100) if total_lidas > 0 else 0

    print(f"\n{'='*55}")
    print(f"  PIPELINE CONCLUÍDO")
    print(f"  Total lidas  : {total_lidas:,}")
    print(f"  Total Silver : {total_escritas:,}")
    print(f"  Taxa qualidade: {taxa:.1f}%  (meta: > 99%)")
    print(f"{'='*55}")

if __name__ == "__main__":
    main()
