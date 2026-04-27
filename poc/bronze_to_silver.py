"""
bronze_to_silver.py — UrbanFlow Prova de Conceito
Pipeline pandas: Bronze (JSON) → Silver (Parquet validado)

Executa transformações na camada Silver:
  - Deduplicação por chave composta
  - Normalização de timestamps para UTC ISO 8601
  - Remoção de registros com campos obrigatórios nulos
  - Sinalização de outliers (is_outlier = True, não deleta)
  - Enriquecimento com dimensão de veículos
  - Exportação em Parquet comprimido (Snappy)

Uso: python bronze_to_silver.py --bronze-dir bronze --silver-dir silver
"""

import json
import sys
import argparse
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ─── Schema esperado por fonte ─────────────────────────────────────────────────
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

# ─── Dimensão de veículos (simplificada) ──────────────────────────────────────
DIM_VEICULOS = pd.DataFrame([
    {"vehicle_id": f"BUS-{i:04d}", "tipo": "onibus", "capacidade": random_cap, "linha_principal": f"L{((i-1)%120)+1:03d}"}
    for i, random_cap in [(i, [60,80,100][(i-1)%3]) for i in range(1, 851)]
])

# ─── Helpers ──────────────────────────────────────────────────────────────────
class PipelineLog:
    def __init__(self, fonte: str):
        self.fonte  = fonte
        self.lidas  = 0
        self.rejeitadas_nulos = 0
        self.rejeitadas_dedup = 0
        self.outliers         = 0
        self.escritas = 0
        self.inicio   = datetime.now()

    def resumo(self) -> str:
        dur = (datetime.now() - self.inicio).total_seconds()
        return (
            f"\n{'─'*55}\n"
            f"  Fonte         : {self.fonte}\n"
            f"  Lidas         : {self.lidas:>6,}\n"
            f"  Rej. nulos    : {self.rejeitadas_nulos:>6,}\n"
            f"  Rej. dedup    : {self.rejeitadas_dedup:>6,}\n"
            f"  Outliers flag : {self.outliers:>6,}\n"
            f"  Escritas      : {self.escritas:>6,}\n"
            f"  Duração       : {dur:.2f}s\n"
            f"{'─'*55}"
        )

# ─── Pipeline GPS ─────────────────────────────────────────────────────────────
def processar_gps(bronze_dir: Path, silver_dir: Path) -> PipelineLog:
    log  = PipelineLog("gps_onibus")
    dfs  = []

    # Lê todos os arquivos JSON do Bronze
    arquivos = list(bronze_dir.glob("gps_onibus/**/*.json"))
    if not arquivos:
        print("  ⚠️  Nenhum arquivo GPS encontrado no Bronze.")
        return log

    for arq in arquivos:
        with open(arq) as f:
            dados = json.load(f)
        df = pd.DataFrame(dados) if isinstance(dados, list) else pd.DataFrame([dados])
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)
    log.lidas = len(df)
    print(f"  📂 {len(arquivos)} arquivo(s) lido(s) | {log.lidas} registros")

    # 1. Remover campos obrigatórios nulos
    antes = len(df)
    df = df.dropna(subset=SCHEMA_GPS["obrigatorios"])
    log.rejeitadas_nulos = antes - len(df)
    print(f"  🔍 Nulos removidos: {log.rejeitadas_nulos}")

    # 2. Deduplicação
    antes = len(df)
    df = df.drop_duplicates(subset=SCHEMA_GPS["chave_dedup"])
    log.rejeitadas_dedup = antes - len(df)
    print(f"  🔍 Duplicatas removidas: {log.rejeitadas_dedup}")

    # 3. Normalizar timestamps para UTC
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    # 4. Sinalizar outliers (não deletar)
    col = SCHEMA_GPS["outlier_col"]
    df["is_outlier"] = df[col] > SCHEMA_GPS["outlier_max"]
    log.outliers = int(df["is_outlier"].sum())
    print(f"  ⚠️  Outliers sinalizados (speed_kmh > 120): {log.outliers}")

    # 5. Validar lat/lon (coordenadas geograficamente plausíveis)
    df = df[df["lat"].between(-90, 90) & df["lon"].between(-180, 180)]

    # 6. Enriquecer com dimensão de veículos
    df = df.merge(DIM_VEICULOS[["vehicle_id", "tipo", "capacidade"]], on="vehicle_id", how="left")

    # 7. Adicionar metadados de pipeline
    df["_processed_at"] = pd.Timestamp.now(tz="UTC")
    df["_source"] = "bronze/gps_onibus"

    # 8. Escrever Parquet
    data_str = df["timestamp"].dt.date.iloc[0].isoformat() if len(df) > 0 else "2026-01-01"
    saida = silver_dir / "gps_onibus_clean" / f"data={data_str}"
    saida.mkdir(parents=True, exist_ok=True)
    arquivo_saida = saida / "part-00000.snappy.parquet"
    df.to_parquet(arquivo_saida, compression="snappy", index=False, engine="pyarrow")

    log.escritas = len(df)
    print(f"  ✅ Escritos: {log.escritas} registros → {arquivo_saida}")
    print(f"  📦 Colunas Silver: {list(df.columns)}")
    return log

# ─── Pipeline Catracas ────────────────────────────────────────────────────────
def processar_catracas(bronze_dir: Path, silver_dir: Path) -> PipelineLog:
    log  = PipelineLog("catracas")
    dfs  = []

    arquivos = list(bronze_dir.glob("catracas/**/*.json"))
    if not arquivos:
        print("  ⚠️  Nenhum arquivo de catracas encontrado no Bronze.")
        return log

    for arq in arquivos:
        with open(arq) as f:
            dados = json.load(f)
        df = pd.DataFrame(dados) if isinstance(dados, list) else pd.DataFrame([dados])
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)
    log.lidas = len(df)
    print(f"  📂 {len(arquivos)} arquivo(s) lido(s) | {log.lidas} registros")

    # 1. Remover nulos obrigatórios
    antes = len(df)
    df = df.dropna(subset=SCHEMA_CATRACAS["obrigatorios"])
    log.rejeitadas_nulos = antes - len(df)

    # 2. Deduplicação por event_id
    antes = len(df)
    df = df.drop_duplicates(subset=SCHEMA_CATRACAS["chave_dedup"])
    log.rejeitadas_dedup = antes - len(df)
    print(f"  🔍 Nulos: {log.rejeitadas_nulos} | Duplicatas: {log.rejeitadas_dedup}")

    # 3. Normalizar timestamps
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    # 4. Validar direction
    df = df[df["direction"].isin(["ENTRY", "EXIT"])]

    # 5. Validar fare_paid >= 0
    if "fare_paid" in df.columns:
        df = df[df["fare_paid"] >= 0]

    # 6. Metadados de pipeline
    df["_processed_at"] = pd.Timestamp.now(tz="UTC")
    df["_source"] = "bronze/catracas"

    # 7. Escrever Parquet
    data_str = df["timestamp"].dt.date.iloc[0].isoformat() if len(df) > 0 else "2026-01-01"
    saida = silver_dir / "catracas_clean" / f"data={data_str}"
    saida.mkdir(parents=True, exist_ok=True)
    arquivo_saida = saida / "part-00000.snappy.parquet"
    df.to_parquet(arquivo_saida, compression="snappy", index=False, engine="pyarrow")

    log.escritas = len(df)
    print(f"  ✅ Escritos: {log.escritas} registros → {arquivo_saida}")
    return log

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Pipeline Bronze → Silver UrbanFlow")
    parser.add_argument("--bronze-dir", type=str, default="bronze")
    parser.add_argument("--silver-dir", type=str, default="silver")
    args = parser.parse_args()

    bronze = Path(args.bronze_dir)
    silver = Path(args.silver_dir)
    silver.mkdir(parents=True, exist_ok=True)

    print("=" * 55)
    print("  UrbanFlow — Pipeline Bronze → Silver")
    print("=" * 55)

    logs = []

    print("\n🚌 Processando GPS Ônibus...")
    logs.append(processar_gps(bronze, silver))

    print("\n🎫 Processando Catracas Metrô...")
    logs.append(processar_catracas(bronze, silver))

    print("\n" + "=" * 55)
    print("  RESUMO DO PIPELINE")
    for log in logs:
        print(log.resumo())

    total_lidas   = sum(l.lidas for l in logs)
    total_escritas = sum(l.escritas for l in logs)
    taxa_qualidade = (total_escritas / total_lidas * 100) if total_lidas > 0 else 0
    print(f"\n  Taxa de qualidade: {taxa_qualidade:.1f}%")
    print(f"  (meta: > 99%)")

if __name__ == "__main__":
    main()