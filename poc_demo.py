"""
poc_demo.py — UrbanFlow Prova de Conceito v2.0
Executa o pipeline completo ponta a ponta SEM Docker.
Gera evidências auditáveis em poc/evidencias/

Uso:
    cd poc/
    pip install -r requirements.txt
    python poc_demo.py

Saída esperada:
    ETAPA 1 — Bronze: 900 GPS + 300 catracas + 30 bikes
    ETAPA 2 — Silver: pandas ETL → 3 Parquets Snappy
    ETAPA 3 — Gold:   dbt run PASS=5 | dbt test PASS=28
    ETAPA 4 — API:    FastAPI consulta DuckDB → KPIs ✅
"""

import sys
import os
import json
import subprocess
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import duckdb

BASE   = Path(__file__).parent
BRONZE = BASE / "bronze"
SILVER = BASE / "silver"
GOLD   = BASE / "gold"
EVID   = BASE / "evidencias"

# Adiciona ingestao/ ao path
sys.path.insert(0, str(BASE.parent / "ingestao"))

for d in [BRONZE, SILVER, GOLD, EVID]:
    d.mkdir(parents=True, exist_ok=True)

SEP = "=" * 62

def h(titulo: str):
    print(f"\n{SEP}")
    print(f"  {titulo}")
    print(SEP)

def salvar_evidencia(nome: str, conteudo: str):
    arq = EVID / nome
    arq.write_text(conteudo, encoding="utf-8")
    print(f"  📄 Evidência → evidencias/{nome}")

# ─── ETAPA 1: Simuladores → Bronze ────────────────────────────────────────────
h("ETAPA 1 — Gerando Dados Simulados (Bronze)")

from simulador_gps      import gerar_evento_gps, publicar_local as pub_gps, VEICULOS, LINHAS
from simulador_catracas import gerar_evento_catraca, publicar_local as pub_cat
from simulador_bikes    import ESTACOES_BIKE, gerar_status_estacao, publicar_local as pub_bike

ts = datetime.now(timezone.utc)
ev1_linhas = []

# GPS — 3 ciclos × 300 veículos
gps_total = 0
print("\n🚌 GPS Ônibus — 3 ciclos:")
for ciclo in range(3):
    eventos = [gerar_evento_gps(v, LINHAS[i % len(LINHAS)], ts) for i, v in enumerate(VEICULOS[:300])]
    path = pub_gps(eventos, ts, base_dir=str(BRONZE))
    gps_total += len(eventos)
    print(f"  Ciclo {ciclo+1}: {len(eventos)} eventos → {path}")
    ev1_linhas.append(f"GPS ciclo {ciclo+1}: {len(eventos)} eventos")

# Catracas — 2 ciclos
cat_total = 0
print("\n🎫 Catracas Metrô — 2 ciclos:")
for ciclo in range(2):
    eventos = [gerar_evento_catraca(ts) for _ in range(150)]
    path = pub_cat(eventos, ts, base_dir=str(BRONZE))
    cat_total += len(eventos)
    print(f"  Ciclo {ciclo+1}: {len(eventos)} eventos → {path}")

# Bikes — 1 ciclo (30 estações)
print("\n🚲 Bicicletas — 1 ciclo:")
bike_eventos = [gerar_status_estacao(e, ts) for e in ESTACOES_BIKE]
path_bike = pub_bike(bike_eventos, ts, base_dir=str(BRONZE))
print(f"  {len(bike_eventos)} estações → {path_bike}")

print(f"\n  ✅ Bronze gerado: {gps_total} GPS + {cat_total} catracas + {len(bike_eventos)} bikes")

# Evidência 1
arq_ex = next(BRONZE.glob("gps_onibus/**/*.json"))
with open(arq_ex) as f:
    exemplo = json.load(f)[0]
ev1 = (f"ETAPA 1 — Bronze Gerado\n{'─'*50}\n"
       f"GPS Ônibus:      {gps_total} eventos\n"
       f"Catracas Metrô:  {cat_total} eventos\n"
       f"Bikes:           {len(bike_eventos)} estações\n\n"
       f"Exemplo GPS:\n{json.dumps(exemplo, indent=2, ensure_ascii=False)}")
salvar_evidencia("01_bronze_gerado.txt", ev1)

# ─── ETAPA 2: Bronze → Silver ─────────────────────────────────────────────────
h("ETAPA 2 — Pipeline Bronze → Silver (pandas ETL)")

from bronze_to_silver import processar_gps, processar_catracas, processar_bikes

print("\n🚌 GPS...")
log_gps  = processar_gps(bronze_dir=BRONZE,  silver_dir=SILVER, source="local")
print("\n🎫 Catracas...")
log_cat  = processar_catracas(bronze_dir=BRONZE, silver_dir=SILVER, source="local")
print("\n🚲 Bikes...")
log_bike = processar_bikes(bronze_dir=BRONZE, silver_dir=SILVER, source="local")

parquets = list(SILVER.rglob("*.parquet"))
print(f"\n  📦 Parquets Silver: {len(parquets)} arquivo(s)")
for p in parquets:
    print(f"    {p.relative_to(BASE)}  ({p.stat().st_size/1024:.1f} KB)")

ev2 = (f"ETAPA 2 — Bronze → Silver\n{'─'*50}\n"
       f"GPS:      lidas={log_gps.lidas} escritas={log_gps.escritas} taxa={log_gps.taxa_qualidade:.1f}%\n"
       f"Catracas: lidas={log_cat.lidas} escritas={log_cat.escritas}\n"
       f"Bikes:    lidas={log_bike.lidas} escritas={log_bike.escritas}\n\n"
       f"Parquets gerados:\n" +
       "\n".join(f"  {p.relative_to(BASE)}" for p in parquets))
salvar_evidencia("02_silver_gerado.txt", ev2)

# ─── ETAPA 3: dbt Silver → Gold ───────────────────────────────────────────────
h("ETAPA 3 — dbt Core: Silver → Gold")

DBT_DIR = BASE.parent / "dbt_project"
GOLD_DB = GOLD / "urbanflow.duckdb"

env = os.environ.copy()
env["URBANFLOW_SILVER_DIR"] = str(SILVER.resolve())
env["URBANFLOW_GOLD_DIR"]   = str(GOLD.resolve())

def run_dbt(cmd: list) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd + ["--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)],
        capture_output=True, text=True, env=env
    )

print("\n  dbt run...")
res_run  = run_dbt(["dbt", "run"])
print(res_run.stdout[-3000:])

print("\n  dbt test...")
res_test = run_dbt(["dbt", "test"])
print(res_test.stdout[-2000:])

ev3 = (f"ETAPA 3 — dbt run + dbt test\n{'─'*50}\n"
       f"dbt run  returncode: {res_run.returncode}\n"
       f"dbt test returncode: {res_test.returncode}\n\n"
       f"── dbt run ──\n{res_run.stdout[-2000:]}\n\n"
       f"── dbt test ──\n{res_test.stdout[-2000:]}")
salvar_evidencia("03_dbt_run_test.txt", ev3)

# ─── ETAPA 4: Queries DuckDB ──────────────────────────────────────────────────
h("ETAPA 4 — Consultas DuckDB (Gold)")

conn   = duckdb.connect(str(GOLD_DB)) if GOLD_DB.exists() else duckdb.connect()
ev4    = f"ETAPA 4 — Consultas DuckDB Gold\n{'─'*50}\n"
silver = str(SILVER.resolve())

# Consulta 1: KPIs por linha
print("\n📊 KPI Operacional por Linha:")
q1 = f"""
    SELECT line_id, COUNT(*) AS eventos,
           ROUND(AVG(speed_kmh::DOUBLE), 1) AS vel_media,
           ROUND(AVG(occupancy_pct::INT), 1) AS ocup_media
    FROM read_parquet('{silver}/gps_onibus_clean/**/*.parquet')
    GROUP BY line_id ORDER BY eventos DESC LIMIT 10
"""
df_kpi = conn.execute(q1).fetchdf()
print(df_kpi.to_string(index=False))
ev4 += f"\nKPI por linha (top 10):\n{df_kpi.to_string()}\n"

# Consulta 2: Demanda por estação
print("\n📊 Demanda por Estação:")
q2 = f"""
    SELECT station_id::VARCHAR AS estacao,
           SUM(CASE WHEN direction='ENTRY' THEN 1 ELSE 0 END) AS entradas,
           SUM(CASE WHEN direction='EXIT'  THEN 1 ELSE 0 END) AS saidas,
           ROUND(SUM(fare_paid::DOUBLE), 2)                   AS receita_brl
    FROM read_parquet('{silver}/catracas_clean/**/*.parquet')
    GROUP BY station_id ORDER BY entradas DESC LIMIT 18
"""
df_dem = conn.execute(q2).fetchdf()
print(df_dem.to_string(index=False))
ev4 += f"\nDemanda por estação:\n{df_dem.to_string()}\n"

# Consulta 3: Resumo geral
print("\n📊 Resumo Geral:")
q3 = f"""
    SELECT
        (SELECT COUNT(DISTINCT vehicle_id) FROM read_parquet('{silver}/gps_onibus_clean/**/*.parquet')) AS veiculos,
        (SELECT COUNT(*) FROM read_parquet('{silver}/gps_onibus_clean/**/*.parquet'))                   AS eventos_gps,
        (SELECT COUNT(*) FROM read_parquet('{silver}/catracas_clean/**/*.parquet'))                     AS eventos_catracas,
        (SELECT ROUND(SUM(fare_paid::DOUBLE),2) FROM read_parquet('{silver}/catracas_clean/**/*.parquet')) AS receita_brl
"""
df_res = conn.execute(q3).fetchdf()
print(df_res.to_string(index=False))
ev4 += f"\nResumo geral:\n{df_res.to_string()}\n"

conn.close()
salvar_evidencia("04_duckdb_queries.txt", ev4)

# ─── Resultado Final ──────────────────────────────────────────────────────────
h("✅ PIPELINE CONCLUÍDO — UrbanFlow v2.0")

total_lidas   = log_gps.lidas + log_cat.lidas + log_bike.lidas
total_escritas = log_gps.escritas + log_cat.escritas + log_bike.escritas
taxa = (total_escritas / total_lidas * 100) if total_lidas > 0 else 0

print(f"""
  Bronze gerado:   {gps_total} GPS + {cat_total} catracas + {len(bike_eventos)} bikes
  Silver gerado:   {total_escritas}/{total_lidas} registros (taxa: {taxa:.1f}%)
  dbt run:         {'✅ PASS' if res_run.returncode == 0 else '❌ FAIL'}
  dbt test:        {'✅ PASS' if res_test.returncode == 0 else '❌ FAIL (verifique evidencias/)'}
  DuckDB consultas: ✅ OK

  Evidências em: {EVID.relative_to(BASE.parent)}
""")

ev_final = (f"RESULTADO FINAL\n{'─'*50}\n"
            f"Bronze:   {gps_total + cat_total + len(bike_eventos)} eventos totais\n"
            f"Silver:   {total_escritas} registros válidos ({taxa:.1f}% qualidade)\n"
            f"dbt run:  {'PASS' if res_run.returncode == 0 else 'FAIL'}\n"
            f"dbt test: {'PASS' if res_test.returncode == 0 else 'FAIL'}\n")
salvar_evidencia("05_resultado_final.txt", ev_final)
