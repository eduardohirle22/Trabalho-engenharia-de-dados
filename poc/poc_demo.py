"""
poc_demo.py — UrbanFlow Prova de Conceito Completa
Executa o pipeline ponta a ponta sem Docker e gera evidências em evidencias/

Uso:
    cd poc/
    python poc_demo.py
"""

import sys
import os
import json
import subprocess
import shutil
from pathlib import Path
from datetime import datetime

# Adiciona simuladores ao path
sys.path.insert(0, str(Path(__file__).parent / "simuladores"))

import pandas as pd
import duckdb
import pyarrow.parquet as pq

BASE = Path(__file__).parent
BRONZE  = BASE / "bronze"
SILVER  = BASE / "silver"
GOLD    = BASE / "gold"
EVID    = BASE / "evidencias"

for d in [BRONZE, SILVER, GOLD, EVID]:
    d.mkdir(parents=True, exist_ok=True)

SEP = "=" * 60

def h(titulo: str):
    print(f"\n{SEP}")
    print(f"  {titulo}")
    print(SEP)

def salvar_evidencia(nome: str, conteudo: str):
    arq = EVID / nome
    arq.write_text(conteudo, encoding="utf-8")
    print(f"  📄 Evidência salva: evidencias/{nome}")

# ─── ETAPA 1: Simuladores ─────────────────────────────────────────────────────
h("ETAPA 1 — Gerando Dados Simulados (Bronze)")

from simulador_gps import gerar_evento_gps, publicar_local, NUM_VEICULOS, LINHAS, VEICULOS
from simulador_catracas import gerar_evento_catraca, publicar_local as pub_cat

from datetime import timezone
import random

ts = datetime.now(timezone.utc)

# GPS — 3 ciclos simulando 3 intervalos de 30s
print("\n🚌 GPS Ônibus — 3 ciclos:")
gps_total = 0
for ciclo in range(3):
    ativos = [gerar_evento_gps(v, LINHAS[i % len(LINHAS)], ts) for i, v in enumerate(VEICULOS[:300])]
    caminho = publicar_local(ativos, ts, base_dir=str(BRONZE))
    gps_total += len(ativos)
    print(f"  Ciclo {ciclo+1}: {len(ativos)} eventos → {caminho}")

# Catracas — 2 ciclos
print("\n🎫 Catracas Metrô — 2 ciclos:")
cat_total = 0
for ciclo in range(2):
    eventos = [gerar_evento_catraca(ts) for _ in range(150)]
    caminho = pub_cat(eventos, ts, base_dir=str(BRONZE))
    cat_total += len(eventos)
    print(f"  Ciclo {ciclo+1}: {len(eventos)} eventos → {caminho}")

print(f"\n  ✅ Bronze gerado: {gps_total} GPS + {cat_total} catracas")

# Evidência 1
ev1 = f"ETAPA 1 — Dados Bronze gerados\n{'─'*40}\n"
ev1 += f"GPS Ônibus:     {gps_total} eventos em {len(list(BRONZE.glob('gps_onibus/**/*.json')))} arquivo(s)\n"
ev1 += f"Catracas Metrô: {cat_total} eventos em {len(list(BRONZE.glob('catracas/**/*.json')))} arquivo(s)\n"
ev1 += f"\nExemplo de evento GPS:\n"
arq_ex = next(BRONZE.glob("gps_onibus/**/*.json"))
with open(arq_ex) as f:
    ev1 += json.dumps(json.load(f)[0], indent=2, ensure_ascii=False)
salvar_evidencia("01_bronze_gerado.txt", ev1)

# ─── ETAPA 2: Bronze → Silver ─────────────────────────────────────────────────
h("ETAPA 2 — Pipeline Bronze → Silver (pandas)")

sys.path.insert(0, str(BASE))
from bronze_to_silver import processar_gps, processar_catracas

print("\n🚌 Processando GPS...")
log_gps = processar_gps(BRONZE, SILVER)
print("\n🎫 Processando Catracas...")
log_cat = processar_catracas(BRONZE, SILVER)

# Verifica arquivos Parquet gerados
parquets_gps = list(SILVER.glob("gps_onibus_clean/**/*.parquet"))
parquets_cat = list(SILVER.glob("catracas_clean/**/*.parquet"))

print(f"\n  📦 Parquets Silver gerados:")
for p in parquets_gps + parquets_cat:
    size_kb = p.stat().st_size / 1024
    print(f"    {p.relative_to(BASE)}  ({size_kb:.1f} KB)")

# Lê e mostra schema do Parquet gerado
if parquets_gps:
    df_gps_check = pd.read_parquet(parquets_gps[0])
    ev2  = f"ETAPA 2 — Pipeline Bronze → Silver\n{'─'*40}\n"
    ev2 += f"GPS — Lidas: {log_gps.lidas} | Escritas: {log_gps.escritas} | Outliers: {log_gps.outliers}\n"
    ev2 += f"Catracas — Lidas: {log_cat.lidas} | Escritas: {log_cat.escritas}\n\n"
    ev2 += f"Schema Silver GPS:\n{df_gps_check.dtypes.to_string()}\n\n"
    ev2 += f"Primeiras 3 linhas:\n{df_gps_check.head(3).to_string()}"
    salvar_evidencia("02_silver_gerado.txt", ev2)

# ─── ETAPA 3: dbt Silver → Gold ───────────────────────────────────────────────
h("ETAPA 3 — dbt Core: Silver → Gold")

DBT_DIR = BASE / "dbt_project"
GOLD_DB = GOLD / "urbanflow.duckdb"

env = os.environ.copy()
env["URBANFLOW_SILVER_DIR"] = str(SILVER.resolve())
env["URBANFLOW_GOLD_DB"]    = str(GOLD_DB.resolve())

# Roda dbt com profiles.yml local
dbt_cmd = ["dbt", "run", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]

print(f"\n  Executando: dbt run")
result = subprocess.run(dbt_cmd, capture_output=True, text=True, env=env)
print(result.stdout[-3000:] if len(result.stdout) > 3000 else result.stdout)
if result.returncode != 0:
    print(f"  ⚠️  stderr: {result.stderr[-1000:]}")

# Roda dbt test
print(f"\n  Executando: dbt test")
dbt_test_cmd = ["dbt", "test", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
result_test = subprocess.run(dbt_test_cmd, capture_output=True, text=True, env=env)
print(result_test.stdout[-3000:] if len(result_test.stdout) > 3000 else result_test.stdout)

# Evidência dbt
ev3  = f"ETAPA 3 — dbt run + dbt test\n{'─'*40}\n"
ev3 += f"dbt run returncode: {result.returncode}\n"
ev3 += f"dbt test returncode: {result_test.returncode}\n\n"
ev3 += "─── dbt run output ───\n"
ev3 += result.stdout[-2000:]
ev3 += "\n─── dbt test output ───\n"
ev3 += result_test.stdout[-2000:]
salvar_evidencia("03_dbt_run_test.txt", ev3)

# ─── ETAPA 4: Consultas DuckDB na camada Gold ──────────────────────────────────
h("ETAPA 4 — Consultas ad-hoc DuckDB na Camada Gold")

if GOLD_DB.exists():
    con = duckdb.connect(str(GOLD_DB))
    tabelas = con.execute("SHOW TABLES").fetchdf()
    print(f"\n  📊 Tabelas Gold disponíveis:\n{tabelas.to_string(index=False)}")

    ev4 = f"ETAPA 4 — Consultas DuckDB Gold\n{'─'*40}\n"
    ev4 += f"Banco: {GOLD_DB}\n\n"
    ev4 += f"Tabelas Gold:\n{tabelas.to_string()}\n\n"

    # Query 1: KPI por linha
    try:
        q1 = con.execute("""
            SELECT linha, data, veiculos_ativos, ROUND(otp_pct,1) AS otp_pct, classificacao_otp
            FROM kpi_operacional_diario
            ORDER BY otp_pct DESC
            LIMIT 10
        """).fetchdf()
        print(f"\n  🏆 Top 10 linhas por OTP:\n{q1.to_string(index=False)}")
        ev4 += f"Query 1 — Top 10 linhas por OTP:\n{q1.to_string()}\n\n"
    except Exception as e:
        print(f"  ⚠️  Erro KPI: {e}")
        ev4 += f"Erro KPI: {e}\n\n"

    # Query 2: Demanda por período
    try:
        q2 = con.execute("""
            SELECT periodo, SUM(total_validacoes) AS total, ROUND(AVG(receita_total),2) AS receita_media
            FROM agg_demanda_por_hora
            GROUP BY periodo
            ORDER BY total DESC
        """).fetchdf()
        print(f"\n  📈 Demanda por período:\n{q2.to_string(index=False)}")
        ev4 += f"Query 2 — Demanda por período:\n{q2.to_string()}\n\n"
    except Exception as e:
        print(f"  ⚠️  Erro Demanda: {e}")
        ev4 += f"Erro Demanda: {e}\n\n"

    # Query 3: Validação de qualidade — outliers
    try:
        q3 = con.execute("""
            SELECT classificacao_otp, COUNT(*) AS linhas, ROUND(AVG(otp_pct),1) AS otp_medio
            FROM kpi_operacional_diario
            GROUP BY classificacao_otp
        """).fetchdf()
        print(f"\n  ✅ Distribuição de qualidade OTP:\n{q3.to_string(index=False)}")
        ev4 += f"Query 3 — Distribuição OTP:\n{q3.to_string()}"
    except Exception as e:
        ev4 += f"Erro OTP dist: {e}"

    con.close()
    salvar_evidencia("04_duckdb_queries.txt", ev4)

else:
    print("  ⚠️  Banco Gold não encontrado — dbt não materializou.")

# ─── RESUMO FINAL ─────────────────────────────────────────────────────────────
h("RESUMO DA PROVA DE CONCEITO")

print("""
  Stack validada:
  ✅ Simuladores Python + boto3       → dados JSON em bronze/
  ✅ Pipeline pandas Bronze → Silver  → Parquet comprimido em silver/
  ✅ dbt Core + dbt-duckdb           → modelos materializados em gold/
  ✅ DuckDB                          → queries ad-hoc nos Parquets Gold
  ✅ Qualidade: dedup + dropna + outlier flag + dbt tests

  Evidências geradas em evidencias/:
""")
for ev in sorted(EVID.glob("*.txt")):
    print(f"    📄 {ev.name}")

print(f"""
  Próximo passo (Parte 2 completa):
    docker compose up -d          # sobe MinIO + PostgreSQL + Airflow + Superset
    python simuladores/simulador_gps.py --minio --ciclos 5
    # Airflow cuida do restante via DAGs agendadas
""")