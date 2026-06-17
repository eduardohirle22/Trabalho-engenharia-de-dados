"""
pipeline_simples.py — UrbanFlow
Dependência única: pip install duckdb
Uso:              python pipeline_simples.py
"""
import json, random, hashlib
from pathlib import Path
from datetime import datetime, timezone
import duckdb

# ── Estrutura de pastas (Data Lake local) ─────────────────────────
BASE = Path("data")
for p in ["bronze/gps","bronze/catracas","bronze/bikes","silver","gold","quarentena"]:
    (BASE / p).mkdir(parents=True, exist_ok=True)

SEP = "═" * 55


# ── Exibe resultado de query sem precisar de pandas ───────────────
def show(cur, titulo=""):
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    if titulo:
        print(f"\n  {titulo}")
    ws = [max(len(c), max((len(str(r[i])) for r in rows), default=4))
          for i, c in enumerate(cols)]
    fmt = "  " + "  ".join(f"{{:<{w}}}" for w in ws)
    print(fmt.format(*cols))
    print("  " + "  ".join("─" * w for w in ws))
    for row in rows[:15]:
        print(fmt.format(*[str(v) for v in row]))


# ══════════════════════════════════════════════════════════════════
# ETAPA 1 — INGESTÃO
# Gera JSON imutável no Bronze (em produção: ingestao/simulador_*.py)
# ══════════════════════════════════════════════════════════════════
def ingestao():
    ts  = datetime.now(timezone.utc).isoformat()
    ST  = ["on_route","at_stop","delayed","out_of_service"]
    WGT = [.75,.15,.08,.02]

    gps = [{"vehicle_id": f"BUS-{i:04d}",
             "line_id":    f"L{(i%20)+1:03d}",
             "lat":   round(random.uniform(-15.95,-15.55), 6),
             "lon":   round(random.uniform(-48.30,-47.75), 6),
             "speed_kmh":     round(random.uniform(100,140),1) if random.random()<.005
                              else max(0, min(round(random.gauss(35,12),1), 90)),
             "occupancy_pct": random.randint(0, 100),
             "status":        random.choices(ST, WGT)[0],
             "timestamp":     ts}
            for i in range(1, 301)]

    catracas = [{"event_id":   f"EVT-{random.randint(100000,999999)}",
                  "station_id":  f"EST-{random.randint(1,18):02d}",
                  "direction":   random.choice(["ENTRY","EXIT"]),
                  "card_hash":   hashlib.sha256(    # LGPD: card_id nunca persiste
                      f"CARD-{random.randint(1,9999)}salt2026"
                      .encode()).hexdigest()[:16],
                  "fare_paid":   random.choice([0.0, 2.5, 5.0]),
                  "timestamp":   ts}
                 for _ in range(150)]

    bikes = [{"station_id":       f"BIKE-{i:02d}",
               "bikes_available":  random.randint(0, 15),
               "docks_available":  random.randint(0, 10),
               "timestamp":        ts}
              for i in range(1, 31)]

    (BASE/"bronze/gps/data.json"     ).write_text(json.dumps(gps))
    (BASE/"bronze/catracas/data.json").write_text(json.dumps(catracas))
    (BASE/"bronze/bikes/data.json"   ).write_text(json.dumps(bikes))
    print(f"  300 GPS  +  150 catracas  +  30 bikes  →  bronze/")


# ══════════════════════════════════════════════════════════════════
# ETAPAS 2+3 — ARMAZENAMENTO + TRANSFORMAÇÃO  Bronze → Silver
# ETL 100 % SQL no DuckDB: lê JSON, valida, grava Parquet Snappy
# Sem pandas, sem pyarrow — DuckDB faz tudo nativamente
# ══════════════════════════════════════════════════════════════════
def bronze_to_silver():
    gps_in  = str(BASE/"bronze/gps/data.json")
    cat_in  = str(BASE/"bronze/catracas/data.json")
    bike_in = str(BASE/"bronze/bikes/data.json")
    gps_out  = str(BASE/"silver/gps_clean.parquet")
    cat_out  = str(BASE/"silver/catracas_clean.parquet")
    bike_out = str(BASE/"silver/bikes_clean.parquet")
    quar_out = str(BASE/"quarentena/gps_outliers.parquet")

    db = duckdb.connect()

    # GPS — aliases explícitos para evitar nomes de coluna com cast
    db.execute(f"""
        COPY (
            SELECT DISTINCT
                vehicle_id   AS vehicle_id,
                line_id      AS line_id,
                lat::DOUBLE  AS lat,
                lon::DOUBLE  AS lon,
                speed_kmh::DOUBLE    AS speed_kmh,
                occupancy_pct::INT   AS occupancy_pct,
                status::VARCHAR      AS status,
                timestamp::TIMESTAMPTZ AS ts,
                (speed_kmh::DOUBLE > 120) AS is_outlier,
                NOW()            AS _processed_at,
                'bronze/gps'     AS _source
            FROM read_json_auto('{gps_in}')
            WHERE vehicle_id IS NOT NULL
              AND lat::DOUBLE BETWEEN -90 AND 90
              AND lon::DOUBLE BETWEEN -180 AND 180
        ) TO '{gps_out}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)

    # Quarentena — outliers de velocidade com motivo registrado
    db.execute(f"""
        COPY (
            SELECT *, 'speed_kmh > 120' AS _motivo_rejeicao
            FROM read_json_auto('{gps_in}')
            WHERE speed_kmh::DOUBLE > 120
        ) TO '{quar_out}' (FORMAT PARQUET)
    """)

    # Catracas
    db.execute(f"""
        COPY (
            SELECT DISTINCT
                event_id::VARCHAR    AS event_id,
                station_id::VARCHAR  AS station_id,
                direction::VARCHAR   AS direction,
                card_hash::VARCHAR   AS card_hash,
                fare_paid::DOUBLE    AS fare_paid,
                timestamp::TIMESTAMPTZ AS ts,
                NOW()                AS _processed_at,
                'bronze/catracas'    AS _source
            FROM read_json_auto('{cat_in}')
            WHERE event_id IS NOT NULL
              AND direction IN ('ENTRY','EXIT')
              AND fare_paid::DOUBLE >= 0
        ) TO '{cat_out}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)

    # Bikes
    db.execute(f"""
        COPY (
            SELECT DISTINCT
                station_id::VARCHAR     AS station_id,
                bikes_available::INT    AS bikes_available,
                docks_available::INT    AS docks_available,
                timestamp::TIMESTAMPTZ  AS ts,
                NOW()                   AS _processed_at
            FROM read_json_auto('{bike_in}')
            WHERE station_id IS NOT NULL
        ) TO '{bike_out}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)

    n_gps = db.execute(f"SELECT COUNT(*) FROM '{gps_out}' ").fetchone()[0]
    n_cat = db.execute(f"SELECT COUNT(*) FROM '{cat_out}' ").fetchone()[0]
    n_bk  = db.execute(f"SELECT COUNT(*) FROM '{bike_out}'").fetchone()[0]
    print(f"  {n_gps} GPS  +  {n_cat} catracas  +  {n_bk} bikes  →  Parquet Snappy")
    db.close()


# ══════════════════════════════════════════════════════════════════
# ETAPA 3 (cont.) — TRANSFORMAÇÃO  Silver → Gold
# Agrega KPIs prontos para consumo
# Equivalente aos modelos dbt em dbt_project/models/
# ══════════════════════════════════════════════════════════════════
def silver_to_gold():
    gps  = str(BASE/"silver/gps_clean.parquet")
    cat  = str(BASE/"silver/catracas_clean.parquet")
    path = str(BASE/"gold/urbanflow.duckdb")
    conn = duckdb.connect(path)

    conn.execute(f"""
        CREATE OR REPLACE TABLE kpi_operacional AS
        SELECT
            line_id,
            COUNT(DISTINCT vehicle_id)                      AS veiculos_ativos,
            ROUND(AVG(speed_kmh), 1)                        AS velocidade_media_kmh,
            ROUND(AVG(occupancy_pct), 1)                    AS ocupacao_media_pct,
            SUM(is_outlier::INT)                            AS outliers_detectados,
            ROUND(100.0 * SUM(CASE WHEN status='on_route' THEN 1 ELSE 0 END)
                  / NULLIF(COUNT(*) - SUM(is_outlier::INT), 0), 1) AS otp_pct,
            CASE
              WHEN ROUND(100.0*SUM(CASE WHEN status='on_route' THEN 1 ELSE 0 END)
                   /NULLIF(COUNT(*)-SUM(is_outlier::INT),0),1) >= 90 THEN 'VERDE'
              WHEN ROUND(100.0*SUM(CASE WHEN status='on_route' THEN 1 ELSE 0 END)
                   /NULLIF(COUNT(*)-SUM(is_outlier::INT),0),1) >= 70 THEN 'AMARELO'
              ELSE 'VERMELHO'
            END AS semaforo_otp
        FROM read_parquet('{gps}')
        GROUP BY line_id
        ORDER BY veiculos_ativos DESC
    """)

    conn.execute(f"""
        CREATE OR REPLACE TABLE agg_demanda AS
        SELECT
            station_id,
            CASE
              WHEN EXTRACT(hour FROM ts) BETWEEN 6  AND 9  THEN 'manha_pico'
              WHEN EXTRACT(hour FROM ts) BETWEEN 17 AND 20 THEN 'tarde_pico'
              WHEN EXTRACT(hour FROM ts) BETWEEN 10 AND 16 THEN 'entrepico'
              ELSE 'noturno'
            END                                                    AS periodo,
            SUM(CASE WHEN direction='ENTRY' THEN 1 ELSE 0 END)    AS entradas,
            SUM(CASE WHEN direction='EXIT'  THEN 1 ELSE 0 END)    AS saidas,
            ROUND(SUM(fare_paid), 2)                               AS receita_brl
        FROM read_parquet('{cat}')
        GROUP BY station_id, periodo
        ORDER BY entradas DESC
    """)

    conn.close()
    print(f"  kpi_operacional  +  agg_demanda  →  gold/urbanflow.duckdb")
    return path


# ══════════════════════════════════════════════════════════════════
# ETAPA 5 — CONSUMO / SERVING
# Consultas DuckDB no Gold — mesmas queries da API serving/main.py
# ══════════════════════════════════════════════════════════════════
def serving(path):
    conn = duckdb.connect(path, read_only=True)

    show(conn.execute("""
        SELECT line_id, veiculos_ativos, velocidade_media_kmh,
               ocupacao_media_pct, otp_pct, semaforo_otp
        FROM kpi_operacional LIMIT 10
    """), "KPI Operacional — top 10 linhas:")

    show(conn.execute("""
        SELECT station_id, periodo, entradas, saidas, receita_brl
        FROM agg_demanda LIMIT 10
    """), "Demanda VLT — top 10 estações:")

    show(conn.execute("""
        SELECT
          (SELECT COUNT(DISTINCT line_id)  FROM kpi_operacional) AS linhas_ativas,
          (SELECT SUM(veiculos_ativos)     FROM kpi_operacional) AS total_veiculos,
          (SELECT SUM(entradas)            FROM agg_demanda)     AS total_passageiros,
          (SELECT ROUND(SUM(receita_brl),2)FROM agg_demanda)     AS receita_total_brl
    """), "Resumo executivo:")

    conn.close()
    gerar_dashboard(path)


# ══════════════════════════════════════════════════════════════════
# DASHBOARD HTML — gerado automaticamente com dados frescos do Gold
# Abre em qualquer navegador, sem servidor, sem instalação extra
# Hospedável no GitHub Pages como página estática
# ══════════════════════════════════════════════════════════════════
def gerar_dashboard(path):
    conn = duckdb.connect(path, read_only=True)

    kpi_rows = conn.execute("""
        SELECT line_id, velocidade_media_kmh, ocupacao_media_pct, otp_pct, semaforo_otp
        FROM kpi_operacional ORDER BY otp_pct DESC
    """).fetchall()

    dem_rows = conn.execute("""
        SELECT station_id,
               SUM(entradas)    AS e,
               SUM(saidas)      AS s,
               SUM(receita_brl) AS r
        FROM agg_demanda GROUP BY station_id ORDER BY e DESC
    """).fetchall()

    resumo = conn.execute("""
        SELECT COUNT(DISTINCT line_id)            AS linhas,
               SUM(veiculos_ativos)               AS veiculos,
               ROUND(AVG(velocidade_media_kmh),1) AS vel,
               ROUND(AVG(ocupacao_media_pct),1)   AS occ
        FROM kpi_operacional
    """).fetchone()

    sem_dist = {r[0]: r[1] for r in conn.execute(
        "SELECT semaforo_otp, COUNT(*) FROM kpi_operacional GROUP BY semaforo_otp"
    ).fetchall()}
    conn.close()

    verde   = sem_dist.get("VERDE",    0)
    amarelo = sem_dist.get("AMARELO",  0)
    verm    = sem_dist.get("VERMELHO", 0)
    total   = verde + amarelo + verm or 1

    kpi_js = str([{"l":r[0],"spd":float(r[1]),"occ":float(r[2]),"otp":float(r[3]),"sem":r[4]}
                  for r in kpi_rows])
    dem_js = str([{"s":r[0],"e":int(r[1]),"x":int(r[2]),"r":float(r[3])}
                  for r in dem_rows])
    ts = datetime.now().strftime("%d/%m/%Y %H:%M")

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>UrbanFlow &mdash; Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f1117;color:#e2e8f0;padding:24px;min-height:100vh}}
h1{{font-size:20px;font-weight:600;margin-bottom:4px}}
.sub{{font-size:13px;color:#718096;margin-bottom:24px}}
.cards{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px}}
.card{{background:#1a1f2e;border:1px solid #2d3748;border-radius:10px;padding:16px 20px}}
.cl{{font-size:12px;color:#718096;margin-bottom:6px;text-transform:uppercase;letter-spacing:.5px}}
.cv{{font-size:28px;font-weight:700;color:#f7fafc}}
.cu{{font-size:12px;color:#4a5568;margin-top:2px}}
.g2{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}}
.pn{{background:#1a1f2e;border:1px solid #2d3748;border-radius:10px;padding:20px}}
.pt{{font-size:13px;font-weight:600;color:#a0aec0;margin-bottom:16px;text-transform:uppercase;letter-spacing:.5px}}
.leg{{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:12px;font-size:12px;color:#718096}}
.dot{{width:10px;height:10px;border-radius:2px;display:inline-block;margin-right:5px;vertical-align:middle}}
.sr{{display:flex;align-items:center;gap:10px;margin-bottom:12px}}
.sb{{flex:1;background:#2d3748;border-radius:4px;height:8px;overflow:hidden}}
.sbi{{height:100%;border-radius:4px}}
.sl{{font-size:13px;color:#a0aec0;width:90px}}
.sc{{font-size:14px;font-weight:600;width:70px;text-align:right}}
canvas{{max-height:260px}}
</style>
</head>
<body>
<h1>&#x1F68C; UrbanFlow &mdash; Mobilidade S.A.</h1>
<p class="sub">Dashboard operacional &middot; Gerado em {ts} por pipeline_simples.py</p>
<div class="cards">
  <div class="card"><div class="cl">Linhas ativas</div><div class="cv">{resumo[0]}</div><div class="cu">rotas monitoradas</div></div>
  <div class="card"><div class="cl">Veículos</div><div class="cv">{resumo[1]}</div><div class="cu">ônibus rastreados</div></div>
  <div class="card"><div class="cl">Velocidade média</div><div class="cv">{resumo[2]}</div><div class="cu">km/h na frota</div></div>
  <div class="card"><div class="cl">Ocupação média</div><div class="cv">{resumo[3]}%</div><div class="cu">capacidade utilizada</div></div>
</div>
<div class="g2">
  <div class="pn"><div class="pt">Semáforo OTP por linha</div>
    <div class="leg">
      <span><span class="dot" style="background:#22c55e"></span>Verde ≥90%</span>
      <span><span class="dot" style="background:#eab308"></span>Amarelo ≥70%</span>
      <span><span class="dot" style="background:#ef4444"></span>Vermelho &lt;70%</span>
    </div>
    <canvas id="cOTP"></canvas>
  </div>
  <div class="pn"><div class="pt">Demanda VLT &mdash; Entradas / Saídas</div>
    <div class="leg">
      <span><span class="dot" style="background:#3b82f6"></span>Entradas</span>
      <span><span class="dot" style="background:#10b981"></span>Saídas</span>
    </div>
    <canvas id="cDem"></canvas>
  </div>
</div>
<div class="g2">
  <div class="pn"><div class="pt">Distribuição do semáforo OTP</div>
    <div style="margin-top:8px">
      <div class="sr"><div class="sl">Verde</div><div class="sb"><div class="sbi" style="width:{verde*100//total}%;background:#22c55e"></div></div><div class="sc" style="color:#22c55e">{verde} linhas</div></div>
      <div class="sr"><div class="sl">Amarelo</div><div class="sb"><div class="sbi" style="width:{amarelo*100//total}%;background:#eab308"></div></div><div class="sc" style="color:#eab308">{amarelo} linhas</div></div>
      <div class="sr"><div class="sl">Vermelho</div><div class="sb"><div class="sbi" style="width:{verm*100//total}%;background:#ef4444"></div></div><div class="sc" style="color:#ef4444">{verm} linhas</div></div>
    </div>
  </div>
  <div class="pn"><div class="pt">Receita por estação VLT (R$)</div>
    <canvas id="cRec"></canvas>
  </div>
</div>
<script>
const kpi={kpi_js};
const dem={dem_js};
const C=s=>s==='VERDE'?'#22c55e':s==='AMARELO'?'#eab308':'#ef4444';
const mk=(id,type,labels,datasets,yFmt)=>new Chart(document.getElementById(id),{{type,data:{{labels,datasets}},options:{{responsive:true,maintainAspectRatio:true,plugins:{{legend:{{display:false}}}},scales:{{x:{{ticks:{{color:'#718096',font:{{size:10}},maxRotation:45}}}},y:{{ticks:{{callback:yFmt,color:'#718096'}},grid:{{color:'rgba(255,255,255,0.05)'}}}}}}}}}}
}});
mk('cOTP','bar',kpi.map(r=>r.l),[{{label:'OTP%',data:kpi.map(r=>r.otp),backgroundColor:kpi.map(r=>C(r.sem)),borderRadius:3,borderSkipped:false}}],v=>v+'%');
mk('cDem','bar',dem.map(r=>r.s),[{{label:'Entradas',data:dem.map(r=>r.e),backgroundColor:'#3b82f6',borderRadius:3,borderSkipped:false}},{{label:'Saídas',data:dem.map(r=>r.x),backgroundColor:'#10b981',borderRadius:3,borderSkipped:false}}],v=>v);
mk('cRec','bar',dem.map(r=>r.s),[{{label:'R$',data:dem.map(r=>r.r),backgroundColor:'#8b5cf6',borderRadius:3,borderSkipped:false}}],v=>'R$'+v);
</script>
</body>
</html>"""

    out = Path("dashboard.html")
    out.write_text(html, encoding="utf-8")
    print(f"  dashboard.html gerado  &rarr; abra no navegador ou hospede no GitHub Pages")


# ══════════════════════════════════════════════════════════════════
# ETAPA 4 — ORQUESTRAÇÃO
# Execução encadeada com controle de dependências.
# Em produção: orchestration/dags/urbanflow_pipeline.py (Airflow)
# ══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    t0 = datetime.now()
    print(f"\n{SEP}\n  UrbanFlow — Pipeline\n{SEP}")

    print("\n[ 1 ] INGESTÃO  →  Bronze")
    ingestao()

    print("\n[ 2+3 ] TRANSFORMAÇÃO  Bronze → Silver  (DuckDB ETL)")
    bronze_to_silver()

    print("\n[ 3 ]   TRANSFORMAÇÃO  Silver → Gold    (DuckDB SQL)")
    db = silver_to_gold()

    print("\n[ 5 ]   CONSUMO / SERVING")
    serving(db)

    dt = (datetime.now() - t0).total_seconds()
    print(f"\n{SEP}")
    print(f"  ✅  Concluído em {dt:.2f}s  ·  dependência: apenas duckdb")
    print(f"  Stack completa:  docker/docker-compose.yml")
    print(f"  Airflow DAG:     orchestration/dags/urbanflow_pipeline.py")
    print(f"  API REST:        serving/main.py  (GET /kpis/operacional)")
    print(SEP)
