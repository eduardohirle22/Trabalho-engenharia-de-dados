# POC — Prova de Conceito UrbanFlow

Valida a stack tecnológica do planejamento antes da implementação completa (Parte 2).

## Resultados

| Etapa | Ferramenta | Status | Evidência |
|---|---|---|---|
| Simuladores → Bronze | Python + boto3 | ✅ 900 eventos GPS + 300 catracas | `evidencias/01_bronze_gerado.txt` |
| Bronze → Silver | Python + pandas | ✅ 300 + 150 registros Parquet | `evidencias/02_silver_gerado.txt` |
| Silver → Gold | dbt Core + DuckDB | ✅ PASS=5 modelos, PASS=26 testes | `evidencias/04_duckdb_queries.txt` |
| Queries Gold | DuckDB ad-hoc | ✅ KPI + Demanda materializados | `evidencias/04_duckdb_queries.txt` |

## Rodar a POC (sem Docker)

```bash
# 1. Instalar dependências
pip install -r ../requirements.txt

# 2. Executar pipeline completo
cd poc/
python poc_demo.py
```

Saída esperada:
```
dbt run:   PASS=5  WARN=0  ERROR=0
dbt test:  PASS=26 WARN=0  ERROR=0

Gold materializado:
  kpi_operacional_diario : 120 linhas
  agg_demanda_por_hora   :  18 linhas
```

## Rodar por etapa

```bash
# Etapa 1 — Simuladores
python simuladores/simulador_gps.py      --local --ciclos 3
python simuladores/simulador_catracas.py --local --ciclos 2

# Etapa 2 — Bronze → Silver
python bronze_to_silver.py

# Etapa 3 — dbt Silver → Gold
cd dbt_project/
dbt run  --profiles-dir .
dbt test --profiles-dir .

# Etapa 4 — Query ad-hoc Gold
python3 -c "
import duckdb
con = duckdb.connect('../gold/urbanflow.duckdb')
print(con.execute('SELECT * FROM kpi_operacional_diario LIMIT 5').fetchdf().to_string())
"
```

## Rodar com Docker (ambiente completo)

```bash
# Copia e configura variáveis
cp ../.env.example ../.env

# Sobe MinIO + PostgreSQL + Airflow + Superset
docker compose up -d

# Aguarda inicialização (~2 minutos) e acessa:
# MinIO:    http://localhost:9001  (minioadmin / minioadmin)
# Airflow:  http://localhost:8080  (admin / admin)
# Superset: http://localhost:8088  (admin / admin)

# Publica dados no MinIO real
python simuladores/simulador_gps.py --minio --ciclos 5
```

## Estrutura

```
poc/
├── poc_demo.py                  ← Executa pipeline ponta a ponta
├── bronze_to_silver.py          ← Pipeline pandas: Bronze → Silver
├── docker-compose.yml           ← Ambiente Docker completo (Parte 2)
│
├── simuladores/
│   ├── simulador_gps.py         ← GPS (850 veículos, ~30s/ciclo)
│   └── simulador_catracas.py    ← Catracas metrô (18 estações)
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml             ← DuckDB local ou MinIO
│   └── models/
│       ├── staging/             ← Lê Parquet Silver, padroniza tipos
│       ├── intermediate/        ← Agregações intermediárias
│       ├── marts/               ← Tabelas Gold finais (materializadas)
│       └── schema.yml           ← 26 testes de qualidade
│
├── scripts/
│   └── init_postgres.sql        ← Cria e popula banco legado
│
└── evidencias/                  ← Logs de execução com números reais
    ├── 01_bronze_gerado.txt
    ├── 02_silver_gerado.txt
    └── 04_duckdb_queries.txt
```

## Nota sobre streaming

Os simuladores geram **ingestão de eventos periódica** — publicam JSON particionado por data/hora no Bronze a cada ciclo. Em produção com dispositivos reais, essa camada seria substituída por Apache Kafka + Kafka Connect S3 Sink, sem alterar nenhuma linha dos pipelines de transformação downstream.