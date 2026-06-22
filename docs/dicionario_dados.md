# Dicionario de Dados — UrbanFlow

> **Versao:** 2.0  |  **Responsavel:** Equipe UrbanFlow  |  **Ultima atualizacao:** 2026-06

Este documento descreve todos os datasets produzidos pelo pipeline UrbanFlow,
seus campos, tipos, restricoes e exemplos de valores.

---

## Sumario

1. [Camada Bronze — Dados Brutos](#camada-bronze)
   - GPS Onibus
   - Eventos de Catracas
   - Status de Bikes
   - Viagens Postgres
   - Bilhetagem Postgres
2. [Camada Silver — Dados Limpos](#camada-silver)
   - gps_onibus_clean
   - catracas_clean
   - bikes_clean
3. [Camada Gold — Dados Analiticos](#camada-gold)
   - gps_onibus_gold
   - catracas_gold
   - bikes_gold
   - viagens_gold

---

## Camada Bronze

**Formato:** JSON  |  **Storage:** MinIO `urbanflow-bronze`  |  **Retencao:** 7 dias

### 1. GPS Onibus

**Prefixo S3:** `gps_onibus/ano=YYYY/mes=MM/dia=DD/HH/`  
**Producao:** Simulador `simulador_gps.py` — a cada hora  
**Volume:** ~300 registros/ciclo

| Campo | Tipo | Nulavel | Descricao | Exemplo |
|-------|------|---------|-----------|---------|
| `vehicle_id` | string | NAO | Identificador unico do veiculo | `"BUS-0042"` |
| `line_id` | string | NAO | Codigo da linha de onibus | `"L042"` |
| `lat` | float | NAO | Latitude WGS84 | `-23.5505` |
| `lon` | float | NAO | Longitude WGS84 | `-46.6333` |
| `speed_kmh` | float | NAO | Velocidade instantanea em km/h | `42.5` |
| `timestamp` | string ISO8601 | NAO | Data/hora UTC do registro | `"2026-06-01T14:30:00Z"` |
| `heading` | float | SIM | Direcao em graus (0-360) | `180.0` |
| `occupancy` | string | SIM | Ocupacao do veiculo | `"MEDIUM"` |

**Regras de validacao:**
- `lat` deve estar entre -90 e 90
- `lon` deve estar entre -180 e 180
- `speed_kmh` deve ser >= 0 e <= 120 (outlier se > 120)

---

### 2. Eventos de Catracas

**Prefixo S3:** `catracas/ano=YYYY/mes=MM/dia=DD/HH/`  
**Producao:** Simulador `simulador_catracas.py` — a cada hora  
**Volume:** ~200 eventos/ciclo

| Campo | Tipo | Nulavel | Descricao | Exemplo |
|-------|------|---------|-----------|---------|
| `event_id` | string UUID | NAO | Identificador unico do evento | `"evt-abc123"` |
| `station_id` | string | NAO | Codigo da estacao de metro/onibus | `"EST-001"` |
| `direction` | enum | NAO | Sentido da passagem | `"ENTRY"` ou `"EXIT"` |
| `card_hash` | string | NAO | Hash anonimizado do cartao | `"a1b2c3d4..."` |
| `timestamp` | string ISO8601 | NAO | Data/hora UTC do evento | `"2026-06-01T14:31:00Z"` |
| `fare_paid` | float | SIM | Valor pago em BRL | `4.40` |
| `card_type` | string | SIM | Tipo do cartao de transporte | `"COMUM"` |

**Regras de validacao:**
- `direction` deve ser exatamente `"ENTRY"` ou `"EXIT"`
- `fare_paid` deve ser >= 0 quando presente
- `card_hash` e irreversivel (privacidade por design)

---

### 3. Status de Bikes

**Prefixo S3:** `bikes/ano=YYYY/mes=MM/dia=DD/HH/`  
**Producao:** Simulador `simulador_bikes.py` — a cada hora  
**Volume:** ~50 estacoes/ciclo

| Campo | Tipo | Nulavel | Descricao | Exemplo |
|-------|------|---------|-----------|---------|
| `station_id` | string | NAO | Identificador da estacao de bikes | `"BIKE-001"` |
| `bikes_available` | integer | NAO | Quantidade de bikes disponiveis | `12` |
| `docks_available` | integer | NAO | Quantidade de vagas disponiveis | `8` |
| `last_reported` | string ISO8601 | NAO | Ultima atualizacao do status | `"2026-06-01T14:00:00Z"` |
| `is_renting` | boolean | SIM | Estacao aceitando retiradas | `true` |
| `is_returning` | boolean | SIM | Estacao aceitando devolucoes | `true` |

---

### 4. Viagens Postgres

**Prefixo S3:** `viagens_postgres/ano=YYYY/mes=MM/dia=DD/`  
**Producao:** Batch `batch_postgres` — diario  
**Origem:** Tabela `viagens` no banco `urbanflow_legado`

| Campo | Tipo | Nulavel | Descricao | Exemplo |
|-------|------|---------|-----------|---------|
| `viagem_id` | string | NAO | Identificador unico da viagem | `"VGM-20260601-001"` |
| `vehicle_id` | string | NAO | Veiculo que realizou a viagem | `"BUS-0042"` |
| `line_id` | string | NAO | Linha percorrida | `"L042"` |
| `data_viagem` | date | NAO | Data da viagem | `"2026-06-01"` |
| `hora_inicio` | time | NAO | Hora de saida do terminal | `"06:30:00"` |
| `hora_fim` | time | NAO | Hora de chegada ao terminal final | `"07:45:00"` |
| `passageiros` | integer | NAO | Total de passageiros transportados | `85` |
| `receita_brl` | float | NAO | Receita total em BRL | `374.00` |
| `km_percorridos` | float | NAO | Quilometros percorridos | `22.5` |
| `no_horario` | boolean | NAO | Viagem dentro do horario previsto | `true` |

---

## Camada Silver

**Formato:** Parquet (Snappy)  |  **Storage:** Local `/opt/airflow/data/silver/` + MinIO `urbanflow-silver`  
**Retencao:** 30 dias  |  **Producao:** Task `bronze_to_silver`

### 1. gps_onibus_clean

**Caminho:** `gps_onibus_clean/data=YYYY-MM-DD/part-00000.snappy.parquet`

Todos os campos do Bronze mais:

| Campo adicional | Tipo | Descricao |
|----------------|------|-----------|
| `is_outlier` | boolean | `true` se `speed_kmh > 120` (nao removido, sinalizado) |
| `tipo` | string | Tipo do veiculo (do DIM_VEICULOS) |
| `capacidade` | integer | Capacidade maxima de passageiros |
| `_processed_at` | timestamp | Data/hora de processamento pelo Silver |
| `_source` | string | Origem do dado: `"bronze/gps_onibus"` |
| `_pipeline_ver` | string | Versao do pipeline: `"2.0"` |

**Transformacoes aplicadas:**
- Campos obrigatorios nulos → quarentena (`urbanflow-quarentena`)
- Deduplicacao por `(vehicle_id, timestamp)`
- Timestamps normalizados para UTC
- Coordenadas invalidas removidas
- Join com `DIM_VEICULOS` para enriquecer com tipo e capacidade

---

### 2. catracas_clean

**Caminho:** `catracas_clean/data=YYYY-MM-DD/part-00000.snappy.parquet`

| Campo adicional | Tipo | Descricao |
|----------------|------|-----------|
| `_processed_at` | timestamp | Data/hora de processamento |
| `_source` | string | `"bronze/catracas"` |
| `_pipeline_ver` | string | `"2.0"` |

**Transformacoes:**
- Deduplicacao por `event_id`
- Filtro: apenas `direction IN ("ENTRY", "EXIT")`
- Filtro: `fare_paid >= 0`
- Timestamps UTC

---

### 3. bikes_clean

**Caminho:** `bikes_clean/data=YYYY-MM-DD/part-00000.snappy.parquet`

| Campo adicional | Tipo | Descricao |
|----------------|------|-----------|
| `_processed_at` | timestamp | Data/hora de processamento |
| `_source` | string | `"bronze/bikes"` |

**Transformacoes:**
- Deduplicacao por `(station_id, last_reported)`
- Remocao de nulos em campos obrigatorios
- Timestamps UTC

---

## Camada Gold

**Formato:** DuckDB  |  **Storage:** `/opt/airflow/data/gold/urbanflow.duckdb`  
**Retencao:** 90 dias  |  **Producao:** Tasks `dbt_run` + `dbt_test`

Os modelos dbt leem os Parquets Silver e materializam tabelas analiticamente agregadas.

### 1. gps_onibus_gold

Metricas de operacao por veiculo e linha.

| Campo | Tipo | Descricao |
|-------|------|-----------|
| `vehicle_id` | string | Identificador do veiculo |
| `line_id` | string | Linha de onibus |
| `data` | date | Data de referencia |
| `total_registros` | integer | Total de registros GPS no periodo |
| `velocidade_media` | float | Velocidade media em km/h |
| `pct_outliers` | float | Percentual de registros com velocidade anomala |
| `capacidade` | integer | Capacidade maxima do veiculo |

### 2. catracas_gold

Fluxo agregado por estacao.

| Campo | Tipo | Descricao |
|-------|------|-----------|
| `station_id` | string | Identificador da estacao |
| `data` | date | Data de referencia |
| `total_entradas` | integer | Total de passagens ENTRY |
| `total_saidas` | integer | Total de passagens EXIT |
| `receita_total` | float | Receita total em BRL |
| `hora_pico` | integer | Hora com maior fluxo (0-23) |

### 3. bikes_gold

Disponibilidade media por estacao.

| Campo | Tipo | Descricao |
|-------|------|-----------|
| `station_id` | string | Identificador da estacao |
| `data` | date | Data de referencia |
| `disponibilidade_media` | float | Percentual medio de bikes disponiveis |
| `horas_vazia` | integer | Horas com 0 bikes disponiveis |

---

## Metadados de Linhagem (Data Lineage)

```
simulador_gps.py ─────────────────────────────► MinIO Bronze: gps_onibus/
simulador_catracas.py ────────────────────────► MinIO Bronze: catracas/
simulador_bikes.py ───────────────────────────► MinIO Bronze: bikes/
PostgreSQL: viagens + bilhetagem ─────────────► MinIO Bronze: viagens_postgres/
                                                                    │
                                               bronze_to_silver ───┘
                                                        │
                                               Silver: Parquet local + MinIO
                                                        │
                                               dbt run ─┘
                                                        │
                                               Gold: DuckDB (tabelas analiticas)
```

## Glossario

| Termo | Definicao |
|-------|-----------|
| **Bronze** | Dados brutos, sem transformacao, exatamente como chegaram da fonte |
| **Silver** | Dados limpos, validados, deduplicados e enriquecidos |
| **Gold** | Dados agregados e prontos para consumo analitico |
| **Quarentena** | Registros rejeitados por violacao de qualidade, armazenados para auditoria |
| **Outlier** | Registro com valor fora do intervalo esperado (sinalizado, nao removido) |
| **Freshness** | Tempo desde a ultima atualizacao de um dataset |
| **Lineage** | Rastreabilidade do dado desde a fonte ate o consumo |
