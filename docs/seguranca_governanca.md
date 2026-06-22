# Seguranca e Governanca de Dados — UrbanFlow

> **Versao:** 1.0  |  **Responsavel:** Equipe UrbanFlow  |  **Ultima atualizacao:** 2026-06

---

## Modelo de Seguranca

O pipeline UrbanFlow opera em ambiente Docker isolado (rede interna airflow_default).
Nenhum servico expoe porta publica exceto Airflow UI (8080) e MinIO Console (9001), ambos acessiveis apenas em localhost.

---

## Criptografia

### Airflow — Fernet Key (CRIPTOGRAFADO)

O Airflow usa criptografia simetrica **Fernet (AES-128-CBC)** para proteger credentials e variaveis sensiveis armazenadas no banco de metadados.

- **O que e criptografado:** senhas de conexao, tokens, variaveis via Variable.set()
- **Como funciona:** AIRFLOW__CORE__FERNET_KEY no .env — chave de 32 bytes em base64

### PostgreSQL — Em repouso (NAO CRIPTOGRAFADO)

**Justificativa:** dados operacionais de mobilidade publica sem PII; container isolado; sem porta externa exposta.
**Mitigacao:** acesso restrito a rede Docker, autenticacao obrigatoria via .env.
Para producao: volumes com dm-crypt/LUKS no host.

### MinIO — Em repouso (NAO CRIPTOGRAFADO)

**Justificativa:** dados anonimizados sem PII; SSE-S3 requer KMS externo; ambiente academico.
**Para producao:** mc admin config set myminio/ kes com HashiCorp Vault ou AWS KMS.

### Dados em Transito

| Conexao | Criptografia |
|---|---|
| Airflow <-> PostgreSQL | Nao (rede Docker interna) |
| Airflow <-> MinIO | Nao (rede Docker interna) |
| Usuario <-> Airflow UI | Nao (localhost apenas) |

Para producao com acesso remoto: nginx reverse proxy com TLS/HTTPS.

---

## Autenticacao e Controle de Acesso

**Airflow:** autenticacao via usuario/senha; roles: Admin, User, Viewer, Op, Public.
**MinIO:** Access Key / Secret Key; buckets privados por padrao.
**PostgreSQL:** usuario separado para urbanflow_legado com permissoes somente-leitura (menor privilegio).
**Variaveis sensiveis:** exclusivamente via .env (no .gitignore, nunca commitado).

---

## Governanca de Dados

### Ownership

| Camada | Dataset | Responsavel Tecnico | Responsavel de Negocio |
|---|---|---|---|
| Bronze | gps_onibus_raw | Engenharia de Dados | Operacoes |
| Bronze | catracas_raw | Engenharia de Dados | Operacoes |
| Bronze | bikes_raw | Engenharia de Dados | Operacoes |
| Bronze | viagens_postgres | Engenharia de Dados | Financeiro |
| Silver | *_clean | Engenharia de Dados | Qualidade |
| Gold | *_gold | Engenharia de Dados | Analitica / BI |

### Classificacao dos Dados

| Nivel | Descricao | Datasets UrbanFlow |
|---|---|---|
| Publico | Dados operacionais sem PII | GPS, catracas, bikes, viagens |
| Interno | Configuracoes, credenciais | .env, Fernet key |
| Confidencial | PII, dados financeiros | N/A neste pipeline |

### Politica de Qualidade

Implementada em urbanflow_pipeline.py via CONFIG_QUALIDADE:
- **Completude:** campos criticos nao podem ser nulos
- **Volume:** minimo de registros por execucao (GPS: 50, catracas: 20, bikes: 5)
- **Validade:** ranges fisicos (lat -90/90, lon -180/180, speed >= 0)
- **Consistencia:** enumeracoes validas (direction: ENTRY ou EXIT)

Falhas de qualidade geram WARNING mas nao interrompem o pipeline.

---

## Retencao e Ciclo de Vida

| Camada | Storage | Retencao | Politica de Expurgo |
|---|---|---|---|
| Bronze | MinIO bronze/ | 7 dias | mc rm --older-than 7d |
| Silver | MinIO silver/ + local | 30 dias | mc rm --older-than 30d |
| Gold | DuckDB local | 90 dias | DELETE WHERE _loaded_at < now()-90d |
| Logs Airflow | PostgreSQL | 30 dias | airflow db clean |

Dados particionados por data=YYYY-MM-DD para expurgo cirurgico.

---

## Lineage e Rastreabilidade

### Fluxo de Dados

```
Fontes --> Bronze --> Silver --> Gold

SimuladorGPS    --> gps_onibus_raw    --> gps_onibus_clean  --> gps_onibus_gold
SimuladorCatrac --> catracas_raw      --> catracas_clean    --> catracas_gold
SimuladorBikes  --> bikes_raw         --> bikes_clean       --> bikes_gold
PostgresLegado  --> viagens_pg        --> (dbt)             --> viagens_gold
```

### Campos de Rastreabilidade (Silver e Gold)

| Campo | Tipo | Descricao |
|---|---|---|
| _processed_at | TIMESTAMP | Momento do processamento |
| _source | VARCHAR | Origem: simulator ou minio |
| _pipeline_ver | VARCHAR | Versao do pipeline |

Auditoria completa via Airflow: DAG Run ID, Task Instance logs, XCom metrics.

---

## Conformidade LGPD

O pipeline UrbanFlow **nao processa dados pessoais identificaveis**:

| Dataset | PII? | Justificativa |
|---|---|---|
| GPS onibus | Nao | Posicao do veiculo, nao do passageiro |
| Catracas | Nao | Evento anonimo (sem ID de usuario) |
| Bikes | Nao | Disponibilidade de estacao, nao uso individual |
| Viagens | Nao | Dados agregados sem identificacao pessoal |

Caso dados pessoais sejam incluidos no futuro: pseudonimizacao (SHA-256+salt),
minimizacao de coleta, registro de base legal, suporte ao direito ao esquecimento.

---

## Resumo das Decisoes de Seguranca

| Decisao | Escolha | Motivo |
|---|---|---|
| Criptografia Airflow credentials | SIM (Fernet AES-128) | Padrao obrigatorio do Airflow |
| Criptografia PostgreSQL em repouso | NAO | Dados publicos, ambiente isolado, sem PII |
| Criptografia MinIO em repouso | NAO | Dados publicos, sem PII, KMS adicionaria complexidade |
| Criptografia em transito | NAO | Rede interna Docker, localhost apenas |
| Autenticacao Airflow | SIM | Controle de acesso a UI e API |
| Autenticacao MinIO | SIM | Access Key / Secret Key obrigatorios |
| Variaveis no codigo | NAO | Exclusivamente via .env (fora do git) |
| Dados pessoais no pipeline | NAO | Mobilidade publica anonimizada |
