# 🚌 UrbanFlow — Plataforma de Engenharia de Dados para Mobilidade Urbana

> Protótipo de Ciclo de Vida de Engenharia de Dados — Parte 1: Planejamento Arquitetural

---

## 👥 Integrantes

| Nome Completo | Matrícula |
|---|---|
| *(Integrante 1)* | *(matrícula)* |
| *(Integrante 2)* | *(matrícula)* |

**Disciplina:** Engenharia de Dados  
**Data de Entrega:** 30/04/2026

---

## 📁 Estrutura do Repositório

```
mobilidade-urbana-dataeng/
│
├── README.md                        ← Este arquivo (visão geral)
├── docs/
│   ├── 01-descricao-projeto.md      ← Contexto, problema e stakeholders
│   ├── 02-definicao-dados.md        ← Fontes, formatos, classificação
│   ├── 03-dominios-servicos.md      ← Domínios de negócio e serviços
│   ├── 04-arquitetura.md            ← Fluxo de dados e decisões arquiteturais
│   ├── 05-tecnologias.md            ← Stack tecnológica justificada
│   └── 06-consideracoes-finais.md   ← Riscos, próximos passos, referências
└── diagrams/
    └── (diagramas embutidos nos .md via Mermaid)
```

---

## 🗺️ Visão Geral Rápida

O projeto **UrbanFlow** simula a plataforma de dados de uma empresa de mobilidade urbana que opera **ônibus, metrô e bicicletas compartilhadas** em uma cidade de médio porte. O objetivo é construir uma infraestrutura de dados capaz de:

- Monitorar frotas e viagens **em tempo real**
- Analisar **padrões de demanda** por região e horário
- Apoiar decisões de **planejamento operacional e expansão**
- Fornecer **indicadores de qualidade de serviço** para órgãos reguladores

### Stack Principal (100% gratuita/open-source)

| Camada | Tecnologia |
|---|---|
| Ingestão Streaming | Apache Kafka |
| Ingestão Batch | Apache Airflow + Python |
| Armazenamento Raw | MinIO (S3-compatível local) |
| Processamento | Apache Spark + dbt |
| Armazenamento Analítico | DuckDB / PostgreSQL |
| Orquestração | Apache Airflow |
| Visualização | Apache Superset |
| Infraestrutura | Docker Compose |

---

## 📚 Documentação Completa

Acesse cada seção pela pasta [`docs/`](./docs/):

1. [Descrição do Projeto](./docs/01-descricao-projeto.md)
2. [Definição e Classificação dos Dados](./docs/02-definicao-dados.md)
3. [Domínios e Serviços](./docs/03-dominios-servicos.md)
4. [Arquitetura e Fluxo de Dados](./docs/04-arquitetura.md)
5. [Tecnologias](./docs/05-tecnologias.md)
6. [Considerações Finais](./docs/06-consideracoes-finais.md)
