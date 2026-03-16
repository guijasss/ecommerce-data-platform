# Arquitetura e roadmap

## Traducao do desenho para este repositorio

### 1. Origem de eventos

`apps/event_generator` representa o bloco "Event Generator App" rodando localmente.

Entidades de referencia:

- `reference-data/customers.json` com 50 customers
- `reference-data/products.json` com 100 products

Dominios cobertos no scaffold:

- clicks
- purchases
- transactions
- support tickets

### 2. Ingestao e contratos

O gerador grava eventos como arquivos JSONL em um Unity Catalog Volume (Databricks Files API), usando
`DATABRICKS_HOST`, `DATABRICKS_TOKEN` e `DATABRICKS_VOLUME_PATH`.

Os contratos ficam versionados em `contracts/jsonschema`.

### 3. Camadas de dados

As pastas `data/bronze`, `data/silver` e `data/gold` representam a separacao logica do lakehouse.

Nesta fase:

- `bronze`: persistencia crua por evento (arquivos no Volume)
- `silver`: eventos enriquecidos/normalizados (Delta/UC tables)
- `gold`: tabelas analiticas e features de recomendacao

### 4. Analytics e recomendacao

O scaffold ainda nao cria consumidores de `gold`, mas organiza o projeto para isso:

- `pipelines/bronze`
- `pipelines/silver`
- `pipelines/gold`
- `pipelines/features`

### 5. Evolucao sugerida

1. Persistir `bronze` e criar tabelas Delta/UC em `silver`.
2. Construir modelo dimensional e fatos em `silver/gold`.
3. Gerar grafo de co-compra e embeddings de produto/cliente.
4. Treinar e versionar modelo com MLflow.
5. Expor um servico de recomendacao HTTP com cache e fallback.

