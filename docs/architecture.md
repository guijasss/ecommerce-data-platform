# Arquitetura e roadmap

## Tradução do desenho para este repositório

### 1. Origem de eventos

`apps/event_generator` representa o bloco "Event Generator App" no `localhost`.

Entidades de referência:

- `reference-data/customers.json` com 50 customers
- `reference-data/products.json` com 100 products

Domínios cobertos no scaffold:

- clicks
- purchases
- transactions
- support tickets

### 2. Streaming e contratos

`docker-compose.yml` sobe:

- Kafka
- Schema Registry
- Kafka UI

`contracts/jsonschema` concentra a primeira versão dos contratos. A ideia é evoluir para validação e registro automático no Schema Registry.

O gerador usa `customer_id` e `product_id` dessas dimensões fixas para dar rastreabilidade e consistência histórica aos eventos.

### 3. Camadas de dados

As pastas `data/bronze`, `data/silver` e `data/gold` representam a separação lógica do lakehouse.

Nesta fase:

- `bronze`: persistência crua por tópico/evento
- `silver`: entidades normalizadas e eventos enriquecidos
- `gold`: tabelas analíticas e features de recomendação

### 4. Analytics e recomendação

O desenho mostra dois consumidores do `gold`:

- dashboards/Metabase
- recommendation service

O scaffold ainda não cria esses serviços, mas já organiza o projeto para isso:

- `pipelines/bronze`
- `pipelines/silver`
- `pipelines/gold`
- `pipelines/features`

### 5. Evolução sugerida

1. Persistir `bronze` em Delta Lake.
2. Construir modelo dimensional e fatos em `silver/gold`.
3. Gerar grafo de co-compra e embeddings de produto/cliente.
4. Treinar e versionar modelo com MLflow.
5. Expor um serviço de recomendação HTTP com cache e fallback.
