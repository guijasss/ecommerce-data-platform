# Ecommerce Data Platform

Base inicial para uma plataforma de dados de e-commerce inspirada na arquitetura do desenho anexado:

- geracao local de eventos de dominio
- entidades fixas de referencia para rastreabilidade
- organizacao para camadas `bronze`, `silver` e `gold`
- preparo para recomendacao, analytics e serving

## Arquitetura inicial

### Fluxo local

1. O app `event-generator` simula fluxos de navegacao e compra.
2. Os eventos usam entidades estaveis vindas de `reference-data/customers.json` e `reference-data/products.json`.
3. Os eventos sao gravados como arquivos JSONL em um Unity Catalog Volume (Databricks Files API).
4. Os contratos ficam versionados em `contracts/jsonschema`.
5. O repositorio ja reserva as pastas de dados para evolucao das camadas `bronze`, `silver` e `gold`.

### Componentes neste scaffold

- `docker-compose.yml`: event generator
- `apps/event_generator`: produtor Python de eventos sinteticos para Unity Catalog Volume
- `reference-data`: dimensoes fixas de customers e products
- `contracts/jsonschema`: contratos de eventos
- `docs/architecture.md`: mapeamento entre o desenho e o roadmap tecnico
- `pipelines/`: espaco para jobs de ingestao e transformacao

## Estrutura

```text
.
|-- apps/
|   `-- event_generator/
|-- contracts/
|   `-- jsonschema/
|-- reference-data/
|-- docs/
|-- pipelines/
`-- docker-compose.yml
```

## Subindo o ambiente local

```powershell
docker compose up -d
```

## Rodando o gerador de eventos localmente

```powershell
cd apps
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r event_generator\requirements.txt
python -m event_generator.main --flows 0 --interval-ms 250 --purchase-probability 0.35
```

Destino (Databricks Volume):

- `.env` na raiz do repo:
  - `DATABRICKS_HOST=https://<workspace-host>/`
  - `DATABRICKS_TOKEN=...`
  - `DATABRICKS_VOLUME_PATH=/Volumes/<catalog>/<schema>/<volume>[/<subdir>]`

O gerador grava JSONL em:

`/Volumes/.../<event_type>s/ingest_date=YYYY-MM-DD/events-....jsonl`

Depois disso, basta subir normalmente:

```powershell
docker compose up -d --build --scale event-generator=3
```

Cada execucao gera um fluxo coerente:

- fluxos podem abandonar no meio do funil, emitindo apenas eventos de `click`
- fluxos de compra sempre emitem `click` ate `checkout`, depois `purchase`
- fluxos de compra sempre emitem uma `transaction` com o mesmo `order_id`

Destino de escrita:

Os eventos sao gravados no Volume.

Campos de correlacao:

- `session_id`: relaciona navegacao, compra e transacao da mesma sessao
- `flow_id`: identifica uma execucao completa do funil

Comportamento de execucao:

- `--flows 0`: executa continuamente enquanto o processo/container estiver de pe
- `--flows N`: executa `N` fluxos e encerra

## Multiplas instancias do event generator

Para rodar varias instancias em paralelo com Docker Compose:

```powershell
docker compose up -d --build --scale event-generator=3
```

Cada instancia executa seus proprios fluxos e grava no mesmo Volume (paths separados por data e batch id).

Para parar todas as replicas do gerador ao mesmo tempo:

```powershell
docker compose stop event-generator
```

Para remover os containers do gerador:

```powershell
docker compose rm -f event-generator
```

Para parar todo o stack:

```powershell
docker compose down
```

## Entidades de referencia

- `50` customers em `reference-data/customers.json`
- `100` products em `reference-data/products.json`

Esses datasets funcionam como dimensoes estaveis para construir comportamento historico por `customer_id` e `product_id`.

## Proximos passos recomendados

1. Criar consumidor `bronze` para persistir eventos crus em Delta/Parquet.
2. Implementar transformacoes `silver` com normalizacao e qualidade.
3. Materializar datasets `gold` para BI e recomendacao.
4. Adicionar feature engineering para grafo de co-compra e similaridade.
5. Integrar treino/registry/serving em Databricks ou MLflow local.
