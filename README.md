# Ecommerce Data Platform

Base inicial para uma plataforma de dados de e-commerce inspirada na arquitetura do desenho anexado:

- geracao local de eventos de dominio
- streaming com Kafka + Schema Registry
- entidades fixas de referencia para rastreabilidade
- organizacao para camadas `bronze`, `silver` e `gold`
- preparo para recomendacao, analytics e serving

## Arquitetura inicial

### Fluxo local

1. O app `event-generator` simula fluxos de navegacao e compra.
2. Os eventos usam entidades estaveis vindas de `reference-data/customers.json` e `reference-data/products.json`.
3. Os eventos sao publicados em topicos Kafka por dominio.
4. Os contratos ficam versionados em `contracts/jsonschema`.
5. O repositorio ja reserva as pastas de dados para evolucao das camadas `bronze`, `silver` e `gold`.

### Componentes neste scaffold

- `docker-compose.yml`: Kafka, Schema Registry, Kafka UI e o event generator
- `apps/event_generator`: produtor Python de eventos sinteticos
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

Servicos:

- Kafka: `localhost:9092`
- Schema Registry: `http://localhost:8081`
- Kafka UI: `http://localhost:8080`

## Rodando o gerador de eventos localmente

```powershell
cd apps
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r event_generator\requirements.txt
python -m event_generator.main --flows 0 --interval-ms 250 --purchase-probability 0.35
```

Selecao do destino Kafka:

- `--target local`: usa Kafka local/containerizado com `PLAINTEXT`
- `--target aiven`: usa Aiven com `SASL_SSL` por padrao, ou `SSL` com cert/key se configurado

Exemplo local:

```powershell
python -m event_generator.main --target local --bootstrap-servers localhost:9092
```

Exemplo Aiven:

```powershell
$env:EVENT_GENERATOR_TARGET="aiven"
$env:AIVEN_KAFKA_AUTH_MODE="sasl_ssl"
$env:AIVEN_KAFKA_BOOTSTRAP_SERVERS="your-project.aivencloud.com:12835"
$env:AIVEN_KAFKA_USERNAME="avnadmin"
$env:AIVEN_KAFKA_PASSWORD="your-password"
$env:AIVEN_KAFKA_SSL_CA="C:\certs\ca.pem"
python -m event_generator.main --flows 0
```

Exemplo Aiven com certificado de cliente:

```powershell
$env:EVENT_GENERATOR_TARGET="aiven"
$env:AIVEN_KAFKA_AUTH_MODE="ssl"
$env:AIVEN_KAFKA_BOOTSTRAP_SERVERS="your-project.aivencloud.com:12835"
$env:AIVEN_KAFKA_SSL_CA="C:\certs\ca.pem"
$env:AIVEN_KAFKA_SSL_CERT="C:\certs\service.cert"
$env:AIVEN_KAFKA_SSL_KEY="C:\certs\service.key"
python -m event_generator.main --flows 0
```

No Docker Compose, essas variaveis podem vir automaticamente do ambiente do host ou de um arquivo `.env` na raiz do projeto. Exemplo de `.env`:

```dotenv
EVENT_GENERATOR_TARGET=aiven
AIVEN_KAFKA_AUTH_MODE=sasl_ssl
AIVEN_KAFKA_BOOTSTRAP_SERVERS=your-project.aivencloud.com:12835
AIVEN_KAFKA_USERNAME=avnadmin
AIVEN_KAFKA_PASSWORD=your-password
AIVEN_KAFKA_SSL_CA=/app/auth/ca.pem
```

Depois disso, basta subir normalmente:

```powershell
docker compose up -d --build --scale event-generator=3
```

Uso da pasta `auth/`:

- `auth/service.cert`: certificado do cliente Aiven
- `auth/service.key`: chave privada do cliente Aiven
- para validacao TLS do broker, voce ainda precisa do certificado da CA do Aiven, por exemplo `auth/ca.pem`

O Docker Compose monta `./auth` como `/app/auth` dentro do container. Entao:

- para `sasl_ssl`, normalmente voce usa `username/password` e `AIVEN_KAFKA_SSL_CA=/app/auth/ca.pem`
- para `ssl`, voce usa `AIVEN_KAFKA_SSL_CA=/app/auth/ca.pem`, `AIVEN_KAFKA_SSL_CERT=/app/auth/service.cert` e `AIVEN_KAFKA_SSL_KEY=/app/auth/service.key`

Exemplo `.env` para `sasl_ssl`:

```dotenv
EVENT_GENERATOR_TARGET=aiven
AIVEN_KAFKA_AUTH_MODE=sasl_ssl
AIVEN_KAFKA_BOOTSTRAP_SERVERS=your-project.aivencloud.com:12835
AIVEN_KAFKA_SASL_MECHANISM=SCRAM-SHA-256
AIVEN_KAFKA_USERNAME=avnadmin
AIVEN_KAFKA_PASSWORD=your-password
AIVEN_KAFKA_SSL_CA=/app/auth/ca.pem
```

Exemplo `.env` para `ssl` com os arquivos da pasta `auth/`:

```dotenv
EVENT_GENERATOR_TARGET=aiven
AIVEN_KAFKA_AUTH_MODE=ssl
AIVEN_KAFKA_BOOTSTRAP_SERVERS=your-project.aivencloud.com:12835
AIVEN_KAFKA_SSL_CA=/app/auth/ca.pem
AIVEN_KAFKA_SSL_CERT=/app/auth/service.cert
AIVEN_KAFKA_SSL_KEY=/app/auth/service.key
```

Cada execucao gera um fluxo coerente:

- fluxos podem abandonar no meio do funil, emitindo apenas eventos de `click`
- fluxos de compra sempre emitem `click` ate `checkout`, depois `purchase`
- fluxos de compra sempre emitem uma `transaction` com o mesmo `order_id`

Os eventos sao publicados em topicos separados:

- `ecommerce.clicks`
- `ecommerce.purchases`
- `ecommerce.transactions`

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

Cada instancia executa seus proprios fluxos e publica nos mesmos topicos de dominio.

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
