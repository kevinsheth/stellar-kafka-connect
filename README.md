# Stellar Soroban Kafka Connect Source Connector

Production-shaped v0 Kafka Connect source connector for Stellar Soroban events. It polls Stellar RPC `getEvents`, emits JSON records, and uses Kafka Connect source offset storage.

## Guarantees

This connector provides at-least-once delivery. Duplicates can occur after retries, worker failures, or restarts, and downstream consumers should use the deterministic key `network:ledger:txHash:eventIndex` for idempotency.

Offsets are stored by Kafka Connect only. The source partition is:

```json
{ "network": "testnet", "stream": "soroban-events" }
```

The source offset is:

```json
{ "lastProcessedLedger": 123456 }
```

The task attaches the ledger offset to each returned `SourceRecord`. RPC failures return no records and do not advance offsets. Empty results are tolerated and do not require an external checkpoint database; the task can scan past empty ledger windows in memory, and after a restart it may rescan those empty windows before finding the next event.

## Bounded Polling

Each poll plans a bounded ledger window from the stored offset and `stellar.max.ledgers.per.poll`, then requests a capped event batch. The task avoids splitting a ledger across polls because the v0 offset is ledger-based; if a single ledger has more events than `stellar.max.records.per.poll`, the task logs a clear error and does not advance offsets until the limit is increased. v0 runs a single task, but the code isolates the source partition and planner so partitioning by contract ID can be added later.

## Configuration

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `stellar.rpc.url` | yes | | Stellar RPC URL |
| `stellar.network` | yes | | `mainnet` or `testnet` |
| `stellar.contract.ids` | no | empty | Comma-separated contract IDs |
| `stellar.event.types` | no | `contract` | Comma-separated `contract,system,diagnostic` |
| `stellar.topic.filters` | no | empty | Comma-separated RPC topic filters |
| `stellar.start.ledger` | yes | | `latest` or a non-negative integer |
| `stellar.max.ledgers.per.poll` | no | `10` | Maximum ledger window size |
| `stellar.max.records.per.poll` | no | `1000` | Maximum records returned from one poll |
| `stellar.poll.interval.ms` | no | `5000` | Sleep interval when no records are available or RPC fails |
| `stellar.request.timeout.ms` | no | `30000` | RPC request timeout |
| `stellar.retry.max.attempts` | no | `3` | RPC retry attempts with exponential backoff and jitter |
| `topic` | yes | | Kafka topic for JSON event records |

## Record Format

Keys are strings:

```text
network:ledger:txHash:eventIndex
```

Values are JSON strings with:

```json
{
  "network": "testnet",
  "ledger": 123,
  "txHash": "...",
  "contractId": "...",
  "eventIndex": 0,
  "eventType": "contract",
  "topics": [],
  "value": {},
  "pagingToken": "optional",
  "closedAt": "optional",
  "raw": {}
}
```

## Build And Test

```bash
mvn test
mvn package
```

## Docker Compose Demo

The demo runs exactly three services:

- `confluentinc/cp-kafka` as a single-node KRaft broker/controller
- `confluentinc/cp-kafka-connect` as the Connect worker that loads this connector
- `stellar/quickstart` with local Stellar RPC enabled

It does not run ZooKeeper, Schema Registry, a UI, or any custom sidecar app. Stellar Quickstart is exposed on host port `8001` to avoid common conflicts on `8000`; Kafka Connect reaches it inside Docker at `http://stellar:8000/rpc`.

Build the connector first so `target/connector-plugin` contains the connector jar and runtime dependencies, then start the stack with an explicit project name:

```bash
mvn package
docker compose -p stellar-kafka-connect up -d
```

Create a connector:

```bash
curl -X PUT http://localhost:8083/connectors/stellar-soroban-source/config \
  -H 'content-type: application/json' \
  -d '{
    "connector.class":"com.stellar.kafka.connect.soroban.StellarSorobanSourceConnector",
    "tasks.max":"1",
    "stellar.rpc.url":"http://stellar:8000/rpc",
    "stellar.network":"testnet",
    "stellar.start.ledger":"latest",
    "stellar.event.types":"contract",
    "stellar.max.ledgers.per.poll":"10",
    "stellar.max.records.per.poll":"1000",
    "stellar.poll.interval.ms":"5000",
    "stellar.request.timeout.ms":"30000",
    "stellar.retry.max.attempts":"3",
    "topic":"stellar.soroban.events"
  }'
```

Check status:

```bash
curl http://localhost:8083/connectors/stellar-soroban-source/status
```

The demo uses Kafka Connect `StringConverter` for keys and values because records are emitted as JSON strings. No Avro, Schema Registry, Horizon support, full indexer, or UI is included in v0.
