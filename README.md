# Stellar Soroban Kafka Connect Source Connector

Kafka Connect source connector for Stellar Soroban events. It polls Stellar RPC `getEvents` and emits schemaless records.

This connector is distributed as a Confluent Hub component archive.

## Features

- At least once delivery using Kafka Connect source offsets.
- Polls Stellar RPC `getEvents` for Soroban events.
- Supports contract ID, event type, and topic filters.
- Emits schemaless values for `JsonConverter` with schemas disabled.

## Install

Install the connector from its Confluent Hub or Confluent Marketplace listing.

```bash
confluent connect plugin install kevinsheth/stellar-soroban-source-connector:latest
```

Self-managed Kafka Connect workers use the same component archive. Extract it under a worker `plugin.path` directory and restart the worker:

```bash
unzip kevinsheth-stellar-soroban-source-connector-0.1.0.zip -d /usr/share/kafka-connect/plugins
```

Configure the worker with `JsonConverter` and schemas disabled for values:

```properties
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
```

The worker must be able to reach the configured Stellar RPC endpoint.

For Confluent Cloud custom connectors, also allow the RPC host with `confluent.custom.connection.endpoints`. Use `host:port` without an `http://` or `https://` prefix:

```json
"confluent.custom.connection.endpoints": "soroban-testnet.stellar.org:443"
```

## Configuration

| Name | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `stellar.rpc.url` | string | yes | | Stellar RPC endpoint. |
| `stellar.network` | string | yes | | `mainnet` or `testnet`. |
| `stellar.start.ledger` | string | yes | | `latest` or a non-negative ledger. |
| `topic` | string | yes | | Kafka topic for event records. |
| `stellar.contract.ids` | list | no | empty | At most five contract IDs. |
| `stellar.event.types` | list | no | `contract` | `contract`, `system`, or `diagnostic`. |
| `stellar.topic.filters` | list | no | empty | At most four segment matchers. |
| `stellar.max.ledgers.per.poll` | int | no | `10` | Must be at least `1`. |
| `stellar.max.records.per.poll` | int | no | `1000` | Must be at least `1`. |
| `stellar.poll.interval.ms` | long | no | `5000` | Must be at least `1`. |
| `stellar.request.timeout.ms` | long | no | `30000` | Must be at least `1`. |
| `stellar.retry.max.attempts` | int | no | `3` | Must be at least `1`. |

Record keys are strings in the form `network:eventId`. Values are schemaless maps suitable for Kafka Connect `JsonConverter` with schemas disabled.

## Limitations

- The connector runs as one task.
- `stellar.contract.ids` accepts at most five contract IDs.
- `stellar.topic.filters` accepts at most four segment matchers.
- The configured Stellar RPC endpoint must support `getEvents`.

## Quick Start

Create a connector with the example configuration:

```bash
curl -X PUT http://localhost:8083/connectors/stellar-soroban-source/config \
  -H 'content-type: application/json' \
  -d @etc/connector-config.json
```

Check status:

```bash
curl http://localhost:8083/connectors/stellar-soroban-source/status
```

## Development

```bash
mvn test
mvn package
```

The release archive is written to `target/components/packages/kevinsheth-stellar-soroban-source-connector-<version>.zip`.

Run live public testnet RPC coverage with `mvn -Dstellar.liveTests=true -Dtest=HttpStellarRpcClientLiveTest test`.

## Docker Demo

```bash
mvn package
docker compose -p stellar-kafka-connect up -d
```

Create the connector:

```bash
curl -X PUT http://localhost:8083/connectors/stellar-soroban-source/config \
  -H 'content-type: application/json' \
  -d @demo/connector-config.json
```

Check status:

```bash
curl http://localhost:8083/connectors/stellar-soroban-source/status
```

## License

Apache License 2.0. See [LICENSE](LICENSE).
