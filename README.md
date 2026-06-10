# Stellar Soroban Kafka Connect Source Connector

Kafka Connect source connector for Stellar Soroban events. It polls Stellar RPC `getEvents` and emits schemaless records.

## Configuration

| Name | Required | Default |
| --- | --- | --- |
| `stellar.rpc.url` | yes | |
| `stellar.network` | yes | |
| `stellar.start.ledger` | yes | |
| `topic` | yes | |
| `stellar.contract.ids` | no | empty |
| `stellar.event.types` | no | `contract` |
| `stellar.topic.filters` | no | empty |
| `stellar.max.ledgers.per.poll` | no | `10` |
| `stellar.max.records.per.poll` | no | `1000` |
| `stellar.poll.interval.ms` | no | `5000` |
| `stellar.request.timeout.ms` | no | `30000` |
| `stellar.retry.max.attempts` | no | `3` |

Record keys are strings in the form `network:eventId`. Values are schemaless maps suitable for Kafka Connect `JsonConverter` with schemas disabled.

## Build

```bash
mvn test
mvn package
```

The release archive is written to `target/components/packages/stellar-soroban-source-connector-<version>.zip`.

Run live public testnet RPC coverage with `mvn -Dstellar.liveTests=true -Dtest=HttpStellarRpcClientLiveTest test`.

## Install

Extract the release archive under a Kafka Connect worker `plugin.path` directory and restart the worker:

```bash
unzip target/components/packages/stellar-soroban-source-connector-0.1.0-SNAPSHOT.zip -d /usr/share/kafka-connect/plugins
```

Configure the worker with `JsonConverter` and schemas disabled for values:

```properties
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
```

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
