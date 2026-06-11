# Changelog

## 0.1.1 - Unreleased

- Prepare Confluent Hub archive documentation for Marketplace distribution.

## 0.1.0 - 2026-06-10

- Initial Stellar Soroban source connector.
- Polls Stellar RPC `getEvents` with Kafka Connect source offsets.
- Emits schemaless Kafka Connect values for `JsonConverter` with schemas disabled.
- Supports contract ID, event type, and topic filters.
