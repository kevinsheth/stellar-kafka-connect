package com.stellar.kafka.connect.soroban.rpc;

public record GetEventsRequest(long startLedger, long endLedgerInclusive, int limit, EventFilter filter) {
}
