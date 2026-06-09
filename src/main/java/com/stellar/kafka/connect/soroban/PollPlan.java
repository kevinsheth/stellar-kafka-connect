package com.stellar.kafka.connect.soroban;

public record PollPlan(long startLedger, long endLedgerInclusive, int maxRecords) {
}
