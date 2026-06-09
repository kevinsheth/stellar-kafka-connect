package com.stellar.kafka.connect.soroban.rpc;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public record SorobanEvent(
        String id,
        long ledger,
        String txHash,
        String contractId,
        int eventIndex,
        String eventType,
        List<JsonNode> topics,
        JsonNode value,
        Optional<String> pagingToken,
        Optional<Instant> closedAt,
        JsonNode raw
) {
}
