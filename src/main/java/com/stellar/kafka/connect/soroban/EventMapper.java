package com.stellar.kafka.connect.soroban;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.stellar.kafka.connect.soroban.rpc.SorobanEvent;

public final class EventMapper {
    private final ObjectMapper mapper;

    public EventMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public String key(String network, SorobanEvent event) {
        return network + ":" + event.ledger() + ":" + event.txHash() + ":" + event.eventIndex();
    }

    public String value(String network, SorobanEvent event) {
        ObjectNode root = mapper.createObjectNode();
        root.put("network", network);
        root.put("ledger", event.ledger());
        root.put("txHash", event.txHash());
        root.put("contractId", event.contractId());
        root.put("eventIndex", event.eventIndex());
        root.put("eventType", event.eventType());
        root.set("topics", mapper.valueToTree(event.topics()));
        root.set("value", event.value());
        event.pagingToken().ifPresent(token -> root.put("pagingToken", token));
        event.closedAt().ifPresent(closedAt -> root.put("closedAt", closedAt.toString()));
        root.set("raw", event.raw());
        try {
            return mapper.writeValueAsString(root);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to map Soroban event to JSON", e);
        }
    }
}
