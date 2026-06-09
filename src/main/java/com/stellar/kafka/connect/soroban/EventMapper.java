package com.stellar.kafka.connect.soroban;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stellar.kafka.connect.soroban.rpc.SorobanEvent;

import java.util.LinkedHashMap;
import java.util.Map;

public final class EventMapper {
    private final ObjectMapper mapper;

    public EventMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public String key(String network, SorobanEvent event) {
        return network + ":" + event.id();
    }

    public Map<String, Object> value(String network, SorobanEvent event) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("network", network);
        value.put("id", event.id());
        value.put("ledger", event.ledger());
        value.put("txHash", event.txHash());
        value.put("contractId", event.contractId());
        value.put("eventIndex", event.eventIndex());
        value.put("eventType", event.eventType());
        value.put("topics", mapper.convertValue(event.topics(), Object.class));
        value.put("value", mapper.convertValue(event.value(), Object.class));
        event.pagingToken().ifPresent(token -> value.put("pagingToken", token));
        event.closedAt().ifPresent(closedAt -> value.put("closedAt", closedAt.toString()));
        value.put("raw", mapper.convertValue(event.raw(), Object.class));
        return value;
    }
}
