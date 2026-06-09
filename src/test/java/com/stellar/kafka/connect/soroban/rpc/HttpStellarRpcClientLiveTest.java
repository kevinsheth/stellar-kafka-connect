package com.stellar.kafka.connect.soroban.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class HttpStellarRpcClientLiveTest {
    @Test
    void readsRecentTestnetContractEvents() throws Exception {
        assumeTrue(Boolean.getBoolean("stellar.liveTests"));

        String rpcUrl = System.getProperty("stellar.rpc.url", "https://soroban-testnet.stellar.org");
        try (HttpStellarRpcClient client = new HttpStellarRpcClient(rpcUrl, Duration.ofSeconds(30), new ObjectMapper())) {
            long latest = client.latestLedger();
            long start = Math.max(1, latest - 20);
            GetEventsResponse response = client.getEvents(new GetEventsRequest(
                    start,
                    latest,
                    10,
                    new EventFilter(List.of(), List.of("contract"), List.of())));

            assertFalse(response.events().isEmpty());
            assertTrue(response.events().stream().allMatch(event -> event.ledger() >= start && event.ledger() <= latest));
            assertTrue(response.events().stream().allMatch(event -> !event.txHash().isBlank()));

            SorobanEvent event = response.events().get(0);
            GetEventsResponse filtered = client.getEvents(new GetEventsRequest(
                    event.ledger(),
                    event.ledger(),
                    10,
                    new EventFilter(List.of(event.contractId()), List.of("contract"), List.of())));

            assertFalse(filtered.events().isEmpty());
            assertTrue(filtered.events().stream().allMatch(filteredEvent -> event.contractId().equals(filteredEvent.contractId())));
        }
    }
}
