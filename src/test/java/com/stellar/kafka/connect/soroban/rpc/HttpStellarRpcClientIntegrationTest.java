package com.stellar.kafka.connect.soroban.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HttpStellarRpcClientIntegrationTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void readsEventsFromMockRpc() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(json("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"events\":[{\"ledger\":100,\"ledgerClosedAt\":\"2026-06-09T11:51:37Z\",\"txHash\":\"tx1\",\"contractId\":\"CABC\",\"id\":\"0000000000000000100-0000000007\",\"type\":\"contract\",\"topic\":[\"AAAADwAAAANmZWU=\"],\"value\":{\"x\":1},\"cursor\":\"c1\"}],\"cursor\":\"c1\"}}"));
            HttpStellarRpcClient client = new HttpStellarRpcClient(server.url("/rpc").toString(), Duration.ofSeconds(10), mapper);

            GetEventsResponse response = client.getEvents(new GetEventsRequest(100, 109, 1000,
                    new EventFilter(List.of("CABC"), List.of("contract"), List.of("AAAADwAAAANmZWU="))));

            assertEquals(1, response.events().size());
            assertEquals("0000000000000000100-0000000007", response.events().get(0).id());
            assertEquals(100L, response.events().get(0).ledger());
            assertEquals(7, response.events().get(0).eventIndex());
            assertEquals("tx1", response.events().get(0).txHash());
            assertEquals(1, response.events().get(0).topics().size());
            assertEquals("2026-06-09T11:51:37Z", response.events().get(0).closedAt().orElseThrow().toString());
            assertEquals("c1", response.cursor().orElseThrow());
            var request = mapper.readTree(server.takeRequest().getBody().readUtf8());
            assertEquals("getEvents", request.get("method").asText());
            assertEquals(110, request.path("params").path("endLedger").asInt());
            assertEquals("AAAADwAAAANmZWU=", request.path("params").path("filters").get(0)
                    .path("topics").get(0).get(0).asText());
        }
    }

    @Test
    void handlesRateLimit() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setResponseCode(429).addHeader("retry-after", "1"));
            HttpStellarRpcClient client = new HttpStellarRpcClient(server.url("/rpc").toString(), Duration.ofSeconds(10), mapper);

            assertThrows(RateLimitedException.class, client::latestLedger);
        }
    }

    private MockResponse json(String body) {
        return new MockResponse().setResponseCode(200).addHeader("content-type", "application/json").setBody(body);
    }
}
