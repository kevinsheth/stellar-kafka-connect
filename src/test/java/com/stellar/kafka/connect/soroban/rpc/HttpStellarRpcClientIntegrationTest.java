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
            server.enqueue(json("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"events\":[{\"ledger\":100,\"txHash\":\"tx1\",\"contractId\":\"CABC\",\"eventIndex\":0,\"type\":\"contract\",\"topics\":[],\"value\":{\"x\":1},\"cursor\":\"c1\"}],\"cursor\":\"c1\"}}"));
            HttpStellarRpcClient client = new HttpStellarRpcClient(server.url("/rpc").toString(), Duration.ofSeconds(10), mapper);

            GetEventsResponse response = client.getEvents(new GetEventsRequest(100, 109, 1000,
                    new EventFilter(List.of("CABC"), List.of("contract"), List.of())));

            assertEquals(1, response.events().size());
            assertEquals(100L, response.events().get(0).ledger());
            assertEquals("tx1", response.events().get(0).txHash());
            assertEquals("c1", response.cursor().orElseThrow());
            assertEquals("getEvents", mapper.readTree(server.takeRequest().getBody().readUtf8()).get("method").asText());
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
