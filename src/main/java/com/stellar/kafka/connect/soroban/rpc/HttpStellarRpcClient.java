package com.stellar.kafka.connect.soroban.rpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public final class HttpStellarRpcClient implements StellarRpcClient {
    private final URI rpcUri;
    private final Duration timeout;
    private final HttpClient httpClient;
    private final ObjectMapper mapper;
    private final AtomicLong ids = new AtomicLong(1);

    public HttpStellarRpcClient(String rpcUrl, Duration timeout, ObjectMapper mapper) {
        this.rpcUri = URI.create(rpcUrl);
        this.timeout = timeout;
        this.mapper = mapper;
        this.httpClient = HttpClient.newBuilder().connectTimeout(timeout).build();
    }

    @Override
    public long latestLedger() throws RpcException {
        JsonNode result = call("getLatestLedger", mapper.createObjectNode());
        JsonNode sequence = result.path("sequence");
        if (!sequence.canConvertToLong()) {
            throw new RpcException("RPC getLatestLedger response missing numeric result.sequence");
        }
        return sequence.asLong();
    }

    @Override
    public GetEventsResponse getEvents(GetEventsRequest request) throws RpcException {
        ObjectNode params = mapper.createObjectNode();
        params.put("startLedger", request.startLedger());
        params.put("endLedger", request.endLedgerInclusive() + 1);
        ObjectNode pagination = params.putObject("pagination");
        pagination.put("limit", request.limit());
        ArrayNode filters = params.putArray("filters");
        for (String eventType : request.filter().eventTypes()) {
            ObjectNode filter = filters.addObject();
            filter.put("type", eventType);
            if (!request.filter().contractIds().isEmpty()) {
                ArrayNode ids = filter.putArray("contractIds");
                request.filter().contractIds().forEach(ids::add);
            }
            if (!request.filter().topicFilters().isEmpty()) {
                ArrayNode topics = filter.putArray("topics");
                ArrayNode topic = topics.addArray();
                request.filter().topicFilters().forEach(topic::add);
            }
        }

        JsonNode result = call("getEvents", params);
        List<SorobanEvent> events = new ArrayList<>();
        for (JsonNode raw : result.path("events")) {
            events.add(toEvent(raw));
        }
        Optional<String> cursor = Optional.ofNullable(result.path("cursor").textValue());
        return new GetEventsResponse(events, cursor);
    }

    private JsonNode call(String method, JsonNode params) throws RpcException {
        ObjectNode body = mapper.createObjectNode();
        body.put("jsonrpc", "2.0");
        body.put("id", ids.getAndIncrement());
        body.put("method", method);
        body.set("params", params);
        HttpRequest request;
        try {
            request = HttpRequest.newBuilder(rpcUri)
                    .timeout(timeout)
                    .header("content-type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
                    .build();
        } catch (IOException e) {
            throw new RpcException("Failed to encode RPC request", e);
        }
        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new RpcException("RPC request failed: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RpcException("RPC request interrupted", e);
        }
        if (response.statusCode() == 429) {
            throw new RateLimitedException("RPC rate limited with HTTP 429", retryAfter(response));
        }
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new RpcException("RPC HTTP status " + response.statusCode() + ": " + response.body());
        }
        try {
            JsonNode root = mapper.readTree(response.body());
            if (root.hasNonNull("error")) {
                throw new RpcException("RPC error for " + method + ": " + root.get("error"));
            }
            JsonNode result = root.get("result");
            if (result == null || result.isNull()) {
                throw new RpcException("RPC response missing result for " + method);
            }
            return result;
        } catch (IOException e) {
            throw new RpcException("Failed to parse RPC response", e);
        }
    }

    private Duration retryAfter(HttpResponse<?> response) {
        return response.headers().firstValue("retry-after")
                .flatMap(value -> {
                    try {
                        return Optional.of(Duration.ofSeconds(Long.parseLong(value)));
                    } catch (NumberFormatException e) {
                        return Optional.empty();
                    }
                })
                .orElse(null);
    }

    private SorobanEvent toEvent(JsonNode raw) {
        String id = raw.path("id").asText("");
        long ledger = raw.path("ledger").asLong(raw.path("ledgerNumber").asLong());
        String txHash = raw.path("txHash").asText(raw.path("transactionHash").asText(""));
        String contractId = raw.path("contractId").asText(null);
        int eventIndex = raw.path("eventIndex").isInt() ? raw.path("eventIndex").asInt() : eventIndexFromId(id);
        String eventType = raw.path("type").asText(raw.path("eventType").asText("contract"));
        List<JsonNode> topics = new ArrayList<>();
        JsonNode topic = raw.has("topic") ? raw.path("topic") : raw.path("topics");
        topic.forEach(topics::add);
        JsonNode value = raw.path("value");
        Optional<String> pagingToken = Optional.ofNullable(raw.path("pagingToken").textValue())
                .or(() -> Optional.ofNullable(raw.path("cursor").textValue()));
        Optional<Instant> closedAt = Optional.ofNullable(raw.path("closedAt").textValue())
                .or(() -> Optional.ofNullable(raw.path("ledgerClosedAt").textValue()))
                .map(Instant::parse);
        return new SorobanEvent(id, ledger, txHash, contractId, eventIndex, eventType, topics, value, pagingToken, closedAt, raw);
    }

    private int eventIndexFromId(String id) {
        int separator = id.lastIndexOf('-');
        if (separator < 0 || separator == id.length() - 1) {
            return 0;
        }
        try {
            return Integer.parseInt(id.substring(separator + 1));
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
