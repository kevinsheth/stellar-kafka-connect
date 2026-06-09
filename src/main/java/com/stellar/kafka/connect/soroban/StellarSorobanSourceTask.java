package com.stellar.kafka.connect.soroban;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stellar.kafka.connect.soroban.rpc.EventFilter;
import com.stellar.kafka.connect.soroban.rpc.GetEventsRequest;
import com.stellar.kafka.connect.soroban.rpc.GetEventsResponse;
import com.stellar.kafka.connect.soroban.rpc.HttpStellarRpcClient;
import com.stellar.kafka.connect.soroban.rpc.RateLimitedException;
import com.stellar.kafka.connect.soroban.rpc.RpcException;
import com.stellar.kafka.connect.soroban.rpc.SorobanEvent;
import com.stellar.kafka.connect.soroban.rpc.StellarRpcClient;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class StellarSorobanSourceTask extends SourceTask {
    static final String STREAM = "soroban-events";
    static final String OFFSET_FIELD = "lastProcessedLedger";
    private static final Logger log = LoggerFactory.getLogger(StellarSorobanSourceTask.class);

    private final ObjectMapper objectMapper;
    private final PollPlanner pollPlanner;
    private final Retry retry;
    private StellarRpcClient rpcClient;
    private StellarSorobanSourceConnectorConfig config;
    private EventMapper eventMapper;
    private Map<String, Object> sourcePartition;
    private boolean ownsClient;
    private Long nextLedgerOverride;
    private volatile boolean stopped;

    public StellarSorobanSourceTask() {
        this(new ObjectMapper(), new PollPlanner(), new Retry(), null);
        this.ownsClient = true;
    }

    StellarSorobanSourceTask(ObjectMapper objectMapper, PollPlanner pollPlanner, Retry retry, StellarRpcClient rpcClient) {
        this.objectMapper = objectMapper;
        this.pollPlanner = pollPlanner;
        this.retry = retry;
        this.rpcClient = rpcClient;
        this.ownsClient = rpcClient == null;
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new StellarSorobanSourceConnectorConfig(props);
        this.eventMapper = new EventMapper(objectMapper);
        this.sourcePartition = Map.of(
                "network", config.network(),
                "stream", STREAM,
                "rpcUrl", config.rpcUrl(),
                "contractIds", String.join(",", config.contractIds()),
                "eventTypes", String.join(",", config.eventTypes()),
                "topicFilters", String.join(",", config.topicFilters()));
        this.stopped = false;
        if (rpcClient == null) {
            this.rpcClient = new HttpStellarRpcClient(config.rpcUrl(), Duration.ofMillis(config.requestTimeoutMs()), objectMapper);
        }
        log.info("event=task_start network={} topic={} maxLedgersPerPoll={} maxRecordsPerPoll={}",
                config.network(), config.topic(), config.maxLedgersPerPoll(), config.maxRecordsPerPoll());
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Long lastProcessedLedger = lastProcessedLedger();
        try {
            long latest = retry.execute(config.retryMaxAttempts(), rpcClient::latestLedger);
            Optional<PollPlanner.PollPlan> maybePlan = pollPlanner.plan(effectiveLastProcessedLedger(lastProcessedLedger), latest, config.startLedger(),
                    config.maxLedgersPerPoll(), config.maxRecordsPerPoll());
            if (maybePlan.isEmpty()) {
                awaitNextPoll();
                return List.of();
            }
            PollPlanner.PollPlan plan = maybePlan.get();
            GetEventsResponse response = retry.execute(config.retryMaxAttempts(), () -> rpcClient.getEvents(new GetEventsRequest(
                    plan.startLedger(), plan.endLedgerInclusive(), plan.maxRecords() + 1,
                    new EventFilter(config.contractIds(), config.eventTypes(), config.topicFilters()))));
            List<SorobanEvent> bounded = completeLedgerPrefix(response.events().stream()
                    .filter(event -> event.ledger() >= plan.startLedger() && event.ledger() <= plan.endLedgerInclusive())
                    .sorted(Comparator.comparingLong(SorobanEvent::ledger).thenComparingInt(SorobanEvent::eventIndex))
                    .toList(), plan.maxRecords());
            if (bounded.isEmpty()) {
                log.info("event=empty_poll network={} startLedger={} endLedger={} latestLedger={}",
                        config.network(), plan.startLedger(), plan.endLedgerInclusive(), latest);
                nextLedgerOverride = plan.endLedgerInclusive() + 1;
                awaitNextPoll();
                return List.of();
            }
            List<SourceRecord> records = new ArrayList<>(bounded.size());
            for (SorobanEvent event : bounded) {
                records.add(toSourceRecord(event));
            }
            log.info("event=records_returned network={} startLedger={} endLedger={} records={}",
                    config.network(), plan.startLedger(), plan.endLedgerInclusive(), records.size());
            nextLedgerOverride = bounded.get(bounded.size() - 1).ledger() + 1;
            return records;
        } catch (RateLimitedException e) {
            log.warn("event=rpc_rate_limited network={} message={}", config.network(), e.getMessage());
            awaitNextPoll();
            return List.of();
        } catch (RpcException e) {
            log.warn("event=rpc_failure network={} message={}", config.network(), e.getMessage());
            awaitNextPoll();
            return List.of();
        }
    }

    @Override
    public void stop() {
        stopped = true;
        synchronized (this) {
            notifyAll();
        }
        log.info("event=task_stop network={}", config == null ? "unknown" : config.network());
        if (ownsClient && rpcClient != null) {
            rpcClient.close();
        }
    }

    private SourceRecord toSourceRecord(SorobanEvent event) {
        Map<String, Object> sourceOffset = Map.of(OFFSET_FIELD, event.ledger());
        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                config.topic(),
                null,
                Schema.STRING_SCHEMA,
                eventMapper.key(config.network(), event),
                null,
                eventMapper.value(config.network(), event));
    }

    private void awaitNextPoll() throws InterruptedException {
        synchronized (this) {
            if (!stopped) {
                wait(config.pollIntervalMs());
            }
        }
    }

    private Long effectiveLastProcessedLedger(Long storedLastProcessedLedger) {
        if (nextLedgerOverride == null) {
            return storedLastProcessedLedger;
        }
        long overrideLastProcessed = nextLedgerOverride - 1;
        if (storedLastProcessedLedger == null || overrideLastProcessed > storedLastProcessedLedger) {
            return overrideLastProcessed;
        }
        nextLedgerOverride = null;
        return storedLastProcessedLedger;
    }

    private List<SorobanEvent> completeLedgerPrefix(List<SorobanEvent> events, int maxRecords) throws RpcException {
        if (events.size() <= maxRecords) {
            return events;
        }
        long possiblyTruncatedLedger = events.get(maxRecords).ledger();
        List<SorobanEvent> complete = events.stream()
                .filter(event -> event.ledger() < possiblyTruncatedLedger)
                .limit(maxRecords)
                .toList();
        if (complete.isEmpty()) {
            throw new RpcException("RPC returned more events for ledger " + possiblyTruncatedLedger
                    + " than stellar.max.records.per.poll; increase the limit to avoid splitting a ledger");
        }
        return complete;
    }

    private Long lastProcessedLedger() {
        Map<String, Object> offset = context.offsetStorageReader().offset(sourcePartition);
        if (offset == null) {
            return null;
        }
        Object value = offset.get(OFFSET_FIELD);
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String string) {
            return Long.parseLong(string);
        }
        throw new IllegalStateException("Invalid stored offset: lastProcessedLedger must be numeric");
    }
}
