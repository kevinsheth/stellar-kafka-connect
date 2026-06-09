package com.stellar.kafka.connect.soroban;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigDef.ValidString;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public final class StellarSorobanSourceConnectorConfig extends AbstractConfig {
    public static final String RPC_URL = "stellar.rpc.url";
    public static final String NETWORK = "stellar.network";
    public static final String CONTRACT_IDS = "stellar.contract.ids";
    public static final String EVENT_TYPES = "stellar.event.types";
    public static final String TOPIC_FILTERS = "stellar.topic.filters";
    public static final String START_LEDGER = "stellar.start.ledger";
    public static final String MAX_LEDGERS_PER_POLL = "stellar.max.ledgers.per.poll";
    public static final String MAX_RECORDS_PER_POLL = "stellar.max.records.per.poll";
    public static final String POLL_INTERVAL_MS = "stellar.poll.interval.ms";
    public static final String REQUEST_TIMEOUT_MS = "stellar.request.timeout.ms";
    public static final String RETRY_MAX_ATTEMPTS = "stellar.retry.max.attempts";
    public static final String TOPIC = "topic";

    private static final Set<String> EVENT_TYPES_ALLOWED = Set.of("contract", "system", "diagnostic");

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(RPC_URL, STRING, ConfigDef.NO_DEFAULT_VALUE, HIGH, "Stellar RPC URL.")
            .define(NETWORK, STRING, ConfigDef.NO_DEFAULT_VALUE, ValidString.in("mainnet", "testnet"), HIGH, "Stellar network: mainnet or testnet.")
            .define(CONTRACT_IDS, LIST, List.of(), MEDIUM, "Comma-separated contract IDs to include.")
            .define(EVENT_TYPES, LIST, List.of("contract"), MEDIUM, "Comma-separated event types: contract,system,diagnostic.")
            .define(TOPIC_FILTERS, LIST, List.of(), MEDIUM, "Comma-separated topic filters passed to RPC.")
            .define(START_LEDGER, STRING, ConfigDef.NO_DEFAULT_VALUE, HIGH, "Starting ledger: latest or integer.")
            .define(MAX_LEDGERS_PER_POLL, INT, 10, ConfigDef.Range.atLeast(1), MEDIUM, "Maximum ledger window per poll.")
            .define(MAX_RECORDS_PER_POLL, INT, 1000, ConfigDef.Range.atLeast(1), MEDIUM, "Maximum SourceRecords returned per poll.")
            .define(POLL_INTERVAL_MS, LONG, 5000L, ConfigDef.Range.atLeast(1), MEDIUM, "Sleep interval when no records are available.")
            .define(REQUEST_TIMEOUT_MS, LONG, 30000L, ConfigDef.Range.atLeast(1), MEDIUM, "RPC request timeout in milliseconds.")
            .define(RETRY_MAX_ATTEMPTS, INT, 3, ConfigDef.Range.atLeast(1), MEDIUM, "Maximum RPC retry attempts.")
            .define(TOPIC, STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), HIGH, "Kafka topic for event records.");

    public StellarSorobanSourceConnectorConfig(Map<?, ?> originals) {
        super(CONFIG_DEF, originals);
        validate();
    }

    public String rpcUrl() {
        return getString(RPC_URL);
    }

    public String network() {
        return getString(NETWORK);
    }

    public List<String> contractIds() {
        return getList(CONTRACT_IDS);
    }

    public List<String> eventTypes() {
        return getList(EVENT_TYPES);
    }

    public List<String> topicFilters() {
        return getList(TOPIC_FILTERS);
    }

    public String startLedger() {
        return getString(START_LEDGER);
    }

    public int maxLedgersPerPoll() {
        return getInt(MAX_LEDGERS_PER_POLL);
    }

    public int maxRecordsPerPoll() {
        return getInt(MAX_RECORDS_PER_POLL);
    }

    public long pollIntervalMs() {
        return getLong(POLL_INTERVAL_MS);
    }

    public long requestTimeoutMs() {
        return getLong(REQUEST_TIMEOUT_MS);
    }

    public int retryMaxAttempts() {
        return getInt(RETRY_MAX_ATTEMPTS);
    }

    public String topic() {
        return getString(TOPIC);
    }

    private void validate() {
        try {
            URI uri = URI.create(rpcUrl());
            if (uri.getScheme() == null || uri.getHost() == null
                    || !("http".equals(uri.getScheme()) || "https".equals(uri.getScheme()))) {
                throw new ConfigException(RPC_URL, rpcUrl(), "must be an absolute HTTP(S) URL");
            }
        } catch (IllegalArgumentException e) {
            throw new ConfigException(RPC_URL, rpcUrl(), "must be a valid URL");
        }
        for (String eventType : eventTypes()) {
            if (!EVENT_TYPES_ALLOWED.contains(eventType)) {
                throw new ConfigException(EVENT_TYPES, eventType, "must be one of " + EVENT_TYPES_ALLOWED);
            }
        }
        String start = startLedger();
        if (!"latest".equals(start)) {
            try {
                long parsed = Long.parseLong(start);
                if (parsed < 0) {
                    throw new NumberFormatException("negative");
                }
            } catch (NumberFormatException e) {
                throw new ConfigException(START_LEDGER, start, "must be 'latest' or a non-negative integer");
            }
        }
    }
}
