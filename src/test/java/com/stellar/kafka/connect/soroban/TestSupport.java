package com.stellar.kafka.connect.soroban;

import java.util.HashMap;
import java.util.Map;

final class TestSupport {
    private TestSupport() {
    }

    static Map<String, String> props() {
        Map<String, String> props = new HashMap<>();
        props.put(StellarSorobanSourceConnectorConfig.RPC_URL, "http://localhost:8000/rpc");
        props.put(StellarSorobanSourceConnectorConfig.NETWORK, "testnet");
        props.put(StellarSorobanSourceConnectorConfig.START_LEDGER, "100");
        props.put(StellarSorobanSourceConnectorConfig.REQUEST_TIMEOUT_MS, "1000");
        props.put(StellarSorobanSourceConnectorConfig.RETRY_MAX_ATTEMPTS, "1");
        props.put(StellarSorobanSourceConnectorConfig.POLL_INTERVAL_MS, "1");
        props.put(StellarSorobanSourceConnectorConfig.TOPIC, "stellar.soroban.events");
        return props;
    }
}
