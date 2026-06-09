package com.stellar.kafka.connect.soroban;

import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.Collection;
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

    static void initialize(SourceTask task) {
        task.initialize(new SourceTaskContext() {
            @Override
            public Map<String, String> configs() {
                return props();
            }

            @Override
            public OffsetStorageReader offsetStorageReader() {
                return new OffsetStorageReader() {
                    @Override
                    public <T> Map<String, Object> offset(Map<String, T> partition) {
                        return null;
                    }

                    @Override
                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
                        return Map.of();
                    }
                };
            }
        });
    }
}
