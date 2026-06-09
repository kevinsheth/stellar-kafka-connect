package com.stellar.kafka.connect.soroban;

import org.apache.kafka.connect.connector.Connector;
import org.junit.jupiter.api.Test;

import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertTrue;

class StellarSorobanSourceConnectorDiscoveryTest {
    @Test
    void isDiscoverableByServiceLoader() {
        boolean found = ServiceLoader.load(Connector.class).stream()
                .anyMatch(provider -> provider.type().equals(StellarSorobanSourceConnector.class));

        assertTrue(found, "connector must be discoverable by Kafka Connect service loader");
    }
}
