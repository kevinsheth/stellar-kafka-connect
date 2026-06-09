package com.stellar.kafka.connect.soroban;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StellarSorobanSourceConnectorConfigTest {
    @Test
    void validatesNetwork() {
        Map<String, String> props = TestSupport.props();
        props.put(StellarSorobanSourceConnectorConfig.NETWORK, "futurenet");
        assertThrows(ConfigException.class, () -> new StellarSorobanSourceConnectorConfig(props));
    }

    @Test
    void defaultsEventTypeToContract() {
        StellarSorobanSourceConnectorConfig config = new StellarSorobanSourceConnectorConfig(TestSupport.props());
        assertEquals(List.of("contract"), config.eventTypes());
    }
}
