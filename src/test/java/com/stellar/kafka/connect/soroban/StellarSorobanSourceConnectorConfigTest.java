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

    @Test
    void rejectsTooManyTopicSegments() {
        Map<String, String> props = TestSupport.props();
        props.put(StellarSorobanSourceConnectorConfig.TOPIC_FILTERS, "a,b,c,d,e");
        assertThrows(ConfigException.class, () -> new StellarSorobanSourceConnectorConfig(props));
    }

    @Test
    void rejectsTooManyContractIds() {
        Map<String, String> props = TestSupport.props();
        props.put(StellarSorobanSourceConnectorConfig.CONTRACT_IDS, "c1,c2,c3,c4,c5,c6");
        assertThrows(ConfigException.class, () -> new StellarSorobanSourceConnectorConfig(props));
    }
}
