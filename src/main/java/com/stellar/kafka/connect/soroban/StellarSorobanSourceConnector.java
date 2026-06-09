package com.stellar.kafka.connect.soroban;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class StellarSorobanSourceConnector extends SourceConnector {
    private Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        new StellarSorobanSourceConnectorConfig(props);
        this.props = Map.copyOf(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return StellarSorobanSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks < 1) {
            return List.of();
        }
        List<Map<String, String>> configs = new ArrayList<>(1);
        configs.add(props);
        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return StellarSorobanSourceConnectorConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return Version.VERSION;
    }
}
