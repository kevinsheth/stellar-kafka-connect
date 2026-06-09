package com.stellar.kafka.connect.soroban.rpc;

import java.util.List;

public record EventFilter(List<String> contractIds, List<String> eventTypes, List<String> topicFilters) {
}
