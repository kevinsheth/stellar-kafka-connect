package com.stellar.kafka.connect.soroban.rpc;

import java.util.List;
import java.util.Optional;

public record GetEventsResponse(List<SorobanEvent> events, Optional<String> cursor) {
}
