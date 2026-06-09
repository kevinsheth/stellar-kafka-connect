package com.stellar.kafka.connect.soroban.rpc;

import java.time.Duration;
import java.util.Optional;

public final class RateLimitedException extends RpcException {
    private final Duration retryAfter;

    public RateLimitedException(String message, Duration retryAfter) {
        super(message);
        this.retryAfter = retryAfter;
    }

    public Optional<Duration> retryAfter() {
        return Optional.ofNullable(retryAfter);
    }
}
