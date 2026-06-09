package com.stellar.kafka.connect.soroban;

import com.stellar.kafka.connect.soroban.rpc.RateLimitedException;
import com.stellar.kafka.connect.soroban.rpc.RpcException;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public final class Retry {
    @FunctionalInterface
    public interface RpcCall<T> {
        T call() throws RpcException;
    }

    public <T> T execute(int maxAttempts, RpcCall<T> call) throws RpcException {
        RpcException last = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return call.call();
            } catch (RpcException e) {
                last = e;
                if (attempt == maxAttempts || !e.retryable()) {
                    throw e;
                }
                sleep(delayFor(attempt, e));
            }
        }
        throw last;
    }

    private Duration delayFor(int attempt, RpcException exception) {
        if (exception instanceof RateLimitedException rateLimited) {
            return rateLimited.retryAfter().orElseGet(() -> exponentialJitter(attempt));
        }
        return exponentialJitter(attempt);
    }

    private Duration exponentialJitter(int attempt) {
        long base = Math.min(30_000L, 250L * (1L << Math.min(10, attempt - 1)));
        long jitter = ThreadLocalRandom.current().nextLong(0, Math.max(1, base / 2));
        return Duration.ofMillis(base + jitter);
    }

    private void sleep(Duration delay) throws RpcException {
        try {
            Thread.sleep(delay.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RpcException("Interrupted while backing off after RPC failure", e);
        }
    }
}
