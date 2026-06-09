package com.stellar.kafka.connect.soroban;

public interface MetricsHooks {
    void pollStarted();

    void recordsReturned(int count);

    void rpcFailure(Exception exception);

    void rateLimited();

    void emptyPoll();

    void pollCompleted(long startLedger, long endLedger, int records);

    static MetricsHooks noop() {
        return new MetricsHooks() {
            @Override public void pollStarted() { }
            @Override public void recordsReturned(int count) { }
            @Override public void rpcFailure(Exception exception) { }
            @Override public void rateLimited() { }
            @Override public void emptyPoll() { }
            @Override public void pollCompleted(long startLedger, long endLedger, int records) { }
        };
    }
}
