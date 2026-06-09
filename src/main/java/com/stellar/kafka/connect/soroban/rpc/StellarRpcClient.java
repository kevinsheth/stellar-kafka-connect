package com.stellar.kafka.connect.soroban.rpc;

public interface StellarRpcClient extends AutoCloseable {
    long latestLedger() throws RpcException;

    GetEventsResponse getEvents(GetEventsRequest request) throws RpcException;

    @Override
    default void close() {
    }
}
