package com.stellar.kafka.connect.soroban.rpc;

public class RpcException extends Exception {
    public RpcException(String message) {
        super(message);
    }

    public RpcException(String message, Throwable cause) {
        super(message, cause);
    }

    public boolean retryable() {
        return true;
    }
}
