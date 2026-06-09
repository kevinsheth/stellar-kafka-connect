package com.stellar.kafka.connect.soroban;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stellar.kafka.connect.soroban.rpc.GetEventsRequest;
import com.stellar.kafka.connect.soroban.rpc.GetEventsResponse;
import com.stellar.kafka.connect.soroban.rpc.RpcException;
import com.stellar.kafka.connect.soroban.rpc.SorobanEvent;
import com.stellar.kafka.connect.soroban.rpc.StellarRpcClient;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StellarSorobanSourceTaskTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void returnsDeterministicKeyAndOffset() throws Exception {
        FakeRpcClient rpc = new FakeRpcClient(110);
        rpc.events.add(event(100, "tx1", 0));
        StellarSorobanSourceTask task = new StellarSorobanSourceTask(mapper, new PollPlanner(), new Retry(), MetricsHooks.noop(), rpc);
        task.start(TestSupport.props());

        List<SourceRecord> records = task.poll();

        assertEquals(1, records.size());
        assertEquals("testnet:100:tx1:0", records.get(0).key());
        assertEquals(100L, records.get(0).sourceOffset().get(StellarSorobanSourceTask.OFFSET_FIELD));
    }

    @Test
    void rpcFailureReturnsNoRecordsAndDoesNotExposeOffset() throws Exception {
        StellarRpcClient rpc = new StellarRpcClient() {
            @Override public long latestLedger() throws RpcException { throw new RpcException("boom"); }
            @Override public GetEventsResponse getEvents(GetEventsRequest request) { throw new AssertionError("not called"); }
        };
        StellarSorobanSourceTask task = new StellarSorobanSourceTask(mapper, new PollPlanner(), new Retry(), MetricsHooks.noop(), rpc);
        task.start(TestSupport.props());

        assertTrue(task.poll().isEmpty());
    }

    @Test
    void emptyWindowDoesNotPersistOffsetButContinuesScanningInMemory() throws Exception {
        FakeRpcClient rpc = new FakeRpcClient(120);
        rpc.events.add(event(115, "tx1", 0));
        StellarSorobanSourceTask task = new StellarSorobanSourceTask(mapper, new PollPlanner(), new Retry(), MetricsHooks.noop(), rpc);
        task.start(TestSupport.props());

        assertTrue(task.poll().isEmpty());
        List<SourceRecord> records = task.poll();

        assertEquals(1, records.size());
        assertEquals(115L, records.get(0).sourceOffset().get(StellarSorobanSourceTask.OFFSET_FIELD));
    }

    @Test
    void refusesToSplitSingleLedgerAcrossPolls() throws Exception {
        FakeRpcClient rpc = new FakeRpcClient(110);
        rpc.events.add(event(100, "tx1", 0));
        rpc.events.add(event(100, "tx1", 1));
        Map<String, String> props = TestSupport.props();
        props.put(StellarSorobanSourceConnectorConfig.MAX_RECORDS_PER_POLL, "1");
        StellarSorobanSourceTask task = new StellarSorobanSourceTask(mapper, new PollPlanner(), new Retry(), MetricsHooks.noop(), rpc);
        task.start(props);

        assertTrue(task.poll().isEmpty());
    }

    private SorobanEvent event(long ledger, String txHash, int index) {
        return new SorobanEvent(ledger, txHash, "CABC", index, "contract", List.of(), mapper.createObjectNode(),
                Optional.of("cursor-" + index), Optional.empty(), mapper.createObjectNode().put("ledger", ledger));
    }

    private static final class FakeRpcClient implements StellarRpcClient {
        private final long latestLedger;
        private final List<SorobanEvent> events = new ArrayList<>();

        private FakeRpcClient(long latestLedger) {
            this.latestLedger = latestLedger;
        }

        @Override public long latestLedger() { return latestLedger; }

        @Override public GetEventsResponse getEvents(GetEventsRequest request) {
            List<SorobanEvent> bounded = events.stream()
                    .filter(event -> event.ledger() >= request.startLedger() && event.ledger() <= request.endLedgerInclusive())
                    .limit(request.limit())
                    .toList();
            return new GetEventsResponse(bounded, Optional.empty());
        }
    }
}
