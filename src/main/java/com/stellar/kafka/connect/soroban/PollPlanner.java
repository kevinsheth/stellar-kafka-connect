package com.stellar.kafka.connect.soroban;

import java.util.Optional;

public final class PollPlanner {
    public Optional<PollPlan> plan(Long lastProcessedLedger, long latestLedger, String configuredStartLedger,
                                   int maxLedgersPerPoll, int maxRecordsPerPoll) {
        long start = lastProcessedLedger == null ? initialStart(configuredStartLedger, latestLedger) : lastProcessedLedger + 1;
        if (start > latestLedger) {
            return Optional.empty();
        }
        long end = Math.min(latestLedger, start + maxLedgersPerPoll - 1L);
        return Optional.of(new PollPlan(start, end, maxRecordsPerPoll));
    }

    private long initialStart(String configuredStartLedger, long latestLedger) {
        if ("latest".equals(configuredStartLedger)) {
            return latestLedger;
        }
        return Long.parseLong(configuredStartLedger);
    }
}
