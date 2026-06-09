package com.stellar.kafka.connect.soroban;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PollPlannerTest {
    private final PollPlanner planner = new PollPlanner();

    @Test
    void boundsLedgerRange() {
        PollPlanner.PollPlan plan = planner.plan(99L, 200L, "latest", 10, 1000).orElseThrow();
        assertEquals(100L, plan.startLedger());
        assertEquals(109L, plan.endLedgerInclusive());
    }

    @Test
    void startsAtLatestWhenConfigured() {
        PollPlanner.PollPlan plan = planner.plan(null, 200L, "latest", 10, 1000).orElseThrow();
        assertEquals(200L, plan.startLedger());
        assertEquals(200L, plan.endLedgerInclusive());
    }

    @Test
    void emptyWhenCaughtUp() {
        assertTrue(planner.plan(200L, 200L, "100", 10, 1000).isEmpty());
    }
}
