package com.ops.mqwf;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.ops.mqwf.FlowController.FlowState;

public class OrchestratorTest {
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private Fakes.Events events;
    private Fakes.Queue queue;
    private Fakes.Flows flows;
    private Fakes.Clock clock;
    private Orchestrator.Options yes;

    @Before
    public void setUp() {
        events = new Fakes.Events();
        queue = new Fakes.Queue(events);
        flows = new Fakes.Flows(events);
        clock = new Fakes.Clock();
        yes = new Orchestrator.Options();
        yes.yes = true;
    }

    private Orchestrator orchestrator(String type) throws Exception {
        return new Orchestrator(Fakes.config(tmp.getRoot().toPath(), type), queue, flows, AuditLog.console(),
                clock, (wfs, action) -> true);
    }

    private WorkflowDef wf(Orchestrator o, String type) throws Exception {
        return Fakes.config(tmp.getRoot().toPath(), type).workflow("WF");
    }

    private int shutdown(String type, Orchestrator.Options opts) throws Exception {
        Orchestrator o = orchestrator(type);
        return o.shutdown(wf(o, type), opts);
    }

    private int expectFailure(String type, Orchestrator.Options opts) throws Exception {
        try {
            shutdown(type, opts);
            fail("expected WorkflowException");
            return -1;
        } catch (WorkflowException e) {
            return e.getExitCode();
        }
    }

    /** The ACE flow consumes one message per poll interval while running. */
    private void flowConsumes() {
        clock.onSleep = () -> {
            if (flows.state == FlowState.RUNNING && queue.depth > 0) {
                queue.depth--;
            }
        };
    }

    // ---------------------------------------------------------------- MQ_TO_DB

    @Test
    public void mqToDb_drainsThenStopsWithoutTouchingPut() throws Exception {
        queue.depth = 3;
        flowConsumes();
        int[] depthAtStop = { -1 };
        flows.onStop = () -> depthAtStop[0] = queue.depth;

        assertEquals(ExitCodes.OK, shutdown("MQ_TO_DB", yes));
        assertEquals(asList("STOP_FLOW"), events.list);
        assertEquals("flow must only be stopped once the queue is empty", 0, depthAtStop[0]);
        assertFalse("PUT attribute is never changed", queue.putInhibited);
    }

    @Test
    public void mqToDb_continuousInflowNeverStopsTheFlow() throws Exception {
        queue.depth = 1;
        clock.onSleep = () -> queue.depth = 1; // flow consumes one, a client puts another, every interval

        assertEquals(ExitCodes.DRAIN_NOT_COMPLETED, expectFailure("MQ_TO_DB", yes));
        assertEquals(emptyList(), events.list);
        assertEquals(FlowState.RUNNING, flows.state);
    }

    @Test
    public void mqToDb_messageArrivingAfterStopRestartsFlow() throws Exception {
        flowConsumes();
        flows.onStop = () -> queue.depth = 1; // a client puts right as the flow stops

        assertEquals(ExitCodes.VERIFY_FAILED, expectFailure("MQ_TO_DB", yes));
        assertEquals(asList("STOP_FLOW", "START_FLOW"), events.list);
        assertEquals(FlowState.RUNNING, flows.state);
        assertEquals("the restarted flow processes the late message", 1, queue.depth);
    }

    @Test
    public void mqToDb_waitsForUncommittedUnitOfWork() throws Exception {
        // CURDEPTH is 0 but the flow still has an uncommitted MQGET + DB insert in flight.
        queue.depth = 0;
        queue.uncommitted = 1;
        int[] polls = { 0 };
        clock.onSleep = () -> {
            if (++polls[0] == 4) {
                queue.uncommitted = 0;
            }
        };
        int[] uncomAtStop = { -1 };
        flows.onStop = () -> uncomAtStop[0] = queue.uncommitted;

        assertEquals(ExitCodes.OK, shutdown("MQ_TO_DB", yes));
        assertEquals(0, uncomAtStop[0]);
    }

    @Test
    public void mqToDb_backedOutMessageResetsStableCount() throws Exception {
        queue.depth = 1;
        int[] polls = { 0 };
        clock.onSleep = () -> {
            polls[0]++;
            if (polls[0] == 1) {
                queue.depth = 0;          // got under syncpoint...
            } else if (polls[0] == 2) {
                queue.depth = 1;          // ...and backed out
            } else if (polls[0] == 3) {
                queue.depth = 0;
            }
        };
        int[] pollsAtStop = { -1 };
        flows.onStop = () -> pollsAtStop[0] = polls[0];

        assertEquals(ExitCodes.OK, shutdown("MQ_TO_DB", yes));
        assertTrue("needs 3 consecutive empty checks after the backout", pollsAtStop[0] >= 5);
    }

    @Test
    public void mqToDb_drainTimeoutLeavesFlowRunning() throws Exception {
        queue.depth = 5; // flow is attached but never makes progress

        assertEquals(ExitCodes.DRAIN_NOT_COMPLETED, expectFailure("MQ_TO_DB", yes));
        assertEquals(emptyList(), events.list);
        assertEquals(FlowState.RUNNING, flows.state);
    }

    @Test
    public void mqToDb_stallWithNoConsumerAbortsBeforeTimeout() throws Exception {
        queue.depth = 5;
        queue.openInput = 0;

        assertEquals(ExitCodes.DRAIN_NOT_COMPLETED, expectFailure("MQ_TO_DB", yes));
        assertTrue("stall detected well before the 60 s timeout", clock.now < 10_000);
        assertEquals(emptyList(), events.list);
    }

    @Test
    public void mqToDb_stoppedFlowWithMessagesFailsPreflight() throws Exception {
        flows.state = FlowState.STOPPED;
        queue.depth = 2;

        assertEquals(ExitCodes.PREFLIGHT_FAILED, expectFailure("MQ_TO_DB", yes));
        assertEquals(emptyList(), events.list);
    }

    @Test
    public void mqToDb_backoutQueueGrowthIsReported() throws Exception {
        queue.depth = 2;
        queue.boq = "APP.Q.BOQ";
        clock.onSleep = () -> {
            if (queue.depth > 0) {
                queue.depth--;
                queue.boqDepth++;
            }
        };
        assertEquals(ExitCodes.OK_WITH_WARNINGS, shutdown("MQ_TO_DB", yes));
    }

    // ---------------------------------------------------------------- DB_TO_MQ

    @Test
    public void dbToMq_stopsProducerThenWaitsForRemoteConsumer() throws Exception {
        queue.depth = 3;
        clock.onSleep = () -> {
            if (queue.depth > 0) {
                queue.depth--; // remote client consumes regardless of the flow
            }
        };
        int[] depthAtStop = { -1 };
        flows.onStop = () -> depthAtStop[0] = queue.depth;

        assertEquals(ExitCodes.OK, shutdown("DB_TO_MQ", yes));
        assertEquals(asList("STOP_FLOW"), events.list);
        assertEquals("producer is stopped first", 3, depthAtStop[0]);
        assertEquals(0, queue.depth);
        assertFalse("DB_TO_MQ never touches the PUT attribute", queue.putInhibited);
    }

    @Test
    public void dbToMq_drainTimeoutRestartsProducer() throws Exception {
        queue.depth = 5;
        queue.openInput = 0; // remote client not connected; stall detection is off for DB_TO_MQ

        assertEquals(ExitCodes.DRAIN_NOT_COMPLETED, expectFailure("DB_TO_MQ", yes));
        assertEquals(asList("STOP_FLOW", "START_FLOW"), events.list);
        assertTrue("waited for the full timeout", clock.now >= 60_000);
    }

    @Test
    public void dbToMq_noRollbackLeavesFlowStopped() throws Exception {
        queue.depth = 5;
        yes.noRollback = true;

        assertEquals(ExitCodes.DRAIN_NOT_COMPLETED, expectFailure("DB_TO_MQ", yes));
        assertEquals(asList("STOP_FLOW"), events.list);
    }

    @Test
    public void failedRollbackIsReportedAsManualAction() throws Exception {
        queue.depth = 5;
        flows.failStart = true;

        assertEquals(ExitCodes.ROLLBACK_FAILED, expectFailure("DB_TO_MQ", yes));
    }

    // ------------------------------------------------------------------ common

    @Test
    public void unknownFlowStateFailsPreflight() throws Exception {
        flows.state = FlowState.UNKNOWN;
        assertEquals(ExitCodes.PREFLIGHT_FAILED, expectFailure("MQ_TO_DB", yes));
        assertEquals(emptyList(), events.list);
    }

    @Test
    public void getInhibitedQueueWithMessagesFailsPreflight() throws Exception {
        queue.depth = 1;
        queue.getInhibited = true;
        assertEquals(ExitCodes.PREFLIGHT_FAILED, expectFailure("DB_TO_MQ", yes));
        assertEquals(emptyList(), events.list);
    }

    @Test
    public void dryRunChangesNothing() throws Exception {
        queue.depth = 3;
        Orchestrator.Options dry = new Orchestrator.Options();
        dry.dryRun = true;
        assertEquals(ExitCodes.OK, shutdown("MQ_TO_DB", dry));
        assertEquals(ExitCodes.OK, shutdown("DB_TO_MQ", dry));
        assertEquals(emptyList(), events.list);
    }

    @Test
    public void declinedConfirmationChangesNothing() throws Exception {
        Orchestrator o = new Orchestrator(Fakes.config(tmp.getRoot().toPath(), "MQ_TO_DB"), queue, flows,
                AuditLog.console(), clock, (wfs, action) -> false);
        try {
            o.shutdown(wf(o, "MQ_TO_DB"), new Orchestrator.Options());
            fail();
        } catch (WorkflowException e) {
            assertEquals(ExitCodes.NOT_CONFIRMED, e.getExitCode());
        }
        assertEquals(emptyList(), events.list);
    }

    // ------------------------------------------------------------------- start

    @Test
    public void startMqToDb_startsFlowOnly() throws Exception {
        flows.state = FlowState.STOPPED;
        Orchestrator o = orchestrator("MQ_TO_DB");

        assertEquals(ExitCodes.OK, o.start(wf(o, "MQ_TO_DB"), yes));
        assertEquals(asList("START_FLOW"), events.list);
    }

    @Test
    public void startMqToDb_leavesPutAttributeAlone() throws Exception {
        flows.state = FlowState.STOPPED;
        queue.putInhibited = true; // set by someone else: warned about, not changed
        Orchestrator o = orchestrator("MQ_TO_DB");

        assertEquals(ExitCodes.OK, o.start(wf(o, "MQ_TO_DB"), yes));
        assertEquals(asList("START_FLOW"), events.list);
        assertTrue(queue.putInhibited);
    }

    @Test
    public void startWhenAlreadyRunningDoesNothing() throws Exception {
        Orchestrator o = orchestrator("MQ_TO_DB");
        assertEquals(ExitCodes.OK, o.start(wf(o, "MQ_TO_DB"), yes));
        assertEquals(emptyList(), events.list);
    }
}
