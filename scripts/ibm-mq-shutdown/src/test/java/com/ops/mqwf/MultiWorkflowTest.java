package com.ops.mqwf;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.ops.mqwf.FlowController.FlowState;

public class MultiWorkflowTest {
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private Fakes.Events events;
    private Fakes.Queue mq;
    private Fakes.Flows flows;
    private Fakes.Clock clock;
    private Config cfg;
    private Orchestrator.Options yes;
    private final List<List<String>> confirmations = new ArrayList<>();
    private boolean confirmAnswer = true;

    @Before
    public void setUp() throws Exception {
        events = new Fakes.Events();
        mq = new Fakes.Queue(events);
        flows = new Fakes.Flows(events);
        clock = new Fakes.Clock();
        cfg = Fakes.config3(tmp.getRoot().toPath());
        yes = new Orchestrator.Options();
        yes.yes = true;
    }

    /** Workflow names whose lock is held by "another run". */
    private final List<String> lockedElsewhere = new ArrayList<>();

    private Orchestrator orchestrator() {
        Orchestrator.Locker locker = wf -> {
            if (lockedElsewhere.contains(wf.name)) {
                throw new WorkflowException(ExitCodes.LOCKED, "Workflow " + wf.name + " is locked");
            }
            events.list.add("LOCK:" + wf.name);
            return () -> events.list.add("UNLOCK:" + wf.name);
        };
        return new Orchestrator(cfg, mq, flows, AuditLog.console(), clock, (wfs, action) -> {
            List<String> names = new ArrayList<>();
            for (WorkflowDef wf : wfs) {
                names.add(wf.name);
            }
            confirmations.add(names);
            return confirmAnswer;
        }, locker);
    }

    private List<WorkflowDef> all() {
        return new ArrayList<>(cfg.workflows().values());
    }

    /** Events without the lock bookkeeping. */
    private List<String> flowEvents() {
        List<String> l = new ArrayList<>();
        for (String e : events.list) {
            if (!e.startsWith("LOCK:") && !e.startsWith("UNLOCK:")) {
                l.add(e);
            }
        }
        return l;
    }

    @Test
    public void shutsDownAllInListedOrder() {
        assertEquals(ExitCodes.OK, orchestrator().shutdownAll(all(), yes));
        assertEquals(asList("STOP_FLOW:FlowA", "STOP_FLOW:FlowB", "STOP_FLOW:FlowC"), flowEvents());
    }

    @Test
    public void eachWorkflowIsLockedOnlyWhileItIsProcessed() {
        assertEquals(ExitCodes.OK, orchestrator().shutdownAll(all(), yes));
        assertEquals(asList(
                "LOCK:A", "STOP_FLOW:FlowA", "UNLOCK:A",
                "LOCK:B", "STOP_FLOW:FlowB", "UNLOCK:B",
                "LOCK:C", "STOP_FLOW:FlowC", "UNLOCK:C"), events.list);
    }

    @Test
    public void preflightFailureOfOneDoesNotAffectTheOthers() {
        flows.others.put("FlowB", FlowState.UNKNOWN);

        assertEquals(ExitCodes.PREFLIGHT_FAILED, orchestrator().shutdownAll(all(), yes));
        assertEquals(asList("STOP_FLOW:FlowA", "STOP_FLOW:FlowC"), flowEvents());
    }

    @Test
    public void executionFailureOfOneDoesNotAffectTheOthers() {
        mq.get("APP.QB").depth = 5; // remote client never consumes B's queue

        assertEquals(ExitCodes.DRAIN_NOT_COMPLETED, orchestrator().shutdownAll(all(), yes));
        assertEquals(asList("STOP_FLOW:FlowA", "STOP_FLOW:FlowB", "START_FLOW:FlowB", "STOP_FLOW:FlowC"),
                flowEvents());
        assertEquals("A stays shut down", FlowState.STOPPED, flows.others.get("FlowA"));
        assertEquals("C is still shut down", FlowState.STOPPED, flows.others.get("FlowC"));
    }

    @Test
    public void lockedWorkflowIsSkippedWithoutBlockingTheOthers() {
        lockedElsewhere.add("B");

        assertEquals(ExitCodes.LOCKED, orchestrator().shutdownAll(all(), yes));
        assertEquals(asList("STOP_FLOW:FlowA", "STOP_FLOW:FlowC"), flowEvents());
    }

    @Test
    public void eachWorkflowIsCheckedWhenItsTurnComes() {
        // While A drains, someone stops C's flow and messages pile up: C must now be refused.
        clock.onSleep = () -> {
            flows.others.put("FlowC", FlowState.STOPPED);
            mq.get("APP.QC").depth = 2;
        };

        assertEquals(ExitCodes.PREFLIGHT_FAILED, orchestrator().shutdownAll(all(), yes));
        assertEquals(asList("STOP_FLOW:FlowA", "STOP_FLOW:FlowB"), flowEvents());
    }

    @Test
    public void startsOneByOneInListedOrderSkippingRunningOnes() {
        flows.others.put("FlowA", FlowState.STOPPED);
        flows.others.put("FlowC", FlowState.STOPPED);

        assertEquals(ExitCodes.OK, orchestrator().startAll(all(), yes));
        assertEquals(asList("START_FLOW:FlowA", "START_FLOW:FlowC"), flowEvents());
    }

    @Test
    public void startFailureOfOneDoesNotAffectTheOthers() {
        flows.others.put("FlowA", FlowState.STOPPED);
        flows.others.put("FlowB", FlowState.UNKNOWN);
        flows.others.put("FlowC", FlowState.STOPPED);

        assertEquals(ExitCodes.PREFLIGHT_FAILED, orchestrator().startAll(all(), yes));
        assertEquals(asList("START_FLOW:FlowA", "START_FLOW:FlowC"), flowEvents());
    }

    @Test
    public void oneConfirmationUpFrontCoversTheWholeList() {
        flows.others.put("FlowA", FlowState.STOPPED);

        assertEquals(ExitCodes.OK, orchestrator().startAll(all(), new Orchestrator.Options()));
        assertEquals(asList(asList("A", "B", "C")), confirmations);
        assertEquals(asList("START_FLOW:FlowA"), flowEvents());
    }

    @Test
    public void declinedConfirmationChangesNothing() {
        confirmAnswer = false;
        assertEquals(ExitCodes.NOT_CONFIRMED, orchestrator().shutdownAll(all(), new Orchestrator.Options()));
        assertEquals(emptyList(), events.list);
    }

    @Test
    public void dryRunChangesNothing() {
        Orchestrator.Options dry = new Orchestrator.Options();
        dry.dryRun = true;
        assertEquals(ExitCodes.OK, orchestrator().shutdownAll(all(), dry));
        assertEquals(emptyList(), flowEvents());
        assertEquals(emptyList(), confirmations);
    }

    @Test
    public void worstExitCodeWins() {
        assertEquals(ExitCodes.OK_WITH_WARNINGS, ExitCodes.worst(ExitCodes.OK, ExitCodes.OK_WITH_WARNINGS));
        assertEquals(ExitCodes.ROLLBACK_FAILED, ExitCodes.worst(ExitCodes.ROLLBACK_FAILED, ExitCodes.ERROR));
        assertEquals(ExitCodes.VERIFY_FAILED, ExitCodes.worst(ExitCodes.DRAIN_NOT_COMPLETED, ExitCodes.VERIFY_FAILED));
    }

    // ------------------------------------------------------- command-line selection

    private static List<String> names(List<WorkflowDef> wfs) {
        List<String> n = new ArrayList<>();
        for (WorkflowDef wf : wfs) {
            n.add(wf.name);
        }
        return n;
    }

    @Test
    public void noNamesSelectsAllInListedOrder() throws Exception {
        assertEquals(asList("A", "B", "C"), names(Main.select(cfg, "shutdown", emptyList())));
        assertEquals(asList("A", "B", "C"), names(Main.select(cfg, "status", emptyList())));
    }

    @Test
    public void startWithoutNamesUsesListedOrder() throws Exception {
        assertEquals(asList("A", "B", "C"), names(Main.select(cfg, "start", emptyList())));
    }

    @Test
    public void explicitNamesKeepGivenOrder() throws Exception {
        assertEquals(asList("C", "A"), names(Main.select(cfg, "shutdown", asList("C", "A"))));
    }

    @Test
    public void duplicateOrUnknownNamesAreUsageErrors() throws Exception {
        for (List<String> bad : asList(asList("A", "A"), asList("A", "NOPE"))) {
            try {
                Main.select(cfg, "shutdown", bad);
                fail();
            } catch (WorkflowException e) {
                assertEquals(ExitCodes.USAGE, e.getExitCode());
            }
        }
    }
}
