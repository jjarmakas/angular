package com.ops.mqwf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.ops.mqwf.FlowController.FlowState;

/**
 * The safe start/stop sequences. Every run is: read-only pre-flight -> plan -> confirmation ->
 * execute -> verify. Any failure after the flow was stopped restarts it (rollback).
 * <p>
 * Several workflows can be handled in one run. They are independent: each is locked,
 * pre-flighted and executed on its own, one after the other, and a failure in one never blocks
 * or skips the others.
 * <p>
 * The only thing this class ever changes is the run state of the message flow. Queue attributes
 * and messages are only read.
 */
public final class Orchestrator {

    public static final class Options {
        public boolean dryRun;
        public boolean yes;
        public boolean noRollback;
        /** Overrides the configured drain timeout when > 0. */
        public int timeoutSeconds;
    }

    /** Asks the operator to approve the plans already written to the log. */
    public interface Confirmer {
        boolean confirm(List<WorkflowDef> workflows, String action) throws WorkflowException;
    }

    /** What the run has changed for the workflow being executed. Read by rollback and by the Ctrl+C hook. */
    public static final class Changes {
        final WorkflowDef wf;
        volatile boolean flowStopRequested;
        volatile boolean finished;
        volatile String phase = "pre-flight";

        Changes(WorkflowDef wf) {
            this.wf = wf;
        }

        boolean any() {
            return flowStopRequested;
        }

        public String describe() {
            return flowStopRequested
                    ? "message flow " + wf.flowLabel() + " was stopped (or a stop was requested)"
                    : "none";
        }

        public boolean needsAttention() {
            return any() && !finished;
        }

        public WorkflowDef workflow() {
            return wf;
        }

        public String phase() {
            return phase;
        }
    }

    /** Result of a successful pre-flight: the state it saw and the plan it derived from it. */
    static final class Prepared {
        final WorkflowDef wf;
        final QueueSnapshot q;
        final FlowState fs;
        final long timeoutS;
        final List<String> plan;

        Prepared(WorkflowDef wf, QueueSnapshot q, FlowState fs, long timeoutS, List<String> plan) {
            this.wf = wf;
            this.q = q;
            this.fs = fs;
            this.timeoutS = timeoutS;
            this.plan = plan;
        }

        boolean nothingToDo() {
            return plan.isEmpty();
        }
    }

    /** A held per-workflow lock. */
    public interface Lock extends AutoCloseable {
        @Override
        void close();
    }

    /** Takes the lock for one workflow, or fails with exit code LOCKED. */
    public interface Locker {
        Lock acquire(WorkflowDef wf) throws WorkflowException;
    }

    private static final Locker NO_LOCKING = wf -> () -> { };

    private interface Preparer {
        Prepared prepare(WorkflowDef wf, Options o) throws WorkflowException;
    }

    private interface Executor {
        int execute(Prepared p, Options o) throws WorkflowException;
    }

    private final Config cfg;
    private final QueueAdmin mq;
    private final FlowController flows;
    private final AuditLog log;
    private final DrainMonitor drain;
    private final DrainMonitor.Clock clock;
    private final Confirmer confirmer;
    private final Locker locker;
    private volatile Changes changes = new Changes(null);

    public Orchestrator(Config cfg, QueueAdmin mq, FlowController flows, AuditLog log,
            DrainMonitor.Clock clock, Confirmer confirmer) {
        this(cfg, mq, flows, log, clock, confirmer, NO_LOCKING);
    }

    public Orchestrator(Config cfg, QueueAdmin mq, FlowController flows, AuditLog log,
            DrainMonitor.Clock clock, Confirmer confirmer, Locker locker) {
        this.cfg = cfg;
        this.mq = mq;
        this.flows = flows;
        this.log = log;
        this.clock = clock;
        this.drain = new DrainMonitor(log, clock);
        this.confirmer = confirmer;
        this.locker = locker;
    }

    /** Changes for the workflow currently (or last) executed. */
    public Changes changes() {
        return changes;
    }

    // ------------------------------------------------------------ multi-workflow

    /** Status of several workflows. Read-only, so one failure does not stop the others. */
    public int statusAll(List<WorkflowDef> wfs) {
        int worst = ExitCodes.OK;
        for (WorkflowDef wf : wfs) {
            try {
                status(wf);
            } catch (WorkflowException e) {
                log.error("Status of %s failed: %s", wf.name, e.getMessage());
                worst = ExitCodes.worst(worst, e.getExitCode());
            }
        }
        return worst;
    }

    public int shutdownAll(List<WorkflowDef> wfs, Options o) {
        return runAll("SHUTDOWN", wfs, o, this::prepareShutdown, this::executeShutdown);
    }

    public int startAll(List<WorkflowDef> wfs, Options o) {
        return runAll("START", wfs, o, this::prepareStart, this::executeStart);
    }

    /**
     * Workflows are independent, so each one is handled completely on its own, one after the other:
     * lock -> pre-flight -> execute -> unlock, then the next. Nothing about one workflow (its lock,
     * its pre-flight, its failure) delays, blocks or skips another. One confirmation up front
     * covers the whole list; each workflow's plan is logged when its turn comes.
     */
    private int runAll(String action, List<WorkflowDef> wfs, Options o, Preparer preparer, Executor executor) {
        log.info("##### %s of %d workflow(s), one at a time: %s%s #####", action, wfs.size(), names(wfs),
                o.dryRun ? " (DRY RUN - no changes will be made)" : "");
        if (!o.dryRun && !o.yes) {
            try {
                if (!confirmer.confirm(wfs, action.toLowerCase())) {
                    log.error("Not confirmed; nothing was changed");
                    return ExitCodes.NOT_CONFIRMED;
                }
            } catch (WorkflowException e) {
                log.error("%s", e.getMessage());
                return e.getExitCode();
            }
        }

        List<String> summary = new ArrayList<>();
        int worst = ExitCodes.OK;
        for (int i = 0; i < wfs.size(); i++) {
            WorkflowDef wf = wfs.get(i);
            log.info("----- [%d/%d] %s %s -----", i + 1, wfs.size(), action, wf.name);
            String result;
            int rc;
            try (Lock lock = locker.acquire(wf)) {
                Prepared p = preparer.prepare(wf, o);
                if (p.nothingToDo()) {
                    rc = ExitCodes.OK;
                    result = "NOTHING TO DO";
                } else if (o.dryRun) {
                    rc = ExitCodes.OK;
                    result = "DRY RUN OK (pre-flight passed, nothing changed)";
                } else {
                    rc = executor.execute(p, o);
                    result = rc == ExitCodes.OK ? "OK" : "OK WITH WARNINGS";
                }
            } catch (WorkflowException e) {
                log.error("%s of %s failed: %s", action, wf.name, e.getMessage());
                rc = e.getExitCode();
                result = "FAILED: " + e.getMessage();
            }
            worst = ExitCodes.worst(worst, rc);
            summary.add(String.format("%-24s %s (exit %d)", wf.name, result, rc));
        }
        log.info("##### %s summary #####", action);
        for (String line : summary) {
            log.info("  %s", line);
        }
        return worst;
    }

    private static String names(List<WorkflowDef> wfs) {
        List<String> n = new ArrayList<>();
        for (WorkflowDef wf : wfs) {
            n.add(wf.name);
        }
        return String.join(", ", n);
    }

    private void logPlan(List<String> plan) {
        log.info("Plan:");
        for (int i = 0; i < plan.size(); i++) {
            log.info("  %d. %s", i + 1, plan.get(i));
        }
    }

    private void confirmOne(Prepared p, String action, Options o) throws WorkflowException {
        if (!o.yes && !confirmer.confirm(java.util.Collections.singletonList(p.wf), action)) {
            throw new WorkflowException(ExitCodes.NOT_CONFIRMED, "Not confirmed; nothing was changed");
        }
    }

    // ------------------------------------------------------------------ status

    public int status(WorkflowDef wf) throws WorkflowException {
        QueueSnapshot q = mq.snapshot(wf.queue);
        FlowState fs = flowState(wf);
        log.info("Workflow : %s", wf);
        log.info("Flow     : %s", fs);
        log.info("Queue    : %s", q);
        if (q.backoutQueue != null) {
            log.info("Backout  : %s depth=%s", q.backoutQueue, mq.depthOrNull(q.backoutQueue));
        }
        if (fs == FlowState.STOPPED && q.isEmpty()) {
            log.info("Assessment: workflow is shut down and its queue is empty");
        } else if (fs == FlowState.STOPPED) {
            log.warn("Assessment: flow is stopped but the queue is not empty");
        } else {
            log.info("Assessment: workflow is %s", fs);
        }
        return ExitCodes.OK;
    }

    // ---------------------------------------------------------------- shutdown

    /** Single workflow: pre-flight, confirm, execute. Throws on failure. */
    public int shutdown(WorkflowDef wf, Options o) throws WorkflowException {
        Prepared p = prepareShutdown(wf, o);
        if (o.dryRun) {
            log.info("DRY RUN complete: pre-flight passed, nothing was changed");
            return ExitCodes.OK;
        }
        confirmOne(p, "shutdown", o);
        return executeShutdown(p, o);
    }

    private Prepared prepareShutdown(WorkflowDef wf, Options o) throws WorkflowException {
        log.info("=== SHUTDOWN %s: pre-flight ===", wf);
        QueueSnapshot q = mq.snapshot(wf.queue);
        FlowState fs = flowState(wf);
        log.info("Pre-flight: flow %s is %s", wf.flowLabel(), fs);
        log.info("Pre-flight: %s", q);

        if (fs == FlowState.UNKNOWN) {
            throw new WorkflowException(ExitCodes.PREFLIGHT_FAILED,
                    "Cannot determine whether flow " + wf.flowLabel() + " is running; refusing to continue");
        }
        if (q.getInhibited && !q.isEmpty()) {
            throw new WorkflowException(ExitCodes.PREFLIGHT_FAILED, "Queue " + wf.queue
                    + " is GET(DISABLED) and holds messages, so it can never drain; investigate first");
        }
        long timeoutS = o.timeoutSeconds > 0 ? o.timeoutSeconds : wf.drainTimeoutSeconds;
        List<String> plan = wf.type == WorkflowType.DB_TO_MQ
                ? planDbToMq(wf, q, fs, timeoutS)
                : planMqToDb(wf, q, fs, timeoutS);

        logPlan(plan);
        return new Prepared(wf, q, fs, timeoutS, plan);
    }

    private int executeShutdown(Prepared p, Options o) throws WorkflowException {
        WorkflowDef wf = p.wf;
        QueueSnapshot q = p.q;
        changes = new Changes(wf);
        log.info("=== SHUTDOWN %s: executing ===", wf.name);
        Integer boqBefore = q.backoutQueue != null ? mq.depthOrNull(q.backoutQueue) : null;
        try {
            if (wf.type == WorkflowType.DB_TO_MQ) {
                executeDbToMq(wf, p.fs, p.timeoutS);
            } else {
                executeMqToDb(wf, p.fs, p.timeoutS);
            }
        } catch (WorkflowException e) {
            log.error("%s", e.getMessage());
            throw rollback(wf, o, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw rollback(wf, o, new WorkflowException(ExitCodes.ERROR, "Interrupted during " + changes.phase, e));
        } catch (RuntimeException e) {
            log.error("Unexpected error: %s", e);
            throw rollback(wf, o, new WorkflowException(ExitCodes.ERROR, "Unexpected error: " + e, e));
        }
        changes.finished = true;

        int rc = ExitCodes.OK;
        if (boqBefore != null) {
            Integer boqAfter = mq.depthOrNull(q.backoutQueue);
            if (boqAfter != null && boqAfter > boqBefore) {
                log.warn("Backout queue %s grew from %d to %d during the drain: %d message(s) could not be "
                        + "processed and need attention (they are NOT lost)", q.backoutQueue, boqBefore, boqAfter,
                        boqAfter - boqBefore);
                rc = ExitCodes.OK_WITH_WARNINGS;
            }
        }
        log.info("=== SHUTDOWN of %s COMPLETE: flow stopped, queue %s verified empty ===", wf.name, wf.queue);
        log.info("To resume: mqwf start %s", wf.name);
        return rc;
    }

    private List<String> planDbToMq(WorkflowDef wf, QueueSnapshot q, FlowState fs, long timeoutS) {
        List<String> plan = new ArrayList<>();
        if (fs == FlowState.RUNNING) {
            plan.add("Stop message flow " + wf.flowLabel() + " normally (the in-flight DB->MQ unit of work "
                    + "completes; rows not yet read stay in the database for the next start)");
        } else {
            plan.add("Message flow " + wf.flowLabel() + " is already stopped - no flow change");
        }
        plan.add("Wait up to " + timeoutS + " s for the remote client to empty " + wf.queue
                + " (CURDEPTH=0 and UNCOM=0 on " + cfg.drain.stablePolls + " consecutive checks)");
        plan.add("On timeout or error: " + (fs == FlowState.RUNNING ? "restart the flow" : "no changes to undo"));
        if (!q.isEmpty() && q.openInput == 0) {
            log.warn("Pre-flight: %s holds %d message(s) but no client has it open for input. The drain "
                    + "will only finish if the remote consumer connects within the timeout.", wf.queue, q.depth);
        }
        if (q.putInhibited && fs == FlowState.RUNNING) {
            log.warn("Pre-flight: %s is PUT(DISABLED) while the producer flow is running", wf.queue);
        }
        return plan;
    }

    private List<String> planMqToDb(WorkflowDef wf, QueueSnapshot q, FlowState fs, long timeoutS)
            throws WorkflowException {
        if (fs == FlowState.STOPPED && !q.isEmpty()) {
            throw new WorkflowException(ExitCodes.PREFLIGHT_FAILED, "Flow " + wf.flowLabel() + " is stopped but "
                    + wf.queue + " holds " + q.depth + " message(s) / " + q.uncommitted + " uncommitted. "
                    + "Nothing would consume them; start the flow (mqwf start " + wf.name + ") or investigate");
        }
        if (fs == FlowState.RUNNING && q.openInput == 0) {
            log.warn("Pre-flight: flow is running but nothing has %s open for input; if that persists for %d "
                    + "checks the drain aborts", wf.queue, cfg.drain.stallPolls);
        }
        if (q.backoutQueue == null) {
            log.warn("Pre-flight: %s has no BOQNAME; a poison message can block the drain until the timeout",
                    wf.queue);
        }
        if (!q.putInhibited && fs == FlowState.RUNNING) {
            log.warn("Pre-flight: %s stays PUT(ENABLED). Remote clients can keep putting, so the drain only "
                    + "completes during a quiet period; ask the client owners to pause sending first", wf.queue);
        }
        List<String> plan = new ArrayList<>();
        if (fs == FlowState.RUNNING) {
            plan.add("Wait up to " + timeoutS + " s for the flow to empty " + wf.queue + " into the database "
                    + "(CURDEPTH=0 and UNCOM=0 on " + cfg.drain.stablePolls + " consecutive checks, i.e. a quiet "
                    + "period of " + cfg.drain.stablePolls * cfg.drain.pollIntervalSeconds + " s with no new input)");
            plan.add("Stop message flow " + wf.flowLabel() + " normally (no in-flight unit of work remains)");
        } else {
            plan.add("Message flow " + wf.flowLabel() + " is already stopped - no flow change");
        }
        plan.add("Verify " + wf.queue + " is still empty. The queue's PUT attribute is never changed, so a "
                + "message put after the stop stays safely on the queue; in that case the flow is restarted "
                + "to process it and the shutdown is reported as not completed");
        plan.add("On timeout or error: " + (fs == FlowState.RUNNING ? "restart the flow if stopped" : "no changes to undo"));
        return plan;
    }

    /** Producer flow: stop the producer first, then let the remote consumer empty the queue. */
    private void executeDbToMq(WorkflowDef wf, FlowState fs, long timeoutS)
            throws WorkflowException, InterruptedException {
        if (fs == FlowState.RUNNING) {
            stopFlowAndWait(wf);
        }
        changes.phase = "drain";
        // Stall detection is off: a remote client may legitimately connect only periodically.
        DrainMonitor.Outcome out = drain.await(() -> mq.snapshot(wf.queue), timeoutS * 1000L,
                cfg.drain.pollIntervalSeconds * 1000L, cfg.drain.stablePolls, 0);
        if (out != DrainMonitor.Outcome.DRAINED) {
            throw new WorkflowException(ExitCodes.DRAIN_NOT_COMPLETED,
                    "Queue " + wf.queue + " was not consumed by the remote client: " + out);
        }
    }

    /**
     * Consumer flow: let the flow finish everything during a quiet period, then stop it. Input is not
     * blocked, so the queue is verified again after the stop.
     */
    private void executeMqToDb(WorkflowDef wf, FlowState fs, long timeoutS)
            throws WorkflowException, InterruptedException {
        if (fs == FlowState.RUNNING) {
            changes.phase = "drain";
            DrainMonitor.Outcome out = drain.await(() -> mq.snapshot(wf.queue), timeoutS * 1000L,
                    cfg.drain.pollIntervalSeconds * 1000L, cfg.drain.stablePolls, cfg.drain.stallPolls);
            if (out != DrainMonitor.Outcome.DRAINED) {
                throw new WorkflowException(ExitCodes.DRAIN_NOT_COMPLETED,
                        "Flow did not drain " + wf.queue + " into the database (no quiet period?): " + out);
            }
            stopFlowAndWait(wf);
        }
        changes.phase = "verify";
        long verifyMs = (cfg.drain.stablePolls + 1L) * cfg.drain.pollIntervalSeconds * 1000L;
        DrainMonitor.Outcome out = drain.await(() -> mq.snapshot(wf.queue), verifyMs,
                cfg.drain.pollIntervalSeconds * 1000L, cfg.drain.stablePolls, 0);
        if (out != DrainMonitor.Outcome.DRAINED) {
            throw new WorkflowException(ExitCodes.VERIFY_FAILED, "Queue " + wf.queue + " is not empty after the "
                    + "flow stopped: a client put new messages (they are safe on the queue) or a unit of work "
                    + "backed out");
        }
    }

    // ------------------------------------------------------------------- start

    /** Single workflow: pre-flight, confirm, execute. Throws on failure. */
    public int start(WorkflowDef wf, Options o) throws WorkflowException {
        Prepared p = prepareStart(wf, o);
        if (p.nothingToDo()) {
            return ExitCodes.OK;
        }
        if (o.dryRun) {
            log.info("DRY RUN complete: pre-flight passed, nothing was changed");
            return ExitCodes.OK;
        }
        confirmOne(p, "start", o);
        return executeStart(p, o);
    }

    private Prepared prepareStart(WorkflowDef wf, Options o) throws WorkflowException {
        log.info("=== START %s: pre-flight ===", wf);
        QueueSnapshot q = mq.snapshot(wf.queue);
        FlowState fs = flowState(wf);
        log.info("Pre-flight: flow %s is %s", wf.flowLabel(), fs);
        log.info("Pre-flight: %s", q);
        if (fs == FlowState.UNKNOWN) {
            throw new WorkflowException(ExitCodes.PREFLIGHT_FAILED,
                    "Cannot determine whether flow " + wf.flowLabel() + " is running; refusing to continue");
        }
        if (wf.type == WorkflowType.DB_TO_MQ && q.putInhibited) {
            throw new WorkflowException(ExitCodes.PREFLIGHT_FAILED, "Queue " + wf.queue + " is PUT(DISABLED); "
                    + "the producer flow could not deliver. mqwf never disables queues, so find out who did");
        }
        if (wf.type == WorkflowType.MQ_TO_DB && q.getInhibited) {
            throw new WorkflowException(ExitCodes.PREFLIGHT_FAILED,
                    "Queue " + wf.queue + " is GET(DISABLED); the flow could not consume it");
        }
        if (wf.type == WorkflowType.MQ_TO_DB && q.putInhibited) {
            log.warn("Pre-flight: %s is PUT(DISABLED); remote clients cannot send until someone re-enables it "
                    + "(mqwf does not change queue attributes)", wf.queue);
        }
        List<String> plan = new ArrayList<>();
        if (fs == FlowState.RUNNING) {
            log.info("Nothing to do: workflow %s is already running", wf.name);
        } else {
            plan.add("Start message flow " + wf.flowLabel() + " and wait until it is running");
            logPlan(plan);
        }
        return new Prepared(wf, q, fs, 0, plan);
    }

    private int executeStart(Prepared p, Options o) throws WorkflowException {
        WorkflowDef wf = p.wf;
        changes = new Changes(wf);
        log.info("=== START %s: executing ===", wf.name);
        try {
            startFlowAndWait(wf);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new WorkflowException(ExitCodes.ERROR, "Interrupted", e);
        }
        changes.finished = true;
        log.info("=== START of %s COMPLETE ===", wf.name);
        return ExitCodes.OK;
    }

    // ---------------------------------------------------------------- rollback

    private WorkflowException rollback(WorkflowDef wf, Options o, WorkflowException cause) {
        if (!changes.any()) {
            return cause;
        }
        if (o.noRollback) {
            log.warn("--no-rollback: leaving changes in place: %s", changes.describe());
            log.warn("Re-run 'mqwf shutdown %s' to continue, or 'mqwf start %s' to resume", wf.name, wf.name);
            return cause;
        }
        log.warn("Rolling back changes made by this run: %s", changes.describe());
        changes.phase = "rollback";
        try {
            FlowState s = flowState(wf);
            if (s == FlowState.STOPPED) {
                startFlowAndWait(wf);
            } else if (s == FlowState.RUNNING) {
                log.info("Flow %s is running; nothing to restart", wf.flowLabel());
            } else {
                throw new WorkflowException(ExitCodes.ROLLBACK_FAILED, "flow state is UNKNOWN");
            }
            changes.flowStopRequested = false;
            changes.finished = true;
            log.warn("Rollback complete: workflow %s is back in its original state. Original failure: %s",
                    wf.name, cause.getMessage());
            return cause;
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.error("ROLLBACK FAILED: %s", e.getMessage());
            log.error("MANUAL ACTION REQUIRED for workflow %s. Still in effect: %s", wf.name, changes.describe());
            log.error("Check with 'mqwf status %s', then run 'mqwf start %s' or start flow %s by hand",
                    wf.name, wf.name, wf.flowLabel());
            return new WorkflowException(ExitCodes.ROLLBACK_FAILED,
                    "Rollback failed (" + e.getMessage() + ") after: " + cause.getMessage(), e);
        }
    }

    // ------------------------------------------------------------ flow helpers

    private void stopFlowAndWait(WorkflowDef wf) throws WorkflowException, InterruptedException {
        changes.phase = "stop flow";
        changes.flowStopRequested = true; // set first: an ambiguous failure is still undone
        log.info("Stopping message flow %s", wf.flowLabel());
        try {
            flows.stop(wf);
        } catch (IOException e) {
            throw new WorkflowException(ExitCodes.FLOW_STATE_FAILED, e.getMessage(), e);
        }
        awaitFlowState(wf, FlowState.STOPPED);
    }

    private void startFlowAndWait(WorkflowDef wf) throws WorkflowException, InterruptedException {
        log.info("Starting message flow %s", wf.flowLabel());
        try {
            flows.start(wf);
        } catch (IOException e) {
            throw new WorkflowException(ExitCodes.FLOW_STATE_FAILED, e.getMessage(), e);
        }
        awaitFlowState(wf, FlowState.RUNNING);
    }

    private void awaitFlowState(WorkflowDef wf, FlowState target) throws WorkflowException, InterruptedException {
        long deadline = clock.millis() + cfg.ace.flowStateTimeoutSeconds * 1000L;
        int failures = 0;
        while (true) {
            try {
                FlowState s = flows.state(wf);
                failures = 0;
                if (s == target) {
                    log.info("Message flow %s is %s", wf.flowLabel(), s);
                    return;
                }
            } catch (IOException e) {
                if (++failures >= 3) {
                    throw new WorkflowException(ExitCodes.FLOW_STATE_FAILED, e.getMessage(), e);
                }
                log.warn("Flow state check failed (%d/3): %s", failures, e.getMessage());
            }
            if (clock.millis() >= deadline) {
                throw new WorkflowException(ExitCodes.FLOW_STATE_FAILED, "Message flow " + wf.flowLabel()
                        + " did not become " + target + " within " + cfg.ace.flowStateTimeoutSeconds + " s");
            }
            clock.sleep(cfg.drain.pollIntervalSeconds * 1000L);
        }
    }

    private FlowState flowState(WorkflowDef wf) throws WorkflowException {
        try {
            return flows.state(wf);
        } catch (IOException e) {
            throw new WorkflowException(ExitCodes.ERROR, e.getMessage(), e);
        }
    }
}
