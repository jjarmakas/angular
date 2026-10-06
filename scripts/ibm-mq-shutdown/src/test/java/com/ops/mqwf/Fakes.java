package com.ops.mqwf;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** In-memory queue manager, IIB and clock for testing the orchestration without a real MQ/IIB. */
final class Fakes {
    private Fakes() {
    }

    private static final String COMMON = String.join("\n",
            "mq.qmgr=QM1",
            "ace.url=http://localhost:4414",
            "ace.api=apiv1",
            "ace.mode=node",
            "ace.flowStateTimeoutSeconds=30",
            "drain.timeoutSeconds=60",
            "drain.pollIntervalSeconds=1",
            "drain.stablePolls=3",
            "drain.stallPolls=5",
            "");

    /** One workflow "WF" on queue APP.Q and flow "Flow". */
    static Config config(Path dir, String type) throws Exception {
        return write(dir, COMMON + String.join("\n",
                "workflows=WF",
                "workflow.WF.type=" + type,
                "workflow.WF.queue=APP.Q",
                "workflow.WF.server=IS1",
                "workflow.WF.application=App",
                "workflow.WF.flow=Flow"));
    }

    /** Workflows A (MQ_TO_DB, APP.QA, FlowA), B (DB_TO_MQ, APP.QB, FlowB), C (MQ_TO_DB, APP.QC, FlowC). */
    static Config config3(Path dir) throws Exception {
        return write(dir, COMMON + String.join("\n",
                "workflows=A,B,C",
                "workflow.A.type=MQ_TO_DB", "workflow.A.queue=APP.QA", "workflow.A.server=IS1", "workflow.A.flow=FlowA",
                "workflow.B.type=DB_TO_MQ", "workflow.B.queue=APP.QB", "workflow.B.server=IS1", "workflow.B.flow=FlowB",
                "workflow.C.type=MQ_TO_DB", "workflow.C.queue=APP.QC", "workflow.C.server=IS1", "workflow.C.flow=FlowC"));
    }

    private static Config write(Path dir, String props) throws Exception {
        Path file = dir.resolve("mqwf.properties");
        Files.write(file, props.getBytes(StandardCharsets.UTF_8));
        return Config.load(file);
    }

    /** Ordered record of every state-changing call. */
    static final class Events {
        final List<String> list = new ArrayList<>();
    }

    static final class Clock implements DrainMonitor.Clock {
        long now;
        Runnable onSleep = () -> { };

        @Override
        public long millis() {
            return now;
        }

        @Override
        public void sleep(long ms) {
            now += ms;
            onSleep.run();
        }
    }

    /**
     * Flow "Flow" is controlled through the top-level fields (single-workflow tests, events
     * STOP_FLOW / START_FLOW). Any other flow has its own state in {@link #others} and records
     * STOP_FLOW:name / START_FLOW:name.
     */
    static final class Flows implements FlowController {
        final Events events;
        FlowState state = FlowState.RUNNING;
        boolean failStart;
        Runnable onStop = () -> { };
        final Map<String, FlowState> others = new HashMap<>();

        Flows(Events events) {
            this.events = events;
        }

        @Override
        public FlowState state(WorkflowDef wf) {
            return wf.flow.equals("Flow") ? state : others.getOrDefault(wf.flow, FlowState.RUNNING);
        }

        @Override
        public void stop(WorkflowDef wf) {
            if (wf.flow.equals("Flow")) {
                onStop.run();
                events.list.add("STOP_FLOW");
                state = FlowState.STOPPED;
            } else {
                events.list.add("STOP_FLOW:" + wf.flow);
                others.put(wf.flow, FlowState.STOPPED);
            }
        }

        @Override
        public void start(WorkflowDef wf) throws IOException {
            if (failStart) {
                throw new IOException("IIB unavailable");
            }
            if (wf.flow.equals("Flow")) {
                events.list.add("START_FLOW");
                state = FlowState.RUNNING;
            } else {
                events.list.add("START_FLOW:" + wf.flow);
                others.put(wf.flow, FlowState.RUNNING);
            }
        }
    }

    /** State of one queue. */
    static class QState {
        int depth;
        int uncommitted;
        int openInput = 1;
        boolean putInhibited;
        boolean getInhibited;
        String boq;
        int boqDepth;
    }

    /** Queue APP.Q is the top-level fields; any other queue gets its own state in {@link #others}. */
    static final class Queue extends QState implements QueueAdmin {
        final Events events;
        final Map<String, QState> others = new HashMap<>();

        Queue(Events events) {
            this.events = events;
        }

        QState get(String queue) {
            return queue.equals("APP.Q") ? this : others.computeIfAbsent(queue, k -> new QState());
        }

        @Override
        public QueueSnapshot snapshot(String queue) {
            QState s = get(queue);
            return new QueueSnapshot(queue, s.depth, s.uncommitted, s.openInput, 0, s.putInhibited, s.getInhibited, s.boq);
        }

        @Override
        public Integer depthOrNull(String queue) {
            return boqDepth;
        }
    }
}
