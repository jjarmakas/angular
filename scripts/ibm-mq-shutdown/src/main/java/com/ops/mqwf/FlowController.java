package com.ops.mqwf;

import java.io.IOException;

/** Starts, stops and inspects an App Connect Enterprise message flow. */
public interface FlowController {

    enum FlowState {
        RUNNING, STOPPED,
        /** State could not be determined. Always treated as unsafe. */
        UNKNOWN
    }

    FlowState state(WorkflowDef wf) throws IOException;

    /**
     * Requests a normal (non-forced) stop. ACE lets each flow instance finish the message it is
     * currently processing, so the in-flight unit of work commits or backs out as a whole.
     */
    void stop(WorkflowDef wf) throws IOException;

    void start(WorkflowDef wf) throws IOException;
}
