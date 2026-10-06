package com.ops.mqwf;

/** The read-only queue manager inquiries the orchestrator needs. Implemented by {@link MqAdmin}. */
public interface QueueAdmin {

    QueueSnapshot snapshot(String queue) throws WorkflowException;

    /** Depth of a local queue, or null if it cannot be read. Never throws. */
    Integer depthOrNull(String queue);
}
