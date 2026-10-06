package com.ops.mqwf;

/** Point-in-time state of a local queue, combined from DISPLAY QLOCAL and DISPLAY QSTATUS. */
public final class QueueSnapshot {
    public final String queue;
    /** CURDEPTH. Note: an MQGET under syncpoint decrements this BEFORE the unit of work commits. */
    public final int depth;
    /** UNCOM: number of uncommitted puts/gets. Must be 0 together with depth for the queue to be empty. */
    public final int uncommitted;
    public final int openInput;
    public final int openOutput;
    public final boolean putInhibited;
    public final boolean getInhibited;
    /** BOQNAME, or null when not set. */
    public final String backoutQueue;

    public QueueSnapshot(String queue, int depth, int uncommitted, int openInput, int openOutput,
            boolean putInhibited, boolean getInhibited, String backoutQueue) {
        this.queue = queue;
        this.depth = depth;
        this.uncommitted = uncommitted;
        this.openInput = openInput;
        this.openOutput = openOutput;
        this.putInhibited = putInhibited;
        this.getInhibited = getInhibited;
        this.backoutQueue = backoutQueue;
    }

    /** True only when there are no committed AND no in-flight (uncommitted) messages. */
    public boolean isEmpty() {
        return depth == 0 && uncommitted == 0;
    }

    @Override
    public String toString() {
        return String.format("%s CURDEPTH=%d UNCOM=%d IPPROCS=%d OPPROCS=%d PUT=%s GET=%s BOQNAME=%s",
                queue, depth, uncommitted, openInput, openOutput,
                putInhibited ? "DISABLED" : "ENABLED", getInhibited ? "DISABLED" : "ENABLED",
                backoutQueue == null ? "-" : backoutQueue);
    }
}
