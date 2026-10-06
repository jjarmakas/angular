package com.ops.mqwf;

/** One configured workflow: an ACE message flow plus the local queue it writes to or reads from. */
public final class WorkflowDef {
    public final String name;
    public final WorkflowType type;
    public final String queue;
    /** Integration server name (required when ACE is an integration node). */
    public final String server;
    /** Application containing the flow; null for an independent flow. */
    public final String application;
    public final String flow;
    /** Per-workflow drain timeout; overrides drain.timeoutSeconds. */
    public final int drainTimeoutSeconds;

    public WorkflowDef(String name, WorkflowType type, String queue, String server, String application,
            String flow, int drainTimeoutSeconds) {
        this.name = name;
        this.type = type;
        this.queue = queue;
        this.server = server;
        this.application = application;
        this.flow = flow;
        this.drainTimeoutSeconds = drainTimeoutSeconds;
    }

    public String flowLabel() {
        StringBuilder sb = new StringBuilder();
        if (server != null) {
            sb.append(server).append('/');
        }
        if (application != null) {
            sb.append(application).append('/');
        }
        return sb.append(flow).toString();
    }

    @Override
    public String toString() {
        return name + " [" + type + ", queue=" + queue + ", flow=" + flowLabel() + "]";
    }
}
