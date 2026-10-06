package com.ops.mqwf;

/** A controlled failure that maps to a specific process exit code. */
public class WorkflowException extends Exception {
    private static final long serialVersionUID = 1L;

    private final int exitCode;

    public WorkflowException(int exitCode, String message) {
        super(message);
        this.exitCode = exitCode;
    }

    public WorkflowException(int exitCode, String message, Throwable cause) {
        super(message, cause);
        this.exitCode = exitCode;
    }

    public int getExitCode() {
        return exitCode;
    }
}
