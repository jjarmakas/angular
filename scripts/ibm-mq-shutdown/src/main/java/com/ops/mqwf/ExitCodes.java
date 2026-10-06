package com.ops.mqwf;

/** Process exit codes. Stable: monitoring and job schedulers may rely on them. */
public final class ExitCodes {
    /** Requested operation completed and was verified. */
    public static final int OK = 0;
    /** Bad command line or configuration. Nothing was changed. */
    public static final int USAGE = 1;
    /** A pre-flight safety check failed. Nothing was changed. */
    public static final int PREFLIGHT_FAILED = 2;
    /** Queue did not drain in time (or stalled). Changes were rolled back unless --no-rollback. */
    public static final int DRAIN_NOT_COMPLETED = 3;
    /** The message flow did not reach the expected state. */
    public static final int FLOW_STATE_FAILED = 4;
    /** Post-shutdown verification found messages on the queue. Changes were rolled back. */
    public static final int VERIFY_FAILED = 5;
    /** Another instance holds the lock for this workflow. Nothing was changed. */
    public static final int LOCKED = 6;
    /** Operator declined the confirmation. Nothing was changed. */
    public static final int NOT_CONFIRMED = 7;
    /** Completed, but with warnings that need attention (e.g. messages moved to the backout queue). */
    public static final int OK_WITH_WARNINGS = 8;
    /** Unexpected MQ / REST / I/O error. */
    public static final int ERROR = 9;
    /** Rollback failed: the workflow is in a partially changed state and needs manual action. */
    public static final int ROLLBACK_FAILED = 10;

    /** Least to most severe; a multi-workflow run exits with the most severe code of its workflows. */
    private static final int[] SEVERITY = {
        OK, OK_WITH_WARNINGS, NOT_CONFIRMED, LOCKED, USAGE, PREFLIGHT_FAILED,
        DRAIN_NOT_COMPLETED, FLOW_STATE_FAILED, VERIFY_FAILED, ERROR, ROLLBACK_FAILED };

    private ExitCodes() {
    }

    public static int worst(int a, int b) {
        return rank(a) >= rank(b) ? a : b;
    }

    private static int rank(int code) {
        for (int i = 0; i < SEVERITY.length; i++) {
            if (SEVERITY[i] == code) {
                return i;
            }
        }
        return SEVERITY.length;
    }
}
