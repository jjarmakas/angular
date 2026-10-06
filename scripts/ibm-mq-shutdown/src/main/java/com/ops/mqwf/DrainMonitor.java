package com.ops.mqwf;

/**
 * Waits until a queue is truly empty: CURDEPTH = 0 AND UNCOM = 0 on several consecutive polls.
 * <p>
 * Checking CURDEPTH alone is not safe: a destructive MQGET under syncpoint decrements CURDEPTH
 * immediately, while the database insert in the same unit of work may still be uncommitted. If that
 * unit of work backs out, the message reappears. UNCOM exposes exactly that in-flight window.
 */
public final class DrainMonitor {

    public enum Outcome {
        DRAINED,
        TIMED_OUT,
        /** Messages are waiting but nothing has the queue open for input. */
        STALLED
    }

    public interface Probe {
        QueueSnapshot read() throws WorkflowException;
    }

    public interface Clock {
        long millis();

        void sleep(long millis) throws InterruptedException;
    }

    public static final Clock SYSTEM_CLOCK = new Clock() {
        @Override
        public long millis() {
            return System.nanoTime() / 1_000_000L;
        }

        @Override
        public void sleep(long millis) throws InterruptedException {
            Thread.sleep(millis);
        }
    };

    private static final long PROGRESS_LOG_MS = 60_000L;

    private final AuditLog log;
    private final Clock clock;

    public DrainMonitor(AuditLog log, Clock clock) {
        this.log = log;
        this.clock = clock;
    }

    /**
     * @param stablePolls consecutive empty readings required (protects against a backout re-queuing a message)
     * @param stallPolls  consecutive "messages but no consumer" readings before giving up; 0 disables
     */
    public Outcome await(Probe probe, long timeoutMs, long intervalMs, int stablePolls, int stallPolls)
            throws WorkflowException, InterruptedException {
        long deadline = clock.millis() + timeoutMs;
        long lastProgress = Long.MIN_VALUE;
        String last = null;
        int stable = 0;
        int stalled = 0;
        while (true) {
            QueueSnapshot s = probe.read();
            String line = s.toString();
            long now = clock.millis();
            if (!line.equals(last) || now - lastProgress >= PROGRESS_LOG_MS) {
                log.info("Drain: %s", line);
                last = line;
                lastProgress = now;
            }

            stable = s.isEmpty() ? stable + 1 : 0;
            if (stable >= stablePolls) {
                log.info("Queue %s confirmed empty on %d consecutive checks", s.queue, stable);
                return Outcome.DRAINED;
            }

            stalled = (!s.isEmpty() && s.openInput == 0) ? stalled + 1 : 0;
            if (stallPolls > 0 && stalled >= stallPolls) {
                log.warn("Queue %s holds messages but no consumer has had it open for %d checks", s.queue, stalled);
                return Outcome.STALLED;
            }

            if (now >= deadline) {
                log.warn("Queue %s did not drain within %d s: %s", s.queue, timeoutMs / 1000, line);
                return Outcome.TIMED_OUT;
            }
            clock.sleep(Math.min(intervalMs, Math.max(0, deadline - now)));
        }
    }
}
