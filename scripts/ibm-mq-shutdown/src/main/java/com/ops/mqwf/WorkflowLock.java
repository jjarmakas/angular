package com.ops.mqwf;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * OS-level exclusive lock per workflow so two operators (or an operator and a scheduled job) cannot
 * drive the same workflow at once. The OS releases the lock automatically if the process dies, so
 * there are no stale locks to clean up.
 */
public final class WorkflowLock implements Orchestrator.Lock {
    private final FileChannel channel;
    private final FileLock lock;

    private WorkflowLock(FileChannel channel, FileLock lock) {
        this.channel = channel;
        this.lock = lock;
    }

    public static WorkflowLock acquire(Path dir, String workflow, String owner) throws WorkflowException {
        try {
            Files.createDirectories(dir);
            FileChannel ch = FileChannel.open(dir.resolve(workflow + ".lock"),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            FileLock l;
            try {
                l = ch.tryLock();
            } catch (OverlappingFileLockException e) {
                l = null;
            }
            if (l == null) {
                ch.close();
                throw new WorkflowException(ExitCodes.LOCKED,
                        "Workflow " + workflow + " is being operated on by another mqwf process; nothing was changed");
            }
            ch.truncate(0);
            ch.write(java.nio.ByteBuffer.wrap(owner.getBytes(StandardCharsets.UTF_8)));
            ch.force(true);
            return new WorkflowLock(ch, l);
        } catch (IOException e) {
            throw new WorkflowException(ExitCodes.ERROR, "Cannot create lock file in " + dir + ": " + e, e);
        }
    }

    @Override
    public void close() {
        try {
            lock.release();
        } catch (IOException ignored) {
            // released by the OS on exit anyway
        }
        try {
            channel.close();
        } catch (IOException ignored) {
            // best effort
        }
    }
}
