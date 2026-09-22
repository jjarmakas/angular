package com.mqarchiver;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.CMQC;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * For every queue in the configuration, checks the current depth against the
 * configured limit and, if it is exceeded, moves all messages on that queue
 * into Archive_YYYYMMDD/&lt;queueName&gt;.txt using the dmpmqmsg utility
 * (destructive get, so messages are removed from the queue as they are
 * dumped).
 */
public class QueueArchiver {

    private static final Logger LOGGER = Logger.getLogger(QueueArchiver.class.getName());

    private final ArchiverConfig config;

    public QueueArchiver(ArchiverConfig config) {
        this.config = config;
    }

    public void archiveQueuesExceedingLimit() {
        for (String queueName : config.getQueueNames()) {
            try {
                int limit = config.getLimitForQueue(queueName);
                int depth = getCurrentDepth(queueName);
                LOGGER.info("Queue " + queueName + ": depth=" + depth + ", limit=" + limit);

                if (depth > limit) {
                    archiveQueue(queueName, depth);
                }
            } catch (Exception e) {
                // One queue failing must not stop the others from being checked/archived.
                LOGGER.log(Level.SEVERE, "Failed to process queue '" + queueName + "'", e);
            }
        }
    }

    private int getCurrentDepth(String queueName) throws MQException {
        MQQueueManager qmgr = new MQQueueManager(config.getQmgrName());
        try {
            MQQueue queue = qmgr.accessQueue(queueName, CMQC.MQOO_INQUIRE);
            try {
                return queue.getCurrentDepth();
            } finally {
                queue.close();
            }
        } finally {
            qmgr.disconnect();
        }
    }

    private void archiveQueue(String queueName, int depth) throws IOException, InterruptedException {
        File archiveDir = resolveArchiveDir();
        if (!archiveDir.exists() && !archiveDir.mkdirs() && !archiveDir.exists()) {
            throw new IOException("Unable to create archive directory: " + archiveDir.getAbsolutePath());
        }

        File targetFile = new File(archiveDir, queueName + ".txt");
        File dumpFile = new File(archiveDir, "." + queueName + "_" + UUID.randomUUID() + ".tmp");

        try {
            runDmpmqmsg(queueName, dumpFile);
            appendFile(dumpFile, targetFile);
            LOGGER.info("Archived " + depth + " message(s) from queue '" + queueName + "' to "
                    + targetFile.getAbsolutePath());
        } finally {
            Files.deleteIfExists(dumpFile.toPath());
        }
    }

    /**
     * Shells out to the dmpmqmsg utility shipped with IBM MQ. "-I" performs a
     * destructive get (each message is removed from the queue as it is
     * dumped), "-f" writes the formatted dump to a file. Flag set confirmed
     * against IBM MQ 9.x documentation for dmpmqmsg; if your MQ 9.2.3.0
     * install reports different flags for "dmpmqmsg -?", update this method
     * or the dmpmqmsgPath/extra-args handling accordingly.
     */
    private void runDmpmqmsg(String queueName, File dumpFile) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add(config.getDmpmqmsgPath());
        command.add("-m");
        command.add(config.getQmgrName());
        command.add("-I");
        command.add(queueName);
        command.add("-f");
        command.add(dumpFile.getAbsolutePath());
        command.add("-q");

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        Process process = pb.start();

        StringBuilder processOutput = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                processOutput.append(line).append(System.lineSeparator());
            }
        }

        boolean finished = process.waitFor(config.getDmpmqmsgTimeoutSeconds(), TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
            throw new IOException("dmpmqmsg timed out after " + config.getDmpmqmsgTimeoutSeconds()
                    + "s for queue '" + queueName + "'");
        }

        int exitCode = process.exitValue();
        if (exitCode != 0) {
            throw new IOException("dmpmqmsg failed for queue '" + queueName + "' (exit code " + exitCode + "): "
                    + processOutput);
        }
    }

    private void appendFile(File source, File target) throws IOException {
        try (InputStream in = new FileInputStream(source);
                OutputStream out = new FileOutputStream(target, true)) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) != -1) {
                out.write(buffer, 0, read);
            }
        }
    }

    private File resolveArchiveDir() {
        String today = new SimpleDateFormat("yyyyMMdd").format(new Date());
        return new File(config.getArchiveBasePath(), "Archive_" + today);
    }
}
