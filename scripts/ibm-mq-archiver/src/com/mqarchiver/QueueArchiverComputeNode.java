package com.mqarchiver;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.MbElement;
import com.ibm.broker.plugin.MbException;
import com.ibm.broker.plugin.MbMessage;
import com.ibm.broker.plugin.MbMessageAssembly;
import com.ibm.broker.plugin.MbOutputTerminal;
import com.ibm.broker.plugin.MbUserException;

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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * JavaCompute node that checks the depth of every queue listed in the
 * archiver configuration file and, for any queue whose depth exceeds its
 * configured limit, moves all its messages into
 * &lt;archiveBasePath&gt;/Archive_YYYYMMDD/&lt;queueName&gt;.txt.
 *
 * A per-queue success/failure log line is produced for every run and sent
 * out the "log" terminal to a downstream FileOutput node (see README.md for
 * flow wiring); it is not just written to the broker log.
 *
 * This node does not act on the content of the message flowing through it -
 * it is meant to be driven by a TimeoutNotification/TimeoutControl input
 * node on a schedule (e.g. every few minutes), not by application traffic.
 */
public class QueueArchiverComputeNode extends MbJavaComputeNode {

    private static final Logger LOGGER = Logger.getLogger(QueueArchiverComputeNode.class.getName());

    private static final String CONFIG_PATH_SYSTEM_PROPERTY = "mqarchiver.configPath";
    private static final String CONFIG_PATH_ENV_VAR = "MQARCHIVER_CONFIG_PATH";
    private static final String DEFAULT_CONFIG_PATH = "mqarchiver/archiver.properties";

    private static final String LOG_FILE_NAME = "archiver.log";

    public void evaluate(MbMessageAssembly inAssembly) throws MbException {
        MbOutputTerminal outTerminal = getOutputTerminal("out");
        MbOutputTerminal logTerminal = getOutputTerminal("log");

        try {
            ArchiverConfig config = ArchiverConfig.load(resolveConfigFilePath());
            List<String> logLines = new QueueArchiver(config).archiveQueuesExceedingLimit();
            propagateLog(logTerminal, config, logLines);

            MbMessage outMessage = new MbMessage(inAssembly.getMessage());
            MbMessageAssembly outAssembly = new MbMessageAssembly(inAssembly, outMessage);
            outTerminal.propagate(outAssembly);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Unable to load archiver configuration from " + resolveConfigFilePath(), e);
            throw new MbUserException(this, "evaluate()", "", "",
                    "Unable to load archiver configuration: " + e.getMessage(), null);
        } catch (MbException e) {
            throw e;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Unexpected error while archiving queues", e);
            throw new MbUserException(this, "evaluate()", "", "",
                    "Unexpected error while archiving queues: " + e.getMessage(), null);
        }
    }

    /**
     * Builds a BLOB message out of this run's per-queue success/failure log
     * lines and propagates it out the "log" terminal, with Local Environment
     * set so a downstream FileOutput node writes it to
     * &lt;archiveBasePath&gt;/Archive_YYYYMMDD/archiver.log (appended).
     */
    private void propagateLog(MbOutputTerminal logTerminal, ArchiverConfig config, List<String> logLines)
            throws MbException {
        if (logLines.isEmpty()) {
            return;
        }

        String logText = String.join(System.lineSeparator(), logLines) + System.lineSeparator();
        byte[] logBytes = logText.getBytes(StandardCharsets.UTF_8);

        MbMessage logMessage = new MbMessage();
        MbElement logRoot = logMessage.getRootElement();
        MbElement logBody = logRoot.createElementAsLastChild(MbElement.TYPE_NAME, "BLOB", null);
        logBody.createElementAsFirstChild(MbElement.TYPE_NAME_VALUE, "BLOB", logBytes);

        MbMessage localEnv = new MbMessage();
        MbElement destination = localEnv.getRootElement().createElementAsLastChild(MbElement.TYPE_NAME,
                "Destination", null);
        MbElement file = destination.createElementAsLastChild(MbElement.TYPE_NAME, "File", null);
        file.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Name", LOG_FILE_NAME);
        file.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Directory", logDirectory(config));
        file.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Append", Boolean.TRUE);

        logTerminal.propagate(new MbMessageAssembly(logMessage, localEnv));
    }

    private String logDirectory(ArchiverConfig config) {
        String today = new SimpleDateFormat("yyyyMMdd").format(new Date());
        return config.getArchiveBasePath() + "/Archive_" + today;
    }

    /**
     * Resolves the archiver.properties location: a JVM system property
     * (set via the integration server's JVM properties), falling back to an
     * environment variable, falling back to a path relative to the
     * integration server's work directory.
     */
    private String resolveConfigFilePath() {
        String path = System.getProperty(CONFIG_PATH_SYSTEM_PROPERTY);
        if (path == null || path.trim().isEmpty()) {
            path = System.getenv(CONFIG_PATH_ENV_VAR);
        }
        if (path == null || path.trim().isEmpty()) {
            path = DEFAULT_CONFIG_PATH;
        }
        return path.trim();
    }
}

/**
 * Loads and holds the settings from the external archiver.properties file:
 * queue manager, queue list, message-count limits, archive location and
 * the dmpmqmsg command to use.
 */
class ArchiverConfig {

    private final String qmgrName;
    private final String archiveBasePath;
    private final int defaultMessageLimit;
    private final List<String> queueNames;
    private final Map<String, Integer> perQueueLimits;
    private final String dmpmqmsgPath;
    private final int dmpmqmsgTimeoutSeconds;

    private ArchiverConfig(String qmgrName, String archiveBasePath, int defaultMessageLimit,
            List<String> queueNames, Map<String, Integer> perQueueLimits, String dmpmqmsgPath,
            int dmpmqmsgTimeoutSeconds) {
        this.qmgrName = qmgrName;
        this.archiveBasePath = archiveBasePath;
        this.defaultMessageLimit = defaultMessageLimit;
        this.queueNames = queueNames;
        this.perQueueLimits = perQueueLimits;
        this.dmpmqmsgPath = dmpmqmsgPath;
        this.dmpmqmsgTimeoutSeconds = dmpmqmsgTimeoutSeconds;
    }

    static ArchiverConfig load(String configFilePath) throws IOException {
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(configFilePath)) {
            props.load(in);
        }

        String qmgrName = requireProperty(props, "qmgrName");
        String archiveBasePath = requireProperty(props, "archiveBasePath");
        int defaultLimit = parsePositiveInt(props.getProperty("messageLimit", "1000"), "messageLimit");

        List<String> queueNames = new ArrayList<>();
        for (String q : requireProperty(props, "queues").split(",")) {
            String trimmed = q.trim();
            if (!trimmed.isEmpty()) {
                queueNames.add(trimmed);
            }
        }
        if (queueNames.isEmpty()) {
            throw new IOException("Property 'queues' does not contain any queue names");
        }

        Map<String, Integer> perQueueLimits = new HashMap<>();
        for (String queueName : queueNames) {
            String override = props.getProperty("queue." + queueName + ".limit");
            if (override != null && !override.trim().isEmpty()) {
                perQueueLimits.put(queueName, parsePositiveInt(override, "queue." + queueName + ".limit"));
            }
        }

        String dmpmqmsgPath = props.getProperty("dmpmqmsgPath", "dmpmqmsg").trim();
        int dmpmqmsgTimeoutSeconds = parsePositiveInt(props.getProperty("dmpmqmsgTimeoutSeconds", "300"),
                "dmpmqmsgTimeoutSeconds");

        return new ArchiverConfig(qmgrName, archiveBasePath, defaultLimit, queueNames, perQueueLimits,
                dmpmqmsgPath, dmpmqmsgTimeoutSeconds);
    }

    private static String requireProperty(Properties props, String key) throws IOException {
        String value = props.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IOException("Missing required property '" + key + "' in archiver configuration file");
        }
        return value.trim();
    }

    private static int parsePositiveInt(String value, String propertyName) throws IOException {
        try {
            int result = Integer.parseInt(value.trim());
            if (result < 0) {
                throw new NumberFormatException();
            }
            return result;
        } catch (NumberFormatException e) {
            throw new IOException("Property '" + propertyName + "' must be a non-negative integer, was: " + value);
        }
    }

    String getQmgrName() {
        return qmgrName;
    }

    String getArchiveBasePath() {
        return archiveBasePath;
    }

    List<String> getQueueNames() {
        return queueNames;
    }

    int getLimitForQueue(String queueName) {
        Integer override = perQueueLimits.get(queueName);
        return override != null ? override : defaultMessageLimit;
    }

    String getDmpmqmsgPath() {
        return dmpmqmsgPath;
    }

    int getDmpmqmsgTimeoutSeconds() {
        return dmpmqmsgTimeoutSeconds;
    }
}

/**
 * For every queue in the configuration, checks the current depth against the
 * configured limit and, if it is exceeded, moves all messages on that queue
 * into Archive_YYYYMMDD/&lt;queueName&gt;.txt using the dmpmqmsg utility
 * (destructive get, so messages are removed from the queue as they are
 * dumped). Returns one success/failure log line per queue, formatted for the
 * downstream FileOutput node rather than just written to the broker log.
 */
class QueueArchiver {

    private static final SimpleDateFormat LOG_TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    private final ArchiverConfig config;

    QueueArchiver(ArchiverConfig config) {
        this.config = config;
    }

    List<String> archiveQueuesExceedingLimit() {
        List<String> logLines = new ArrayList<>();
        for (String queueName : config.getQueueNames()) {
            try {
                int limit = config.getLimitForQueue(queueName);
                int depth = getCurrentDepth(queueName);

                if (depth > limit) {
                    archiveQueue(queueName, depth);
                    logLines.add(logLine("SUCCESS", queueName,
                            "depth=" + depth + " limit=" + limit + " archived and removed from queue"));
                } else {
                    logLines.add(logLine("OK", queueName,
                            "depth=" + depth + " limit=" + limit + " (below limit, not archived)"));
                }
            } catch (Exception e) {
                // One queue failing must not stop the others from being checked/archived.
                logLines.add(logLine("FAILURE", queueName, e.getMessage() != null ? e.getMessage() : e.toString()));
            }
        }
        return logLines;
    }

    private String logLine(String status, String queueName, String detail) {
        String timestamp;
        synchronized (LOG_TIMESTAMP_FORMAT) {
            timestamp = LOG_TIMESTAMP_FORMAT.format(new Date());
        }
        return timestamp + " " + status + " queue=" + queueName + " " + detail;
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
