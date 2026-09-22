package com.mqarchiver;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Loads and holds the settings from the external archiver.properties file:
 * queue manager, queue list, message-count limits, archive location and
 * the dmpmqmsg command to use.
 */
public class ArchiverConfig {

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

    public static ArchiverConfig load(String configFilePath) throws IOException {
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

    public String getQmgrName() {
        return qmgrName;
    }

    public String getArchiveBasePath() {
        return archiveBasePath;
    }

    public List<String> getQueueNames() {
        return queueNames;
    }

    public int getLimitForQueue(String queueName) {
        Integer override = perQueueLimits.get(queueName);
        return override != null ? override : defaultMessageLimit;
    }

    public String getDmpmqmsgPath() {
        return dmpmqmsgPath;
    }

    public int getDmpmqmsgTimeoutSeconds() {
        return dmpmqmsgTimeoutSeconds;
    }
}
