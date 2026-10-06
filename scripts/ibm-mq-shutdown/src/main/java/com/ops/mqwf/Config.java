package com.ops.mqwf;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Loads and strictly validates the properties file. Any problem is reported before a connection is
 * made, so a typo can never lead to acting on the wrong object.
 */
public final class Config {
    private static final Pattern MQ_NAME = Pattern.compile("[A-Za-z0-9._/%]{1,48}");
    private static final Pattern WORKFLOW_NAME = Pattern.compile("[A-Za-z0-9_.-]{1,64}");
    private static final Pattern ACE_NAME = Pattern.compile("[^/\\\\?#%\\s][^/\\\\?#%]{0,254}");

    public static final class Mq {
        public String qmgr;
        public boolean client;
        public String host;
        public int port;
        public String channel;
        public String sslCipherSuite;
        public String user;
        public String password;
    }

    public static final class Ace {
        public String url;
        /** true = IIB v10 REST API (/apiv1), false = ACE v11/v12 REST API (/apiv2). */
        public boolean apiV1;
        public boolean nodeMode;
        public String user;
        public String password;
        public int httpTimeoutSeconds;
        public int flowStateTimeoutSeconds;
    }

    public static final class Drain {
        public int timeoutSeconds;
        public int pollIntervalSeconds;
        public int stablePolls;
        public int stallPolls;
    }

    public final Mq mq = new Mq();
    public final Ace ace = new Ace();
    public final Drain drain = new Drain();
    public Path auditDir;
    public Path lockDir;
    private final Map<String, WorkflowDef> workflows = new LinkedHashMap<>();

    private final Properties props;
    private final Path source;

    private Config(Properties props, Path source) {
        this.props = props;
        this.source = source;
    }

    public Map<String, WorkflowDef> workflows() {
        return Collections.unmodifiableMap(workflows);
    }

    public WorkflowDef workflow(String name) throws WorkflowException {
        WorkflowDef wf = workflows.get(name);
        if (wf == null) {
            throw new WorkflowException(ExitCodes.USAGE,
                    "Unknown workflow '" + name + "'. Configured: " + workflows.keySet());
        }
        return wf;
    }

    public static Config load(Path file) throws WorkflowException {
        Properties p = new Properties();
        try (Reader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            p.load(r);
        } catch (IOException e) {
            throw new WorkflowException(ExitCodes.USAGE, "Cannot read configuration " + file + ": " + e, e);
        }
        Config c = new Config(p, file);
        c.parse();
        return c;
    }

    private void parse() throws WorkflowException {
        mq.qmgr = required("mq.qmgr", MQ_NAME);
        String conn = optional("mq.connection", "bindings");
        if (!conn.equals("bindings") && !conn.equals("client")) {
            throw invalid("mq.connection", "must be 'bindings' or 'client'");
        }
        mq.client = conn.equals("client");
        if (mq.client) {
            mq.host = required("mq.host", null);
            mq.port = intValue("mq.port", 1414, 1, 65535);
            mq.channel = required("mq.channel", Pattern.compile("[A-Za-z0-9._/%]{1,20}"));
            mq.sslCipherSuite = optional("mq.sslCipherSuite", null);
        }
        mq.user = optional("mq.user", null);
        mq.password = secret("mq.password.env", mq.user != null);

        ace.url = required("ace.url", Pattern.compile("https?://[^\\s/?#]+"));
        String api = required("ace.api", Pattern.compile("apiv1|apiv2"));
        ace.apiV1 = api.equals("apiv1");
        String mode = required("ace.mode", Pattern.compile("node|server"));
        ace.nodeMode = mode.equals("node");
        if (ace.apiV1 && !ace.nodeMode) {
            throw invalid("ace.mode", "must be 'node' with ace.api=apiv1 (IIB v10 has no standalone integration servers)");
        }
        ace.user = optional("ace.user", null);
        ace.password = secret("ace.password.env", ace.user != null);
        ace.httpTimeoutSeconds = intValue("ace.httpTimeoutSeconds", 120, 5, 3600);
        ace.flowStateTimeoutSeconds = intValue("ace.flowStateTimeoutSeconds", 300, 10, 7200);

        drain.timeoutSeconds = intValue("drain.timeoutSeconds", 1800, 10, 86400);
        drain.pollIntervalSeconds = intValue("drain.pollIntervalSeconds", 5, 1, 300);
        drain.stablePolls = intValue("drain.stablePolls", 3, 2, 100);
        drain.stallPolls = intValue("drain.stallPolls", 12, 2, 1000);

        Path base = source.toAbsolutePath().getParent();
        auditDir = base.resolve(optional("audit.dir", "logs"));
        lockDir = base.resolve(optional("lock.dir", "locks"));

        parseWorkflows();
    }

    /**
     * The "workflows" property is the authoritative, ordered list. Every listed workflow must be
     * defined and every defined workflow must be listed, so a typo can never silently drop one.
     */
    private void parseWorkflows() throws WorkflowException {
        TreeSet<String> defined = new TreeSet<>();
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("workflow.")) {
                int dot = key.indexOf('.', "workflow.".length());
                if (dot < 0) {
                    throw invalid(key, "expected workflow.<NAME>.<attribute>");
                }
                defined.add(key.substring("workflow.".length(), dot));
            }
        }
        List<String> listed = new ArrayList<>();
        for (String n : required("workflows", null).split(",")) {
            String name = n.trim();
            if (name.isEmpty()) {
                continue;
            }
            if (!WORKFLOW_NAME.matcher(name).matches()) {
                throw invalid("workflows", "workflow name '" + name + "' may only contain letters, digits, '_', '.', '-'");
            }
            if (listed.contains(name)) {
                throw invalid("workflows", "lists " + name + " more than once");
            }
            listed.add(name);
        }
        if (listed.isEmpty()) {
            throw invalid("workflows", "lists no workflows");
        }
        TreeSet<String> unlisted = new TreeSet<>(defined);
        unlisted.removeAll(listed);
        if (!unlisted.isEmpty()) {
            throw invalid("workflows", "does not list " + unlisted + ", which have workflow.<NAME>.* entries");
        }
        List<String> undefined = new ArrayList<>(listed);
        undefined.removeAll(defined);
        if (!undefined.isEmpty()) {
            throw invalid("workflows", "lists " + undefined + ", which have no workflow.<NAME>.* entries");
        }

        Map<String, String> queueOwner = new HashMap<>();
        Map<String, String> flowOwner = new HashMap<>();
        for (String name : listed) {
            String k = "workflow." + name + ".";
            WorkflowType type;
            try {
                type = WorkflowType.valueOf(required(k + "type", null));
            } catch (IllegalArgumentException e) {
                throw invalid(k + "type", "must be DB_TO_MQ or MQ_TO_DB");
            }
            String queue = required(k + "queue", MQ_NAME);
            String server = ace.nodeMode ? required(k + "server", ACE_NAME) : null;
            String app = optional(k + "application", null);
            if (app != null && !ACE_NAME.matcher(app).matches()) {
                throw invalid(k + "application", "invalid name");
            }
            String flow = required(k + "flow", ACE_NAME);
            int timeout = intValue(k + "drainTimeoutSeconds", drain.timeoutSeconds, 10, 86400);

            WorkflowDef wf = new WorkflowDef(name, type, queue, server, app, flow, timeout);
            // Two workflows on one queue or one flow would let one shutdown silently break the other.
            String prevQ = queueOwner.put(queue, name);
            if (prevQ != null) {
                throw invalid(k + "queue", "queue " + queue + " is also used by workflow " + prevQ);
            }
            String prevF = flowOwner.put(wf.flowLabel(), name);
            if (prevF != null) {
                throw invalid(k + "flow", "flow " + wf.flowLabel() + " is also used by workflow " + prevF);
            }
            workflows.put(name, wf);
        }
    }

    private String optional(String key, String def) {
        String v = props.getProperty(key);
        if (v == null) {
            return def;
        }
        v = v.trim();
        return v.isEmpty() ? def : v;
    }

    private String required(String key, Pattern pattern) throws WorkflowException {
        String v = optional(key, null);
        if (v == null) {
            throw invalid(key, "is required");
        }
        if (pattern != null && !pattern.matcher(v).matches()) {
            throw invalid(key, "has invalid value '" + v + "'");
        }
        return v;
    }

    private int intValue(String key, int def, int min, int max) throws WorkflowException {
        String v = optional(key, null);
        if (v == null) {
            return def;
        }
        try {
            int i = Integer.parseInt(v);
            if (i < min || i > max) {
                throw invalid(key, "must be between " + min + " and " + max);
            }
            return i;
        } catch (NumberFormatException e) {
            throw invalid(key, "must be an integer");
        }
    }

    /** Passwords are only ever taken from environment variables, never from the file. */
    private String secret(String envKey, boolean needed) throws WorkflowException {
        if (props.getProperty(envKey.replace(".env", "")) != null) {
            throw invalid(envKey.replace(".env", ""),
                    "plain-text passwords are not allowed; set " + envKey + "=<ENV_VAR_NAME> instead");
        }
        String var = optional(envKey, null);
        if (var == null) {
            if (needed) {
                throw invalid(envKey, "is required when a user is configured");
            }
            return null;
        }
        String value = System.getenv(var);
        if (value == null || value.isEmpty()) {
            throw invalid(envKey, "environment variable " + var + " is not set");
        }
        return value;
    }

    private WorkflowException invalid(String key, String why) {
        return new WorkflowException(ExitCodes.USAGE, "Configuration " + source.getFileName() + ": " + key + " " + why);
    }

    public static Path defaultPath() {
        return Paths.get("mqwf.properties");
    }
}
