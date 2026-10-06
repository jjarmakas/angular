package com.ops.mqwf;

import java.io.Console;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import com.ibm.mq.MQException;

/**
 * mqwf - safe shutdown / start of IBM MQ + IIB / App Connect Enterprise workflows.
 *
 * <pre>
 * mqwf [--config FILE] list
 * mqwf [--config FILE] status   [WORKFLOW...]
 * mqwf [--config FILE] shutdown [WORKFLOW...] [--dry-run] [--yes] [--no-rollback] [--timeout SECONDS]
 * mqwf [--config FILE] start    [WORKFLOW...] [--dry-run] [--yes]
 * </pre>
 * Without WORKFLOW names, every workflow in the "workflows" property is processed, in listed order.
 * Workflows are independent and handled one at a time; a failure in one does not affect the others.
 */
public final class Main {
    private static final String USAGE = String.join(System.lineSeparator(),
            "Usage:",
            "  mqwf [--config FILE] list",
            "  mqwf [--config FILE] status   [WORKFLOW...]",
            "  mqwf [--config FILE] shutdown [WORKFLOW...] [--dry-run] [--yes] [--no-rollback] [--timeout SECONDS]",
            "  mqwf [--config FILE] start    [WORKFLOW...] [--dry-run] [--yes]",
            "",
            "  WORKFLOW...    workflows to process, one at a time, in the given order.",
            "                 Default: all workflows in the 'workflows' property, in listed order",
            "  --dry-run      run the pre-flight checks and print the plans; change nothing",
            "  --yes          do not ask for confirmation (for scheduled jobs)",
            "  --no-rollback  on failure leave changes in place instead of restoring the original state",
            "  --timeout N    drain timeout in seconds (overrides the configuration)");

    private Main() {
    }

    public static void main(String[] args) {
        System.exit(run(args));
    }

    static int run(String[] args) {
        AuditLog console = AuditLog.console();
        Path configPath = Config.defaultPath();
        Orchestrator.Options opts = new Orchestrator.Options();
        List<String> positional = new ArrayList<>();
        try {
            for (int i = 0; i < args.length; i++) {
                String a = args[i];
                switch (a) {
                    case "--config":
                        configPath = Paths.get(value(args, ++i, a));
                        break;
                    case "--dry-run":
                        opts.dryRun = true;
                        break;
                    case "--yes":
                        opts.yes = true;
                        break;
                    case "--no-rollback":
                        opts.noRollback = true;
                        break;
                    case "--timeout":
                        opts.timeoutSeconds = parseTimeout(value(args, ++i, a));
                        break;
                    case "-h":
                    case "--help":
                        System.out.println(USAGE);
                        return ExitCodes.OK;
                    default:
                        if (a.startsWith("-")) {
                            throw new WorkflowException(ExitCodes.USAGE, "Unknown option " + a);
                        }
                        positional.add(a);
                }
            }
            if (positional.isEmpty()) {
                throw new WorkflowException(ExitCodes.USAGE, "No command given");
            }
        } catch (WorkflowException e) {
            console.error("%s", e.getMessage());
            System.err.println(USAGE);
            return e.getExitCode();
        }

        String command = positional.get(0);
        Config cfg;
        try {
            cfg = Config.load(configPath);
        } catch (WorkflowException e) {
            console.error("%s", e.getMessage());
            return e.getExitCode();
        }

        if (command.equals("list")) {
            for (WorkflowDef wf : cfg.workflows().values()) {
                System.out.println(wf);
            }
            return ExitCodes.OK;
        }
        if (!command.equals("status") && !command.equals("shutdown") && !command.equals("start")) {
            console.error("Unknown command %s", command);
            System.err.println(USAGE);
            return ExitCodes.USAGE;
        }
        List<WorkflowDef> selected;
        try {
            selected = select(cfg, command, positional.subList(1, positional.size()));
        } catch (WorkflowException e) {
            console.error("%s", e.getMessage());
            return e.getExitCode();
        }

        String runId = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        AuditLog log;
        try {
            log = AuditLog.open(cfg.auditDir, runId);
        } catch (Exception e) {
            // No audit trail, no changes.
            console.error("Cannot open audit log in %s: %s", cfg.auditDir, e);
            return ExitCodes.ERROR;
        }
        try {
            return execute(cfg, command, selected, opts, log);
        } finally {
            log.close();
        }
    }

    /** Named workflows in the given order, or all of them in listed order. */
    static List<WorkflowDef> select(Config cfg, String command, List<String> names) throws WorkflowException {
        List<WorkflowDef> selected = new ArrayList<>();
        if (names.isEmpty()) {
            selected.addAll(cfg.workflows().values());
            return selected;
        }
        for (String name : names) {
            WorkflowDef wf = cfg.workflow(name);
            if (selected.contains(wf)) {
                throw new WorkflowException(ExitCodes.USAGE, "Workflow " + name + " is given more than once");
            }
            selected.add(wf);
        }
        return selected;
    }

    private static int execute(Config cfg, String command, List<WorkflowDef> wfs, Orchestrator.Options opts,
            AuditLog log) {
        List<String> names = new ArrayList<>();
        for (WorkflowDef wf : wfs) {
            names.add(wf.name);
        }
        log.info("mqwf %s %s by %s@%s (dry-run=%s, yes=%s, no-rollback=%s, timeout=%s)",
                command, names, System.getProperty("user.name"), hostName(), opts.dryRun, opts.yes,
                opts.noRollback, opts.timeoutSeconds > 0 ? opts.timeoutSeconds + "s" : "config");

        // Each workflow is locked only while it is being processed, so a lock held elsewhere on one
        // workflow affects only that workflow.
        String owner = "pid " + processId() + " " + System.getProperty("user.name") + " " + command;
        Orchestrator.Locker locker = wf -> WorkflowLock.acquire(cfg.lockDir, wf.name, owner);
        try (MqAdmin mq = MqAdmin.connect(cfg.mq, log)) {
            Orchestrator orch = new Orchestrator(cfg, mq, new AceRestFlowController(cfg.ace, log), log,
                    DrainMonitor.SYSTEM_CLOCK, Main::confirmOnConsole, locker);
            Thread hook = new Thread(() -> reportInterrupted(orch.changes(), log), "mqwf-interrupt");
            Runtime.getRuntime().addShutdownHook(hook);
            try {
                int rc;
                switch (command) {
                    case "status":
                        rc = orch.statusAll(wfs);
                        break;
                    case "shutdown":
                        rc = orch.shutdownAll(wfs, opts);
                        break;
                    default:
                        rc = orch.startAll(wfs, opts);
                }
                log.info("Exit code %d", rc);
                return rc;
            } finally {
                Runtime.getRuntime().removeShutdownHook(hook);
            }
        } catch (MQException e) {
            log.error("Cannot connect to queue manager %s: reason %d%s", cfg.mq.qmgr, e.getReason(),
                    connectHint(e.getReason()));
            log.info("Exit code %d", ExitCodes.ERROR);
            return ExitCodes.ERROR;
        } catch (Exception e) {
            log.error("Failed: %s", e);
            log.info("Exit code %d", ExitCodes.ERROR);
            return ExitCodes.ERROR;
        }
    }

    /**
     * Runs on Ctrl+C / service stop: it cannot safely undo work, but it records exactly what is in
     * effect. Only the workflow being executed can be mid-change; the others are either finished or
     * untouched.
     */
    private static void reportInterrupted(Orchestrator.Changes changes, AuditLog log) {
        if (changes.needsAttention()) {
            WorkflowDef wf = changes.workflow();
            log.error("INTERRUPTED in workflow %s during phase '%s'. In effect: %s", wf.name, changes.phase(),
                    changes.describe());
            log.error("No messages were removed. Re-run 'mqwf shutdown %s' to continue, or 'mqwf start %s' to resume",
                    wf.name, wf.name);
        }
    }

    /**
     * Guards against acting on the wrong scope: for one workflow the operator types its name, for
     * several the action and the count (e.g. "SHUTDOWN 3").
     */
    private static boolean confirmOnConsole(List<WorkflowDef> wfs, String action) throws WorkflowException {
        Console c = System.console();
        if (c == null) {
            throw new WorkflowException(ExitCodes.NOT_CONFIRMED,
                    "No interactive console for confirmation; use --yes for unattended runs");
        }
        String expected;
        String answer;
        if (wfs.size() == 1) {
            expected = wfs.get(0).name;
            answer = c.readLine("%nType the workflow name '%s' to %s it (anything else aborts): ", expected, action);
        } else {
            expected = action.toUpperCase() + " " + wfs.size();
            c.printf("%nWorkflows to %s, one at a time in this order (each independently):%n", action);
            for (int i = 0; i < wfs.size(); i++) {
                c.printf("  %d. %s%n", i + 1, wfs.get(i));
            }
            answer = c.readLine("Type '%s' to %s these %d workflows (anything else aborts): ", expected, action,
                    wfs.size());
        }
        return answer != null && answer.trim().equals(expected);
    }

    private static String connectHint(int reason) {
        switch (reason) {
            case 2035:
                return " (MQRC_NOT_AUTHORIZED: check CONNAUTH/CHLAUTH and setmqaut for this user)";
            case 2058:
                return " (MQRC_Q_MGR_NAME_ERROR: check mq.qmgr)";
            case 2059:
                return " (MQRC_Q_MGR_NOT_AVAILABLE: is the queue manager / listener running?)";
            case 2538:
                return " (MQRC_HOST_NOT_AVAILABLE: check mq.host / mq.port)";
            case 2495:
                return " (MQRC_MODULE_NOT_FOUND: bindings mode needs a local MQ installation and "
                        + "-Djava.library.path pointing at MQ java\\lib64)";
            default:
                return "";
        }
    }

    private static String value(String[] args, int i, String option) throws WorkflowException {
        if (i >= args.length) {
            throw new WorkflowException(ExitCodes.USAGE, option + " needs a value");
        }
        return args[i];
    }

    private static int parseTimeout(String v) throws WorkflowException {
        try {
            int t = Integer.parseInt(v);
            if (t >= 10 && t <= 86400) {
                return t;
            }
        } catch (NumberFormatException ignored) {
            // fall through
        }
        throw new WorkflowException(ExitCodes.USAGE, "--timeout must be 10..86400 seconds");
    }

    private static String hostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown-host";
        }
    }

    private static String processId() {
        String jvm = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        int at = jvm.indexOf('@');
        return at > 0 ? jvm.substring(0, at) : jvm;
    }
}
