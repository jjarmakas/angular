package com.ops.mqwf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ConfigTest {
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private static final String BASE = String.join("\n",
            "mq.qmgr=QM1", "ace.url=http://localhost:4414", "ace.api=apiv1", "ace.mode=node", "");

    private String load(String extra) throws Exception {
        Path f = tmp.newFile().toPath();
        Files.write(f, (BASE + extra).getBytes(StandardCharsets.UTF_8));
        try {
            Config.load(f);
            return null;
        } catch (WorkflowException e) {
            assertEquals(ExitCodes.USAGE, e.getExitCode());
            return e.getMessage();
        }
    }

    private static final String WF_A = String.join("\n", "workflows=A", "workflow.A.type=MQ_TO_DB", "workflow.A.queue=Q1",
            "workflow.A.server=IS1", "workflow.A.flow=F1", "");

    @Test
    public void validConfigLoads() throws Exception {
        assertEquals(null, load(WF_A));
    }

    @Test
    public void requiresApiVersion() throws Exception {
        Path f = tmp.newFile().toPath();
        Files.write(f, (BASE.replace("ace.api=apiv1\n", "") + WF_A).getBytes(StandardCharsets.UTF_8));
        try {
            Config.load(f);
            fail();
        } catch (WorkflowException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ace.api"));
        }
    }

    @Test
    public void iib10RejectsStandaloneServerMode() throws Exception {
        Path f = tmp.newFile().toPath();
        Files.write(f, (BASE.replace("ace.mode=node", "ace.mode=server") + WF_A).getBytes(StandardCharsets.UTF_8));
        try {
            Config.load(f);
            fail();
        } catch (WorkflowException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("IIB v10"));
        }
    }

    @Test
    public void rejectsPlainTextPassword() throws Exception {
        String err = load(WF_A + "ace.user=admin\nace.password=secret\n");
        assertTrue(err, err.contains("plain-text passwords"));
    }

    @Test
    public void rejectsTwoWorkflowsOnOneQueue() throws Exception {
        String err = load(WF_A.replace("workflows=A", "workflows=A,B")
                + "workflow.B.type=DB_TO_MQ\nworkflow.B.queue=Q1\nworkflow.B.server=IS1\nworkflow.B.flow=F2\n");
        assertTrue(err, err.contains("also used by workflow"));
    }

    private static final String WF_B = "workflow.B.type=DB_TO_MQ\nworkflow.B.queue=Q2\nworkflow.B.server=IS1\nworkflow.B.flow=F2\n";

    @Test
    public void workflowsAreKeptInListedOrder() throws Exception {
        Path f = tmp.newFile().toPath();
        Files.write(f, (BASE + WF_A.replace("workflows=A", "workflows= B , A") + WF_B).getBytes(StandardCharsets.UTF_8));
        assertEquals(java.util.Arrays.asList("B", "A"), new java.util.ArrayList<>(Config.load(f).workflows().keySet()));
    }

    @Test
    public void requiresWorkflowsList() throws Exception {
        String err = load(WF_A.replace("workflows=A\n", ""));
        assertTrue(err, err.contains("workflows is required"));
    }

    @Test
    public void rejectsDefinedButUnlistedWorkflow() throws Exception {
        String err = load(WF_A + WF_B);
        assertTrue(err, err.contains("does not list [B]"));
    }

    @Test
    public void rejectsListedButUndefinedWorkflow() throws Exception {
        String err = load(WF_A.replace("workflows=A", "workflows=A,X"));
        assertTrue(err, err.contains("lists [X]"));
    }

    @Test
    public void rejectsDuplicateInList() throws Exception {
        String err = load(WF_A.replace("workflows=A", "workflows=A,A"));
        assertTrue(err, err.contains("more than once"));
    }

    @Test
    public void rejectsWildcardQueueName() throws Exception {
        String err = load(WF_A.replace("queue=Q1", "queue=Q*"));
        assertTrue(err, err.contains("invalid value"));
    }

    @Test
    public void rejectsUnknownType() throws Exception {
        String err = load(WF_A.replace("MQ_TO_DB", "BOTH"));
        assertTrue(err, err.contains("DB_TO_MQ or MQ_TO_DB"));
    }

    @Test
    public void requiresServerInNodeMode() throws Exception {
        String err = load(WF_A.replace("workflow.A.server=IS1\n", ""));
        assertTrue(err, err.contains("workflow.A.server"));
    }

    @Test
    public void rejectsTooShortStableWindow() throws Exception {
        String err = load(WF_A + "drain.stablePolls=1\n");
        assertTrue(err, err.contains("drain.stablePolls"));
    }

    @Test
    public void unknownWorkflowIsUsageError() throws Exception {
        Path f = tmp.newFile().toPath();
        Files.write(f, (BASE + WF_A).getBytes(StandardCharsets.UTF_8));
        try {
            Config.load(f).workflow("NOPE");
            fail();
        } catch (WorkflowException e) {
            assertEquals(ExitCodes.USAGE, e.getExitCode());
        }
    }
}
