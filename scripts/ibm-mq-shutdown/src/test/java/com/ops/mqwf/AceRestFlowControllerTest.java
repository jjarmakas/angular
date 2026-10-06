package com.ops.mqwf;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ops.mqwf.FlowController.FlowState;

public class AceRestFlowControllerTest {

    private static AceRestFlowController controller(boolean apiV1, boolean nodeMode) {
        Config.Ace ace = new Config.Ace();
        ace.url = "http://localhost:4414";
        ace.apiV1 = apiV1;
        ace.nodeMode = nodeMode;
        return new AceRestFlowController(ace, AuditLog.console());
    }

    private static WorkflowDef flow(String app) {
        return new WorkflowDef("WF", WorkflowType.MQ_TO_DB, "Q", "IS 1", app, "My Flow", 60);
    }

    // ------------------------------------------------------------- IIB v10

    @Test
    public void iib10PathUsesExecutionGroups() {
        assertEquals("/apiv1/executiongroups/IS%201/applications/App/messageflows/My%20Flow",
                controller(true, true).flowPath(flow("App")));
    }

    @Test
    public void iib10IndependentFlowHasNoApplicationSegment() {
        assertEquals("/apiv1/executiongroups/IS%201/messageflows/My%20Flow",
                controller(true, true).flowPath(flow(null)));
    }

    @Test
    public void iib10ParsesTopLevelIsRunning() {
        assertEquals(FlowState.RUNNING, AceRestFlowController.parseState(
                "{\"type\":\"messageFlow\",\"name\":\"F\",\"isRunning\":true,\"uri\":\"/apiv1/...\"}", true));
        assertEquals(FlowState.STOPPED, AceRestFlowController.parseState(
                "{\"name\":\"F\",\"isRunning\" : false}", true));
    }

    @Test
    public void iib10FindsIsRunningEvenIfAnActiveKeyFollows() {
        assertEquals(FlowState.STOPPED, AceRestFlowController.parseState(
                "{\"isRunning\":false,\"properties\":{\"active\":\"x\"}}", true));
    }

    // ---------------------------------------------------------- ACE v11/12

    @Test
    public void aceNodePathUsesServers() {
        assertEquals("/apiv2/servers/IS%201/applications/App/messageflows/My%20Flow",
                controller(false, true).flowPath(flow("App")));
    }

    @Test
    public void aceStandalonePathHasNoServer() {
        assertEquals("/apiv2/applications/App/messageflows/My%20Flow",
                controller(false, false).flowPath(flow("App")));
    }

    @Test
    public void aceParsesActiveIsRunning() {
        assertEquals(FlowState.RUNNING, AceRestFlowController.parseState(
                "{\"name\":\"F\",\"type\":\"messageFlow\",\"active\":{\"isRunning\":true,\"state\":\"started\"}}", false));
        assertEquals(FlowState.STOPPED, AceRestFlowController.parseState(
                "{\"name\":\"F\",\"active\": {\"isRunning\" : false}}", false));
    }

    @Test
    public void aceFallsBackToStateField() {
        assertEquals(FlowState.STOPPED, AceRestFlowController.parseState("{\"active\":{\"state\":\"stopped\"}}", false));
        assertEquals(FlowState.RUNNING, AceRestFlowController.parseState("{\"active\":{\"state\":\"started\"}}", false));
    }

    // -------------------------------------------------------------- common

    @Test
    public void anythingElseIsUnknown() {
        for (boolean v1 : new boolean[] { true, false }) {
            assertEquals(FlowState.UNKNOWN, AceRestFlowController.parseState("{\"state\":\"starting\"}", v1));
            assertEquals(FlowState.UNKNOWN, AceRestFlowController.parseState("<html>login</html>", v1));
            assertEquals(FlowState.UNKNOWN, AceRestFlowController.parseState("", v1));
        }
    }
}
