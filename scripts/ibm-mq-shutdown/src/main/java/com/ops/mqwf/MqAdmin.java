package com.ops.mqwf;

import java.io.IOException;
import java.util.Hashtable;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.pcf.PCFException;
import com.ibm.mq.headers.pcf.PCFMessage;
import com.ibm.mq.headers.pcf.PCFMessageAgent;

/**
 * Queue manager administration through PCF (the programmatic equivalent of runmqsc). Responses are
 * typed integers, so nothing depends on parsing localised command output.
 * <p>
 * Access is read-only: only INQUIRE_Q and INQUIRE_Q_STATUS are ever issued. The utility never
 * changes queue attributes and never gets, puts, clears or deletes messages.
 */
public final class MqAdmin implements QueueAdmin, AutoCloseable {
    private final Config.Mq settings;
    private final AuditLog log;
    private MQQueueManager qmgr;
    private PCFMessageAgent agent;

    private MqAdmin(Config.Mq settings, AuditLog log) {
        this.settings = settings;
        this.log = log;
    }

    public static MqAdmin connect(Config.Mq settings, AuditLog log) throws MQException, MQDataException {
        MqAdmin admin = new MqAdmin(settings, log);
        admin.open();
        return admin;
    }

    private void open() throws MQException, MQDataException {
        Hashtable<String, Object> props = new Hashtable<>();
        if (settings.client) {
            props.put(MQConstants.TRANSPORT_PROPERTY, MQConstants.TRANSPORT_MQSERIES_CLIENT);
            props.put(MQConstants.HOST_NAME_PROPERTY, settings.host);
            props.put(MQConstants.PORT_PROPERTY, settings.port);
            props.put(MQConstants.CHANNEL_PROPERTY, settings.channel);
            if (settings.sslCipherSuite != null) {
                props.put(MQConstants.SSL_CIPHER_SUITE_PROPERTY, settings.sslCipherSuite);
            }
        } else {
            props.put(MQConstants.TRANSPORT_PROPERTY, MQConstants.TRANSPORT_MQSERIES_BINDINGS);
        }
        if (settings.user != null) {
            props.put(MQConstants.USER_ID_PROPERTY, settings.user);
            props.put(MQConstants.PASSWORD_PROPERTY, settings.password);
            props.put(MQConstants.USE_MQCSP_AUTHENTICATION_PROPERTY, Boolean.TRUE);
        }
        qmgr = new MQQueueManager(settings.qmgr, props);
        agent = new PCFMessageAgent(qmgr);
        log.info("Connected to queue manager %s (%s)", settings.qmgr,
                settings.client ? "client " + settings.host + "(" + settings.port + ") " + settings.channel : "bindings");
    }

    /** Current queue state. Fails if the queue does not exist or is not a local queue. */
    @Override
    public QueueSnapshot snapshot(String queue) throws WorkflowException {
        PCFMessage inq = new PCFMessage(MQConstants.MQCMD_INQUIRE_Q);
        inq.addParameter(MQConstants.MQCA_Q_NAME, queue);
        inq.addParameter(MQConstants.MQIACF_Q_ATTRS, new int[] {
                MQConstants.MQIA_Q_TYPE, MQConstants.MQIA_INHIBIT_PUT, MQConstants.MQIA_INHIBIT_GET,
                MQConstants.MQCA_BACKOUT_REQ_Q_NAME });
        PCFMessage q = single(send(inq, queue), queue);

        int type = intParam(q, MQConstants.MQIA_Q_TYPE);
        if (type != MQConstants.MQQT_LOCAL) {
            throw new WorkflowException(ExitCodes.PREFLIGHT_FAILED,
                    "Queue " + queue + " is not a local queue (QTYPE=" + type + "); refusing to manage it");
        }
        boolean putInhibited = intParam(q, MQConstants.MQIA_INHIBIT_PUT) == MQConstants.MQQA_PUT_INHIBITED;
        boolean getInhibited = intParam(q, MQConstants.MQIA_INHIBIT_GET) == MQConstants.MQQA_GET_INHIBITED;
        String boq = strParam(q, MQConstants.MQCA_BACKOUT_REQ_Q_NAME);

        PCFMessage st = new PCFMessage(MQConstants.MQCMD_INQUIRE_Q_STATUS);
        st.addParameter(MQConstants.MQCA_Q_NAME, queue);
        st.addParameter(MQConstants.MQIACF_Q_STATUS_TYPE, MQConstants.MQIACF_Q_STATUS);
        st.addParameter(MQConstants.MQIACF_Q_STATUS_ATTRS, new int[] {
                MQConstants.MQIA_CURRENT_Q_DEPTH, MQConstants.MQIA_OPEN_INPUT_COUNT,
                MQConstants.MQIA_OPEN_OUTPUT_COUNT, MQConstants.MQIACF_UNCOMMITTED_MSGS });
        PCFMessage s = single(send(st, queue), queue);

        return new QueueSnapshot(queue,
                intParam(s, MQConstants.MQIA_CURRENT_Q_DEPTH),
                intParam(s, MQConstants.MQIACF_UNCOMMITTED_MSGS),
                intParam(s, MQConstants.MQIA_OPEN_INPUT_COUNT),
                intParam(s, MQConstants.MQIA_OPEN_OUTPUT_COUNT),
                putInhibited, getInhibited, boq.isEmpty() ? null : boq);
    }

    /** Depth of a local queue, or null if it cannot be read (e.g. it is not local). Never throws. */
    @Override
    public Integer depthOrNull(String queue) {
        try {
            return snapshot(queue).depth;
        } catch (WorkflowException e) {
            log.warn("Cannot read depth of %s: %s", queue, e.getMessage());
            return null;
        }
    }

    /**
     * Sends a PCF request. A broken connection (e.g. a network blip in client mode) is retried once
     * after reconnecting; every command issued here is a read-only inquiry, so a retry is safe.
     */
    private PCFMessage[] send(PCFMessage request, String queue) throws WorkflowException {
        for (int attempt = 1;; attempt++) {
            try {
                return agent.send(request);
            } catch (PCFException e) {
                throw new WorkflowException(ExitCodes.ERROR, describe(queue, e), e);
            } catch (MQDataException | IOException e) {
                int reason = e instanceof MQDataException ? ((MQDataException) e).getReason() : -1;
                if (attempt == 1 && isConnectionLoss(reason)) {
                    log.warn("Connection to %s lost (reason %d); reconnecting once", settings.qmgr, reason);
                    try {
                        closeQuietly();
                        open();
                        continue;
                    } catch (MQException | MQDataException re) {
                        throw new WorkflowException(ExitCodes.ERROR, "Reconnect to " + settings.qmgr + " failed: " + re, re);
                    }
                }
                throw new WorkflowException(ExitCodes.ERROR, "PCF request for " + queue + " failed: " + e, e);
            }
        }
    }

    private static boolean isConnectionLoss(int reason) {
        return reason == MQConstants.MQRC_CONNECTION_BROKEN || reason == MQConstants.MQRC_HCONN_ERROR;
    }

    private static String describe(String queue, PCFException e) {
        int r = e.getReason();
        if (r == MQConstants.MQRC_UNKNOWN_OBJECT_NAME || r == MQConstants.MQRCCF_Q_WRONG_TYPE) {
            return "Queue " + queue + " does not exist as a local queue (reason " + r + ")";
        }
        if (r == MQConstants.MQRC_NOT_AUTHORIZED) {
            return "Not authorized to administer queue " + queue + " (reason 2035); check setmqaut for this user";
        }
        return "PCF command for " + queue + " was rejected (reason " + r + "): " + e.getMessage();
    }

    private static PCFMessage single(PCFMessage[] responses, String queue) throws WorkflowException {
        if (responses == null || responses.length != 1) {
            throw new WorkflowException(ExitCodes.ERROR, "Expected exactly one PCF response for " + queue
                    + " but got " + (responses == null ? 0 : responses.length));
        }
        return responses[0];
    }

    private static int intParam(PCFMessage m, int id) throws WorkflowException {
        try {
            return m.getIntParameterValue(id);
        } catch (PCFException e) {
            throw new WorkflowException(ExitCodes.ERROR, "PCF response is missing parameter " + id, e);
        }
    }

    private static String strParam(PCFMessage m, int id) throws WorkflowException {
        try {
            String v = m.getStringParameterValue(id);
            return v == null ? "" : v.trim();
        } catch (PCFException e) {
            throw new WorkflowException(ExitCodes.ERROR, "PCF response is missing parameter " + id, e);
        }
    }

    private void closeQuietly() {
        try {
            if (agent != null) {
                agent.disconnect();
            }
        } catch (Exception ignored) {
            // best effort
        }
        try {
            if (qmgr != null && qmgr.isConnected()) {
                qmgr.disconnect();
            }
        } catch (Exception ignored) {
            // best effort
        }
        agent = null;
        qmgr = null;
    }

    @Override
    public void close() {
        closeQuietly();
    }
}
