package com.mqarchiver;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.MbException;
import com.ibm.broker.plugin.MbMessage;
import com.ibm.broker.plugin.MbMessageAssembly;
import com.ibm.broker.plugin.MbOutputTerminal;
import com.ibm.broker.plugin.MbUserException;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * JavaCompute node that checks the depth of every queue listed in the
 * archiver configuration file and, for any queue whose depth exceeds its
 * configured limit, moves all its messages into
 * &lt;archiveBasePath&gt;/Archive_YYYYMMDD/&lt;queueName&gt;.txt.
 *
 * This node does not act on the content of the message flowing through it -
 * it is meant to be driven by a TimeoutNotification/TimeoutControl input
 * node on a schedule (e.g. every few minutes), not by application traffic.
 * See README.md for flow wiring and deployment notes.
 */
public class QueueArchiverComputeNode extends MbJavaComputeNode {

    private static final Logger LOGGER = Logger.getLogger(QueueArchiverComputeNode.class.getName());

    private static final String CONFIG_PATH_SYSTEM_PROPERTY = "mqarchiver.configPath";
    private static final String CONFIG_PATH_ENV_VAR = "MQARCHIVER_CONFIG_PATH";
    private static final String DEFAULT_CONFIG_PATH = "mqarchiver/archiver.properties";

    public void evaluate(MbMessageAssembly inAssembly) throws MbException {
        MbOutputTerminal outTerminal = getOutputTerminal("out");

        try {
            ArchiverConfig config = ArchiverConfig.load(resolveConfigFilePath());
            new QueueArchiver(config).archiveQueuesExceedingLimit();

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
