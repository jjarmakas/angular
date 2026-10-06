package com.ops.mqwf;

public enum WorkflowType {
    /**
     * DatabaseInput node -> MQOutput node -> local queue, consumed by a remote client.
     * The flow is the PRODUCER of the queue.
     */
    DB_TO_MQ,

    /**
     * Remote client -> local queue -> MQInput node -> Database node.
     * The flow is the CONSUMER of the queue.
     */
    MQ_TO_DB
}
