package org.simplestorage4j.sync.ops;

import lombok.Getter;

@Getter
public abstract class BlobStorageOperation {

    public final int taskId;
    
    // --------------------------------------------------------------------------------------------
    
    public BlobStorageOperation(int taskId) {
        this.taskId = taskId;
    }

    // --------------------------------------------------------------------------------------------
    
    public abstract String taskTypeName();

    public abstract BlobStorageOperationExecutionCostEstimation estimateExecutionCost();

    public abstract BlobStorageOperationExecutionResult execute();

    // --------------------------------------------------------------------------------------------

    
}
