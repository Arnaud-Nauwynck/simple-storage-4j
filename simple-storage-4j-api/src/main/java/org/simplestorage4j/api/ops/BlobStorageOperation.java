package org.simplestorage4j.api.ops;

import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesIOTimeResult;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesPreEstimateIOCost;

import lombok.Getter;

@Getter
public abstract class BlobStorageOperation {

    public final int taskId;
    
//    TODO 
//    private List<BlobStorageOperation> dependencies;
    
    // --------------------------------------------------------------------------------------------
    
    public BlobStorageOperation(int taskId) {
        this.taskId = taskId;
    }

    // --------------------------------------------------------------------------------------------
    
    public abstract String taskTypeName();

    public abstract PerBlobStoragesPreEstimateIOCost preEstimateExecutionCost();

    public abstract PerBlobStoragesIOTimeResult execute();

    // --------------------------------------------------------------------------------------------

    
}
