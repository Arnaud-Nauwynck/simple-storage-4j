package org.simplestorage4j.api.ops;

import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesPreEstimateIOCost;

import lombok.Getter;

@Getter
public abstract class BlobStorageOperation {

	public final long jobId; 
    public final long taskId;
    
//    TODO 
//    private List<BlobStorageOperation> dependencies;
    
    // --------------------------------------------------------------------------------------------
    
    public BlobStorageOperation(long jobId, long taskId) {
        this.jobId = jobId;
    	this.taskId = taskId;
    }

    // --------------------------------------------------------------------------------------------
    
    public abstract String taskTypeName();

    public abstract PerBlobStoragesPreEstimateIOCost preEstimateExecutionCost();

    public abstract BlobStorageOperationResult execute(BlobStorageOperationExecContext ctx);
    
}
