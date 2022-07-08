package org.simplestorage4j.sync.ops;

import org.simplestorage4j.api.BlobStoragePath;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesIOTimeResult;
import org.simplestorage4j.api.iocost.immutable.BlobStoragePreEstimateIOCost;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesPreEstimateIOCost;

import lombok.val;

/**
 * 
 */
public class MkdirStorageOperation extends BlobStorageOperation {

	public final BlobStoragePath storagePath;
	
	// ------------------------------------------------------------------------
	
    public MkdirStorageOperation(int taskId, //
    		BlobStoragePath storagePath) {
        super(taskId);
        this.storagePath = storagePath;
    }
    
    // ------------------------------------------------------------------------
    
    @Override
    public String taskTypeName() {
        return "mkdir";
    }

    @Override
	public PerBlobStoragesPreEstimateIOCost preEstimateExecutionCost() {
		return PerBlobStoragesPreEstimateIOCost.of(storagePath.blobStorage, 
				BlobStoragePreEstimateIOCost.ofMetadataCall(1, 0, 1));
	}

	@Override
	public PerBlobStoragesIOTimeResult execute() {
		val startTime = System.currentTimeMillis();
		
		storagePath.mkdirs();
		
		val millis = System.currentTimeMillis() - startTime;
		return PerBlobStoragesIOTimeResult.ofMetadataCall(taskId, startTime, millis, // 
				storagePath.blobStorage.id, 1, 0, 1);
	}

	@Override
    public String toString() {
        return "{mkdir " + taskId //
        		+ storagePath // 
        		+ "}";
    }
    
}
