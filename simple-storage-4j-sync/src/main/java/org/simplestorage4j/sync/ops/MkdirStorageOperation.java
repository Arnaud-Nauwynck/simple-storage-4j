package org.simplestorage4j.sync.ops;

import org.simplestorage4j.api.BlobStoragePath;
import org.simplestorage4j.sync.ops.stats.BlobStorageIOCost;
import org.simplestorage4j.sync.ops.stats.BlobStorageOperationCost;

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
	public BlobStorageOperationCost estimateExecutionCost() {
		return BlobStorageOperationCost.of(storagePath.blobStorage, 
				BlobStorageIOCost.ofMetadataCall(1, 0, 1));
	}

	@Override
	public BlobStorageOperationExecutionResult execute() {
		val startTime = System.currentTimeMillis();
		
		storagePath.mkdirs();
		
		val millis = System.currentTimeMillis() - startTime;
		return BlobStorageOperationExecutionResult.ofMetadataCall(taskId, startTime, millis, // 
				storagePath.blobStorage.id, 1, 0, 1);
	}

	@Override
    public String toString() {
        return "{mkdir " + taskId //
        		+ storagePath // 
        		+ "}";
    }
    
}
