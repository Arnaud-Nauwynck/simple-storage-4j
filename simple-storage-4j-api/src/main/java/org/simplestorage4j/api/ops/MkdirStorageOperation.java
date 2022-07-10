package org.simplestorage4j.api.ops;

import org.simplestorage4j.api.BlobStoragePath;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesIOTimeResult;
import org.simplestorage4j.api.iocost.immutable.BlobStoragePreEstimateIOCost;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesPreEstimateIOCost;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 */
@Slf4j
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
	public PerBlobStoragesIOTimeResult execute(BlobStorageOperationExecContext ctx) {
		val startTime = System.currentTimeMillis();
		
		storagePath.mkdirs();
		
		val millis = System.currentTimeMillis() - startTime;
		val res = PerBlobStoragesIOTimeResult.ofMetadataCall(taskId, startTime, millis, // 
				storagePath.blobStorage.id, 1, 0, 1);
		ctx.logIncr_mkdir(this, res, logPrefix -> log.info(logPrefix + "(" + storagePath + ")"));
		return res;
	}

	@Override
    public String toString() {
        return "{mkdir " + taskId //
        		+ storagePath // 
        		+ "}";
    }
    
}
