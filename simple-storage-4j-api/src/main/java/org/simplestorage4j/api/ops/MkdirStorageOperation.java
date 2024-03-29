package org.simplestorage4j.api.ops;

import org.simplestorage4j.api.BlobStoragePath;
import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.iocost.immutable.BlobStoragePreEstimateIOCost;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesPreEstimateIOCost;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO;
import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO.MkdirStorageOperationDTO;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 *
 */
@Slf4j
public class MkdirStorageOperation extends BlobStorageOperation {

	public final BlobStoragePath storagePath;
	
	// ------------------------------------------------------------------------
	
    public MkdirStorageOperation(long jobId, long taskId, //
    		BlobStoragePath storagePath) {
        super(jobId, taskId);
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
	public BlobStorageOperationResult execute(BlobStorageOperationExecContext ctx) {
		val startTime = System.currentTimeMillis();
		
		storagePath.mkdirs();
		
		val millis = System.currentTimeMillis() - startTime;
		val res = BlobStorageOperationResult.ofMetadataCall(jobId, taskId, startTime, millis, //
				storagePath.blobStorage.id, 1, 0, 1);
		ctx.logIncr_mkdir(this, res, logPrefix -> log.info(logPrefix + "(" + storagePath + ")"));
		return res;
	}

	@Override
    public BlobStorageOperationDTO toDTO() {
    	return new MkdirStorageOperationDTO(jobId, taskId, storagePath.toDTO());
    }

	@Override
    public String toString() {
        return "{mkdir " + taskId //
        		+ " " + storagePath //
        		+ "}";
    }

}
