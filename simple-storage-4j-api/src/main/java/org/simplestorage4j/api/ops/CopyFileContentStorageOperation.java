package org.simplestorage4j.api.ops;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.simplestorage4j.api.BlobStoragePath;
import org.simplestorage4j.api.iocost.counter.BlobStorageIOTimeCounter;
import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.iocost.immutable.BlobStoragePreEstimateIOCost;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesPreEstimateIOCost;
import org.simplestorage4j.api.util.BlobStorageIOUtils;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 */
@Slf4j
public class CopyFileContentStorageOperation extends BlobStorageOperation {
    
	public final @Nonnull BlobStoragePath destStoragePath;

    private final @Nonnull byte[] srcContent;
    
    // ------------------------------------------------------------------------
	
    public CopyFileContentStorageOperation(long jobId, long taskId, //
    		@Nonnull BlobStoragePath destStoragePath, // 
    		@Nonnull byte[] srcContent) {
		super(jobId, taskId);
		this.destStoragePath = Objects.requireNonNull(destStoragePath);
		this.srcContent = Objects.requireNonNull(srcContent);
	}

    // ------------------------------------------------------------------------

	@Override
    public String taskTypeName() {
        return "copy-file-content";
    }

    @Override
	public PerBlobStoragesPreEstimateIOCost preEstimateExecutionCost() {
    	return PerBlobStoragesPreEstimateIOCost.of( //
    			destStoragePath.blobStorage, BlobStoragePreEstimateIOCost.ofIoWrite1(srcContent.length));
	}

	@Override
	public BlobStorageOperationResult execute(BlobStorageOperationExecContext ctx) {
		val startTime = System.currentTimeMillis();
		val outputIOCounter = new BlobStorageIOTimeCounter();
		
		try (val output = destStoragePath.openWrite()) {
			BlobStorageIOUtils.copy(srcContent, output, outputIOCounter);
		} catch(IOException ex) {
			throw new RuntimeException("Failed " + toString(), ex);
		}
		
		val millis = System.currentTimeMillis();
		val res = BlobStorageOperationResult.of(jobId, taskId, startTime, millis,
				destStoragePath.blobStorage.id, outputIOCounter.toImmutable()
				);
		ctx.logIncr_copyFileContent(this, res, logPrefix -> log.info(logPrefix + "(" + destStoragePath + ", srcContent.len:" + srcContent.length + ")"));
		return res;
	}

    @Override
    public String toString() {
        return "{copy-file-content " + taskId 
        		+ " dest: " + destStoragePath
        		+ " srcLen: " + srcContent.length
        		+ "}";
    }
    
}
