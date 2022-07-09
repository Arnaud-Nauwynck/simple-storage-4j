package org.simplestorage4j.api.ops;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.simplestorage4j.api.BlobStoragePath;
import org.simplestorage4j.api.iocost.counter.BlobStorageIOTimeCounter;
import org.simplestorage4j.api.iocost.immutable.BlobStoragePreEstimateIOCost;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesIOTimeResult;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesPreEstimateIOCost;
import org.simplestorage4j.api.util.BlobStorageIOUtils;

import lombok.val;

/**
 * 
 */
public class CopyFileContentStorageOperation extends BlobStorageOperation {
    
	public final @Nonnull BlobStoragePath destStoragePath;

    private final byte[] srcContent;
    
    // ------------------------------------------------------------------------
	
    public CopyFileContentStorageOperation(int taskId, //
    		@Nonnull BlobStoragePath destStoragePath, // 
    		@Nonnull byte[] srcContent) {
		super(taskId);
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
	public PerBlobStoragesIOTimeResult execute() {
		val startTime = System.currentTimeMillis();
		val outputIOCounter = new BlobStorageIOTimeCounter();
		
		try (val output = destStoragePath.openWrite()) {
			BlobStorageIOUtils.copy(srcContent, output, outputIOCounter);
		} catch(IOException ex) {
			throw new RuntimeException("Failed " + toString(), ex);
		}
		
		val millis = System.currentTimeMillis();
		return PerBlobStoragesIOTimeResult.of(taskId, startTime, millis,
				destStoragePath.blobStorage.id, outputIOCounter.toImmutable()
				);
	}

    @Override
    public String toString() {
        return "{copy-file-content " + taskId 
        		+ " dest: " + destStoragePath
        		+ " srcLen: " + srcContent.length
        		+ "}";
    }
    
}
