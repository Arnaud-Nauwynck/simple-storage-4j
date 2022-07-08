package org.simplestorage4j.sync.ops;

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
public class CopyFileStorageOperation extends BlobStorageOperation {
    
	public final @Nonnull BlobStoragePath destStoragePath;

	public final @Nonnull BlobStoragePath srcStoragePath;
    public final long srcFileLen;
    
    // ------------------------------------------------------------------------
	
    public CopyFileStorageOperation(int taskId, //
    		@Nonnull BlobStoragePath destStoragePath, // 
    		@Nonnull BlobStoragePath srcStoragePath, 
    		long srcFileLen) {
		super(taskId);
		this.destStoragePath = Objects.requireNonNull(destStoragePath);
		this.srcStoragePath = Objects.requireNonNull(srcStoragePath);
		this.srcFileLen = srcFileLen;
	}

    // ------------------------------------------------------------------------

	@Override
    public String taskTypeName() {
        return "copy-file";
    }

    @Override
	public PerBlobStoragesPreEstimateIOCost preEstimateExecutionCost() {
    	return PerBlobStoragesPreEstimateIOCost.of( //
    			srcStoragePath.blobStorage, BlobStoragePreEstimateIOCost.ofIoRead1(srcFileLen),
    			destStoragePath.blobStorage, BlobStoragePreEstimateIOCost.ofIoWrite1(srcFileLen));
	}

	@Override
	public PerBlobStoragesIOTimeResult execute() {
		val startTime = System.currentTimeMillis();
		val inputIOCounter = new BlobStorageIOTimeCounter();
		val outputIOCounter = new BlobStorageIOTimeCounter();
		
		try (val input = srcStoragePath.openRead()) {
			try (val output = destStoragePath.openWrite()) {
				// equivalent to .. IOUtils.copy(input, output); 
				// with IOstats per input|output
				BlobStorageIOUtils.copy(input, inputIOCounter, output, outputIOCounter);
			}
		} catch(IOException ex) {
			throw new RuntimeException("Failed " + toString(), ex);
		}
		
		val millis = System.currentTimeMillis();
		return PerBlobStoragesIOTimeResult.of(taskId, startTime, millis,
				srcStoragePath.blobStorage.id, inputIOCounter.toImmutable(),
				destStoragePath.blobStorage.id, outputIOCounter.toImmutable()
				);
	}

    @Override
    public String toString() {
        return "{copy-file " + taskId 
        		+ " dest: " + destStoragePath
        		+ " src: " + srcStoragePath
        		+ " (len: " + srcFileLen + ")"
        		+ "}";
    }
    
}
