package org.simplestorage4j.sync.ops;

import org.simplestorage4j.api.BlobStoragePath;
import org.simplestorage4j.api.util.BlobStorageNotImpl;
import org.simplestorage4j.sync.ops.stats.BlobStorageIOCost;
import org.simplestorage4j.sync.ops.stats.BlobStorageOperationCost;

import javax.annotation.Nonnull;

import java.util.Objects;

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
	public BlobStorageOperationCost estimateExecutionCost() {
    	return BlobStorageOperationCost.of( //
    			srcStoragePath.blobStorage, BlobStorageIOCost.ofIoRead1(srcFileLen),
    			destStoragePath.blobStorage, BlobStorageIOCost.ofIoWrite1(srcFileLen));
	}

	@Override
	public BlobStorageOperationExecutionResult execute() {
		// TODO
		throw BlobStorageNotImpl.notImpl();
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
