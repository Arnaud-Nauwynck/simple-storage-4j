package org.simplestorage4j.sync.ops;

import org.simplestorage4j.api.BlobStorage;

/**
 * 
 */
public class CopyStorageFileOperation extends BlobStorageOperation {
    
	public final BlobStorage srcStorage;
    public final String srcPath;

	public final BlobStorage destStorage;
    public final String destPath;

    public final long srcFileLen;
    
    // ------------------------------------------------------------------------
	
    public CopyStorageFileOperation(int taskId, //
    		BlobStorage srcStorage, String srcPath, 
    		BlobStorage destStorage, String destPath, // 
    		long srcFileLen) {
		super(taskId);
		this.srcStorage = srcStorage;
		this.srcPath = srcPath;
		this.destStorage = destStorage;
		this.destPath = destPath;
		this.srcFileLen = srcFileLen;
	}

    // ------------------------------------------------------------------------

	@Override
    public String taskTypeName() {
        return "copy-file";
    }

    @Override
	public BlobStorageOperationExecutionCostEstimation estimateExecutionCost() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BlobStorageOperationExecutionResult execute() {
		// TODO Auto-generated method stub
		return null;
	}

	
    @Override
    public String toString() {
        return "{copy-file " + taskId 
        		+ " src: " + srcStorage.displayName
        		+ " '" + srcPath + "'"
        		+ " dest: " + destStorage.displayName
        		+ ", '" + destPath + "'"
        		+ " (" + srcFileLen + ")"
        		+ "}";
    }
    
}