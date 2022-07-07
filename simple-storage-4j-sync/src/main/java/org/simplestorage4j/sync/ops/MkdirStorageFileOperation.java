package org.simplestorage4j.sync.ops;

import org.simplestorage4j.api.BlobStorage;

/**
 * 
 */
public class MkdirStorageFileOperation extends BlobStorageOperation {

	public final BlobStorage storage;
	public final String path;
	
	// ------------------------------------------------------------------------
	
    public MkdirStorageFileOperation(int taskId, //
    		BlobStorage storage,
    		String path) {
        super(taskId);
        this.storage = storage;
        this.path = path;
    }
    
    // ------------------------------------------------------------------------
    
    @Override
    public String taskTypeName() {
        return "mkdir";
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
        return "{mkdir " + taskId //
        		+ storage.displayName + " '" + path + "'" // 
        		+ "}";
    }
    
}
