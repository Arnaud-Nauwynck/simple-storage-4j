package org.simplestorage4j.api.ops.executor;

import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.ops.BlobStorageOperation;

/**
 * hook callbacks support for BlobStorageOperationExecQueue
 */
public class BlobStorageOperationExecQueueHook {
	
	public void onOpExecutedSuccess(BlobStorageOperationResult result, BlobStorageOperation op) {
		// do nothing, cf override
	}

	public void onOpExecutedError(BlobStorageOperationResult result, BlobStorageOperation op) {
		// do nothing, cf override
	}

	public void onOpUnexpectedError(Throwable ex, BlobStorageOperation op) {
		// do nothing, cf override
	}

	public void onPolled(BlobStorageOperation op) {
		// do nothing, cf override
	}
	
	public void onOpRequeue(BlobStorageOperation op) {
		// do nothing, cf override
	}

	public void onAddOp(BlobStorageOperation op) {
		// do nothing, cf override
	}

}
