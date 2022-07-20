package org.simplestorage4j.api.ops.executor;

import java.util.List;

import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.ops.BlobStorageOperation;
import org.simplestorage4j.api.ops.BlobStorageOperationExecContext;

/**
 * delegate execution of multiple independent {@link BlobStorageOperation}s
 *
 * maybe remotly... typically called in spark to distribute computing
 *
 */
public abstract class BlobStorageOperationBatchExecutor {

	public abstract List<BlobStorageOperationResult> executeBatch(
			BlobStorageOperationExecContext execCtx, List<BlobStorageOperation> ops);
	
}
