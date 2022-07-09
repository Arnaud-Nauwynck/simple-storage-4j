package org.simplestorage4j.api.ops.executor;

import java.util.List;

import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesIOTimeResult;
import org.simplestorage4j.api.ops.BlobStorageOperation;

/**
 * delegate execution of multiple independent {@link BlobStorageOperation}s
 * 
 * maybe remotly... typically called in spark to distribute computing
 *
 */
public abstract class BlobStorageOperationBatchExecutor {

	public abstract List<PerBlobStoragesIOTimeResult> executeBatch(List<BlobStorageOperation> ops);
	
}
