package org.simplestorage4j.api.ops.executor;

import org.simplestorage4j.api.ops.BlobStorageOperation;
import org.simplestorage4j.api.ops.BlobStorageOperationExecContext;

import lombok.val;

/**
 * implementation of BlobStorageOperationsRunner using single-thread
 */
public class SingleThreadBlobStorageOperationsQueuePoller extends AbstractBlobStorageOperationQueuePoller {

	// ------------------------------------------------------------------------
	
	public SingleThreadBlobStorageOperationsQueuePoller(
			BlobStorageJobOperationsExecQueue queue, BlobStorageOperationExecContext execCtx) {
		super(queue, execCtx);
	}
	
	// ------------------------------------------------------------------------

	@Override
	public void run() {
		for(;;) {
			if (isInterruptRequested()) {
				break;
			}
			val polled = queue.poll();
			if (polled == null) {
				break;
			}
			doExecOp(polled);
		}
	}

	private void doExecOp(BlobStorageOperation op) {
		try {
			val opResult = op.execute(execCtx);
			
			queue.onOpExecuted(opResult);
		} catch(Throwable ex) {
			queue.onOpUnexpectedError(ex, op);
		}
	}

}
