package org.simplestorage4j.api.ops.executor;

import org.simplestorage4j.api.ops.BlobStorageOperation;

import lombok.val;

/**
 * implementation of BlobStorageOperationsRunner using single-thread
 */
public class SingleThreadBlobStorageOperationsRunner extends AbstractBlobStorageOperationRunner {

	// ------------------------------------------------------------------------
	
	public SingleThreadBlobStorageOperationsRunner(BlobStorageOperationExecQueue queue) {
		super(queue);
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
			val opResult = op.execute();
			
			queue.onOpExecuted(opResult, op);
		} catch(Throwable ex) {
			queue.onOpUnexpectedError(ex, op);
		}
	}

}
