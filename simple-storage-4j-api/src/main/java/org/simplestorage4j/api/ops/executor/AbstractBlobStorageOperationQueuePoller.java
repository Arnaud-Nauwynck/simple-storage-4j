package org.simplestorage4j.api.ops.executor;

import java.util.concurrent.atomic.AtomicBoolean;

import org.simplestorage4j.api.ops.BlobStorageOperationExecContext;

public abstract class AbstractBlobStorageOperationQueuePoller {

	protected final BlobStorageOperationExecQueue queue;
	protected final BlobStorageOperationExecContext execCtx;
	
	private AtomicBoolean interruptRequested = new AtomicBoolean();
	
	// ------------------------------------------------------------------------
	
	public AbstractBlobStorageOperationQueuePoller(BlobStorageOperationExecQueue queue,
			BlobStorageOperationExecContext execCtx) {
		this.queue = queue;
		this.execCtx = execCtx;
	}
	
	// ------------------------------------------------------------------------

	public abstract void run();
	
	public void setInterruptRequested() {
		this.interruptRequested.set(true);
	}
	public boolean isInterruptRequested() {
		return this.interruptRequested.get();
	}

}
