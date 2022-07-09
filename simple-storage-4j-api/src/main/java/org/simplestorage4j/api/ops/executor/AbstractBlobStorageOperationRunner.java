package org.simplestorage4j.api.ops.executor;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractBlobStorageOperationRunner {

	protected final BlobStorageOperationExecQueue queue;

	private AtomicBoolean interruptRequested = new AtomicBoolean();
	
	// ------------------------------------------------------------------------
	
	public AbstractBlobStorageOperationRunner(BlobStorageOperationExecQueue queue) {
		this.queue = queue;
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
