package org.simplestorage4j.api.ops.executor;

import java.util.concurrent.ExecutorService;

import org.simplestorage4j.api.ops.BlobStorageOperation;
import org.simplestorage4j.api.ops.BlobStorageOperationExecContext;

import lombok.val;

/**
 * implementation of BlobStorageOperationsRunner using parallel threads ExecutorService
 */
public class ParallelBlobStorageOperationsQueuePoller extends AbstractBlobStorageOperationQueuePoller {

	private final ExecutorService executorService;
	
	private final int maxParallelSubmittedCount;
	
	private final Object lock = new Object();

	// @GuardedBy("lock")
	private int currParallelSubmittedCount;
	// @GuardedBy("lock")
	private int currParallelProcessingCount;
	
	// ------------------------------------------------------------------------
	
	public ParallelBlobStorageOperationsQueuePoller( //
			BlobStorageJobOperationsExecQueue queue, BlobStorageOperationExecContext execCtx, //
			ExecutorService executorService,
			int maxParallelSubmittedCount) {
		super(queue, execCtx);
		this.executorService = executorService;
		this.maxParallelSubmittedCount = maxParallelSubmittedCount;
	}
	
	// ------------------------------------------------------------------------

	public int getCurrParallelSubmittedCount() {
		synchronized (lock) {
			return currParallelSubmittedCount;
		}
	}

	public int getCurrParallelProcessingCount() {
		synchronized (lock) {
			return currParallelProcessingCount;
		}
	}
	
	@Override
	public void run() {
		main_loop: for(;;) {
			if (isInterruptRequested()) {
				break;
			}

			val polled = queue.poll();
			if (polled == null) {
				break;
			}
			
			for(;;) {
				synchronized (lock) {
					if (currParallelSubmittedCount < maxParallelSubmittedCount) {
						currParallelSubmittedCount++;
						break;
					}
				}
				try {
					Thread.sleep(1000);
				} catch(InterruptedException ex) {
					queue.onOpRequeue(polled);
					break main_loop;
				}
			}
			
			executorService.execute(() -> doExecOp(polled));
		}
	}

	private void doExecOp(BlobStorageOperation op) {
		synchronized (lock) {
			currParallelSubmittedCount--;
			currParallelProcessingCount++;
		}
		try {
			val opResult = op.execute(execCtx);
			
			queue.onOpExecuted(opResult);
		} catch(Throwable ex) {
			queue.onOpUnexpectedError(ex, op);
		} finally {
			synchronized (lock) {
				currParallelProcessingCount--;
			}
		}
	}

}
