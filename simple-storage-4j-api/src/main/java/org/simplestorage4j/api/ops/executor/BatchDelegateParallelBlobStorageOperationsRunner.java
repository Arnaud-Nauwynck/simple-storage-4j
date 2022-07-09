package org.simplestorage4j.api.ops.executor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.simplestorage4j.api.iocost.immutable.BlobStoragePreEstimateIOCost;
import org.simplestorage4j.api.ops.BlobStorageOperation;

import lombok.val;

/**
 * implementation of BlobStorageOperationsRunner using parallel threads ExecutorService
 */
public class BatchDelegateParallelBlobStorageOperationsRunner extends AbstractBlobStorageOperationRunner {

	private final int maxTotalIOPerBatch;
	private final int maxOpPerBatch;
	private final BlobStorageOperationBatchExecutor delegate;

	private final ExecutorService executorService;
	private final int maxParallelSubmittedBatchCount;
	
	private final Object lock = new Object();

	// @GuardedBy("lock")
	private int currParallelSubmittedBatchCount;
	// @GuardedBy("lock")
	private int currParallelProcessingBatchCount;
	
	// ------------------------------------------------------------------------
	
	public BatchDelegateParallelBlobStorageOperationsRunner(BlobStorageOperationExecQueue queue, //
			BlobStorageOperationBatchExecutor delegate, int maxTotalIOPerBatch, int maxOpPerBatch, //
			ExecutorService executorService, int maxParallelSubmittedCount //
			) {
		super(queue);
		this.delegate = delegate;
		this.maxTotalIOPerBatch = maxTotalIOPerBatch;
		this.maxOpPerBatch = maxOpPerBatch;
		this.executorService = executorService;
		this.maxParallelSubmittedBatchCount = maxParallelSubmittedCount;
	}
	
	// ------------------------------------------------------------------------

	public int getCurrParallelSubmittedBatchCount() {
		synchronized (lock) {
			return currParallelSubmittedBatchCount;
		}
	}

	public int getCurrParallelProcessingBatchCount() {
		synchronized (lock) {
			return currParallelProcessingBatchCount;
		}
	}
	
	@Override
	public void run() {
		main_loop: for(;;) {
			if (isInterruptRequested()) {
				break;
			}

			// poll many operations to put in a single batch
			val currOpBatch = new ArrayList<BlobStorageOperation>();
			
			BlobStoragePreEstimateIOCost batchIOCost = BlobStoragePreEstimateIOCost.ofIoRead(0, 0);
			for(int i = 0; i < maxOpPerBatch; i++) {
				val polled = queue.poll();
				if (polled == null) {
					break;
				}
				currOpBatch.add(polled);
				
				val opIOCost = polled.preEstimateExecutionCost();
				for(val cost : opIOCost.perStorage.values()) {
					batchIOCost = BlobStoragePreEstimateIOCost.sum(batchIOCost, cost);
				}
				if (batchIOCost.ioReadLen + batchIOCost.ioWriteLen > maxTotalIOPerBatch) {
					break;
				}
			}
			
			if (currOpBatch.isEmpty()) {
				break; // finished
			}
			
			// wait to submit
			for(;;) {
				synchronized (lock) {
					if (currParallelSubmittedBatchCount < maxParallelSubmittedBatchCount) {
						currParallelSubmittedBatchCount++;
						break;
					}
				}
				try {
					Thread.sleep(1000);
				} catch(InterruptedException ex) {
					for(val polled: currOpBatch) {
						queue.onOpRequeue(polled);
					}
					break main_loop;
				}
			}
			
			executorService.execute(() -> doExecOpBatch(currOpBatch));
		}
	}

	private void doExecOpBatch(List<BlobStorageOperation> opBatch) {
		synchronized (lock) {
			currParallelSubmittedBatchCount--;
			currParallelProcessingBatchCount++;
		}
		try {
			// *** the Biggy ***
			val opResults = delegate.executeBatch(opBatch);

			val opByIds = new HashMap<Integer,BlobStorageOperation>();
			for(val op: opBatch) {
				opByIds.put(op.taskId, op);
			}
			for(val opResult: opResults) {
				val op = opByIds.get(opResult.taskId);
				queue.onOpExecuted(opResult, op);
			}
		} catch(Throwable ex) {
			for(val op: opBatch) {
				queue.onOpUnexpectedError(ex, op);
			}
		} finally {
			synchronized (lock) {
				currParallelProcessingBatchCount--;
			}
		}
	}

}
