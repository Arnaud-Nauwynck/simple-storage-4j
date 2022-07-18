package org.simplestorage4j.api.ops.executor;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.simplestorage4j.api.iocost.counter.PerBlobStoragesIOTimeCounter;
import org.simplestorage4j.api.iocost.counter.PerBlobStoragesPreEstimateIOCostCounter;
import org.simplestorage4j.api.iocost.dto.QueueStatsDTO;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesIOTimeResult;
import org.simplestorage4j.api.ops.BlobStorageOperation;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * Queue for BlobStorageOperation execution
 */
@Slf4j
public class BlobStorageOperationExecQueue {

	private final Object lock = new Object();

	private final boolean keepDoneOps;
	
	/**
	 * operation ready to be executed
	 */
	private final ArrayDeque<BlobStorageOperation> queuedOps = new ArrayDeque<>();
	
	/**
	 * operation polled by caller and running, to be marked later as done/error.
	 */
	private final Map<Integer,BlobStorageOperation> runningOps = new HashMap<>();
	
	// TOADD ops that need to wait for dependencies ops to be finished
//	private final Map<Integer,BlobStorageOperation> waitingOps = new HashMap<>();
	
	/**
	 * optionnal, operations fully done. can be purged
	 */
	private final List<BlobStorageOperation> doneOps;
	
	private int doneOpsCount;
	
	/**
	 * operations that failed, and may be retryed later if possible
	 */
	private final Map<Integer,BlobStorageOperation> errorOps = new HashMap<>();

	private final PerBlobStoragesPreEstimateIOCostCounter queuePreEstimateIOCosts = new PerBlobStoragesPreEstimateIOCostCounter(); 
	
	private final PerBlobStoragesPreEstimateIOCostCounter runningPreEstimateIOCosts = new PerBlobStoragesPreEstimateIOCostCounter(); 
	
	private final PerBlobStoragesIOTimeCounter perStoragesIOTimeCounter = new PerBlobStoragesIOTimeCounter();  
	
	private final PerBlobStoragesIOTimeCounter perStoragesErrorIOTimeCounter = new PerBlobStoragesIOTimeCounter();  
	
	private final BlobStorageOperationExecQueueHook opHook;
	
	// ------------------------------------------------------------------------
	
	public BlobStorageOperationExecQueue(BlobStorageOperationExecQueueHook opHook) {
		this(opHook, true, Collections.emptyList());
	}
	
	public BlobStorageOperationExecQueue(
			BlobStorageOperationExecQueueHook opHook,
			boolean keepDoneOps, 
			Collection<BlobStorageOperation> queuedOps) {
		this.opHook = opHook;
		this.keepDoneOps = keepDoneOps;
		this.doneOps = (keepDoneOps)? new ArrayList<>() : null;
		if (queuedOps != null) {
			this.queuedOps.addAll(queuedOps);
			for(val op: queuedOps) {
				val opPreEstimateCost = op.preEstimateExecutionCost();
				queuePreEstimateIOCosts.incr(opPreEstimateCost);
			}
		}
	}
	
	// ------------------------------------------------------------------------
	
	public void addOp(BlobStorageOperation op) {
		synchronized(lock) {
			this.queuedOps.add(op);
			val opPreEstimateCost = op.preEstimateExecutionCost();
			queuePreEstimateIOCosts.incr(opPreEstimateCost);
		}
		if (opHook != null) {
			opHook.onAddOp(op);
		}
	}

	public void addOps(Collection<BlobStorageOperation> ops) {
		synchronized(lock) {
			this.queuedOps.addAll(ops);
			for(val op: queuedOps) {
				val opPreEstimateCost = op.preEstimateExecutionCost();
				queuePreEstimateIOCosts.incr(opPreEstimateCost);
			}
		}
		if (opHook != null) {
			for(val op: ops) {
				opHook.onAddOp(op);
			}
		}
	}

	/**
	 * @return polled op, transfered to 'runningOps'.. to be marked when done/failed by caller
	 */
	public BlobStorageOperation poll() {
		BlobStorageOperation res;
		synchronized(lock) {
			res = queuedOps.poll();
			if (res == null) {
				return null;
			}
			runningOps.put(res.taskId, res);
		}
		val opPreEstimateCost = res.preEstimateExecutionCost();
		queuePreEstimateIOCosts.decr(opPreEstimateCost);
		runningPreEstimateIOCosts.incr(opPreEstimateCost);

		if (opHook != null) {
			opHook.onPolled(res);
		}
		return res;
	}
	
	public void onOpExecuted(PerBlobStoragesIOTimeResult result, BlobStorageOperation op) {
		if (result.errorMessage == null) {
			onOpExecutedSuccess(result, op);
		} else {
			onOpExecutedError(result, op);
		}
	}

	public void onOpExecuted(PerBlobStoragesIOTimeResult result, int taskId) {
		if (result.errorMessage == null) {
			onOpExecutedSuccess(result, taskId);
		} else {
			onOpExecutedError(result, taskId);
		}
	}
	
	protected void onOpExecutedSuccess(PerBlobStoragesIOTimeResult result, BlobStorageOperation op) {
		synchronized(lock) {
			runningOps.remove(op.taskId);
			this.doneOpsCount++;
			if (keepDoneOps) {
				doneOps.add(op);
			}
		}
		val opPreEstimateCost = op.preEstimateExecutionCost();
		runningPreEstimateIOCosts.decr(opPreEstimateCost);
		perStoragesIOTimeCounter.incr(result);
		
		// TOADD update op stats per storage... for speed / counters 
		
		if (opHook != null) {
			opHook.onOpExecutedSuccess(result, op);
		}
	}

	protected void onOpExecutedError(PerBlobStoragesIOTimeResult result, BlobStorageOperation op) {
		synchronized(lock) {
			runningOps.remove(op.taskId);
			errorOps.put(op.taskId, op);
		}
		val opPreEstimateCost = op.preEstimateExecutionCost();
		runningPreEstimateIOCosts.decr(opPreEstimateCost);
		perStoragesIOTimeCounter.incr(result);
		perStoragesErrorIOTimeCounter.incr(result);
		
		// TOADD update op stats per storage... for speed / counters 
		
		if (opHook != null) {
			opHook.onOpExecutedError(result, op);
		}
	}
	
	protected void onOpExecutedSuccess(PerBlobStoragesIOTimeResult result, int taskId) {
		BlobStorageOperation op;
		synchronized(lock) {
			op = runningOps.remove(taskId);
			if (op == null) {
				log.warn("op taskId:" + taskId + " not found to handle onOpExecutedSuccess() .. ignore!");
				return;
			}
			this.doneOpsCount++;
			if (keepDoneOps) {
				doneOps.add(op);
			}
		}
		val opPreEstimateCost = op.preEstimateExecutionCost();
		runningPreEstimateIOCosts.decr(opPreEstimateCost);
		perStoragesIOTimeCounter.incr(result);

		// TOADD update op stats per storage... for speed / counters 

		if (opHook != null) {
			opHook.onOpExecutedSuccess(result, op);
		}
	}

	protected void onOpExecutedError(PerBlobStoragesIOTimeResult result, int taskId) {
		BlobStorageOperation op;
		synchronized(lock) {
			op = runningOps.remove(taskId);
			if (op == null) {
				log.warn("op taskId:" + taskId + " not found to handle onOpExecutedError() .. ignore!");
				return;
			}
			errorOps.put(op.taskId, op);
		}
		val opPreEstimateCost = op.preEstimateExecutionCost();
		runningPreEstimateIOCosts.decr(opPreEstimateCost);
		perStoragesIOTimeCounter.incr(result);
		perStoragesErrorIOTimeCounter.incr(result);

		// TOADD update op stats per storage... for speed / counters 

		if (opHook != null) {
			opHook.onOpExecutedError(result, op);
		}
	}
	
	protected void onOpUnexpectedError(Throwable ex, BlobStorageOperation op) {
		synchronized(lock) {
			runningOps.remove(op.taskId);
			errorOps.put(op.taskId, op);
		}
		val opPreEstimateCost = op.preEstimateExecutionCost();
		runningPreEstimateIOCosts.decr(opPreEstimateCost);
		if (opHook != null) {
			opHook.onOpUnexpectedError(ex, op);
		}
	}

	public void onOpRequeue(BlobStorageOperation op) {
		synchronized(lock) {
			runningOps.remove(op.taskId);
			queuedOps.addFirst(op);
		}
		val opPreEstimateCost = op.preEstimateExecutionCost();
		runningPreEstimateIOCosts.decr(opPreEstimateCost);
		queuePreEstimateIOCosts.incr(opPreEstimateCost);
		if (opHook != null) {
			opHook.onOpRequeue(op);
		}
	}

	public List<BlobStorageOperation> purgeDoneOps() {
		if (! keepDoneOps) {
			return null;
		}
		synchronized(lock) {
			val res = new ArrayList<>(doneOps);
			this.doneOps.clear();
			return res;
		}
	}

	public QueueStatsDTO getQueueStatsDTO() {
		synchronized(lock) {
			val queuedCount = this.queuedOps.size();
			val runningOpsCount = this.runningOps.size();
			val doneOpsCount = this.doneOpsCount;
			val errorOpsCount = this.errorOps.size();

			val perStorageQueuedPreEstimateIOCosts = queuePreEstimateIOCosts.toDTO();
			val perStorageRunningPreEstimateIOCosts = runningPreEstimateIOCosts.toDTO();
			val perStorageDoneStats = perStoragesIOTimeCounter.toDTO();
			val perStorageErrorStats = perStoragesErrorIOTimeCounter.toDTO();
			return new QueueStatsDTO(queuedCount, runningOpsCount, doneOpsCount, errorOpsCount, //
					perStorageQueuedPreEstimateIOCosts, perStorageRunningPreEstimateIOCosts, //
					perStorageDoneStats, perStorageErrorStats);
		}
	}

}
