package org.simplestorage4j.api.ops.executor;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.GuardedBy;

import org.simplestorage4j.api.iocost.counter.PerBlobStoragesIOTimeCounter;
import org.simplestorage4j.api.iocost.counter.PerBlobStoragesPreEstimateIOCostCounter;
import org.simplestorage4j.api.iocost.dto.QueueStatsDTO;
import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.ops.BlobStorageOperation;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * in-memory Queue for BlobStorageOperation execution: { queuedOps,runningOps,doneOps,errorOps, statistics }
 * 
 * cf also hook for notifying operation done / added / empty.. 
 * which is used for persisting
 */
@Slf4j
public class BlobStorageJobOperationsExecQueue {

	@Getter
	private final long jobId;

	private final boolean keepDoneOps;

	private final Object lock = new Object();

	private AtomicLong taskIdGenerator = new AtomicLong();

	/**
	 * operation ready to be executed
	 */
	@GuardedBy("lock")
	private final ArrayDeque<BlobStorageOperation> queuedOps = new ArrayDeque<>();

	/**
	 * operation polled by caller and running, to be marked later as done/error.
	 */
	@GuardedBy("lock")
	private final Map<Long,BlobStorageOperation> runningOps = new HashMap<>();

	// TOADD ops that need to wait for dependencies ops to be finished
	//	private final Map<Integer,BlobStorageOperation> waitingOps = new HashMap<>();

	/**
	 * optionnal, operations fully done. can be purged
	 */
	@GuardedBy("lock")
	private final List<BlobStorageOperation> doneOps;

	@GuardedBy("lock")
	private int doneOpsCount;

	/**
	 * operations that failed, and may be retryed later if possible
	 */
	@GuardedBy("lock")
	private final Map<Long,BlobStorageOperationError> errorOps = new HashMap<>();

	@GuardedBy("lock")
	private final Map<Long,BlobStorageOperationWarning> warningOps = new HashMap<>();

	private final PerBlobStoragesPreEstimateIOCostCounter queuePreEstimateIOCosts = new PerBlobStoragesPreEstimateIOCostCounter();

	private final PerBlobStoragesPreEstimateIOCostCounter runningPreEstimateIOCosts = new PerBlobStoragesPreEstimateIOCostCounter();

	private final PerBlobStoragesIOTimeCounter perStoragesIOTimeCounter = new PerBlobStoragesIOTimeCounter();

	private final PerBlobStoragesIOTimeCounter perStoragesErrorIOTimeCounter = new PerBlobStoragesIOTimeCounter();

	private final BlobStorageOperationExecQueueHook opHook;

	// ------------------------------------------------------------------------

	public BlobStorageJobOperationsExecQueue(long jobId, BlobStorageOperationExecQueueHook opHook, boolean keepDoneOps) {
		this(jobId, opHook, keepDoneOps, Collections.emptyList());
	}

	public BlobStorageJobOperationsExecQueue(
			long jobId,
			BlobStorageOperationExecQueueHook opHook,
			boolean keepDoneOps,
			Collection<BlobStorageOperation> queuedOps) {
		this.jobId = jobId;
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
	
	public void setReloadedData(BlobStorageOperationsQueueDTO data, List<BlobStorageOperationResult> doneResults) {
		this.taskIdGenerator.set(data.taskIdGenerator);
		if (doneResults != null) {
			this.doneOpsCount = doneResults.size();
			for(val opResult : doneResults) {
				this.perStoragesIOTimeCounter.incr(opResult.ioTimePerStorage);
			}
		}
	}

	// ------------------------------------------------------------------------

	public long newTaskId() {
		return taskIdGenerator.incrementAndGet();
	}

	public long newTaskIdsRange(int len) {
		return taskIdGenerator.addAndGet(len);
	}

	public void addOp(BlobStorageOperation op) {
		// TOADD ensure jobId + unique taskId
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
		// TOADD ensure jobId + unique taskId
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

	public void reevalQueuePreEstimateIOCosts() {
		queuePreEstimateIOCosts.clear();
		synchronized(lock) {
			for(val op: queuedOps) {
				val opPreEstimateCost = op.preEstimateExecutionCost();
				queuePreEstimateIOCosts.incr(opPreEstimateCost);
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

	public void onOpExecuted(BlobStorageOperationResult result) {
		if (result.errorMessage == null) {
			onOpExecutedSuccess(result);
		} else {
			onOpExecutedError(result);
		}
	}

	protected void onOpExecutedSuccess(BlobStorageOperationResult result) {
		BlobStorageOperation op;
		val taskId = result.taskId;
		boolean finished;
		synchronized(lock) {
			op = runningOps.remove(taskId);
			if (op == null) {
				// throw new IllegalArgumentException("running op not found for " + taskId);
				log.error("running op not found for " + taskId + " .. ignore onOpExecutedSuccess");
				return;
			}
			this.doneOpsCount++;
			if (keepDoneOps) {
				doneOps.add(op);
			}
			if (result.warnings != null && !result.warnings.isEmpty()) {
				warningOps.put(taskId, new BlobStorageOperationWarning(op, result.warnings));
			}
			finished = queuedOps.isEmpty() && runningOps.isEmpty();
		}
		val opPreEstimateCost = op.preEstimateExecutionCost();
		runningPreEstimateIOCosts.decr(opPreEstimateCost);
		perStoragesIOTimeCounter.incr(result.ioTimePerStorage);

		// TOADD update op stats per storage... for speed / counters

		if (opHook != null) {
			opHook.onOpExecutedSuccess(result, op);
			if (finished) {
				opHook.onFinished();
			}
		}
	}

	protected void onOpExecutedError(BlobStorageOperationResult result) {
		BlobStorageOperation op;
		val taskId = result.taskId;
		boolean finished;
		synchronized(lock) {
			op = runningOps.remove(taskId);
			if (op == null) {
				// throw new IllegalArgumentException("running op not found for " + taskId);
				log.error("running op not found for " + taskId + " .. ignore onOpExecutedError");
			}
			val errorOp = errorOps.computeIfAbsent(taskId, x -> new BlobStorageOperationError(op));
			errorOp.addError(result);
			finished = queuedOps.isEmpty() && runningOps.isEmpty();
		}
		val opPreEstimateCost = op.preEstimateExecutionCost();
		runningPreEstimateIOCosts.decr(opPreEstimateCost);
		perStoragesIOTimeCounter.incr(result.ioTimePerStorage);
		perStoragesErrorIOTimeCounter.incr(result.ioTimePerStorage);

		// TOADD update op stats per storage... for speed / counters

		if (opHook != null) {
			opHook.onOpExecutedError(result, op);
			if (finished) {
				opHook.onFinished();
			}
		}
	}

	protected void onOpUnexpectedError(Throwable ex, BlobStorageOperation op) {
		boolean finished;
		val taskId = op.taskId;
		log.error("Failed " + op, ex);
		synchronized(lock) {
			runningOps.remove(taskId);
			val errorOp = errorOps.computeIfAbsent(taskId, x -> new BlobStorageOperationError(op));
			errorOp.unexpectedError = ex;
			finished = queuedOps.isEmpty() && runningOps.isEmpty();
		}
		val opPreEstimateCost = op.preEstimateExecutionCost();
		runningPreEstimateIOCosts.decr(opPreEstimateCost);
		if (opHook != null) {
			opHook.onOpUnexpectedError(ex, op);
			if (finished) {
				opHook.onFinished();
			}
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

	public QueueStatsDTO toQueueStatsDTO() {
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

	public List<BlobStorageOperation> listRemainOps() {
		synchronized(lock) {
			return new ArrayList<>(queuedOps);
		}
	}

	public boolean hasRemainOps() {
		synchronized(lock) {
			return ! queuedOps.isEmpty();
		}
	}

	public List<BlobStorageOperationError> listOpErrors() {
		synchronized(lock) {
			return new ArrayList<>(errorOps.values());
		}
	}

	public List<BlobStorageOperationWarning> listOpWarnings() {
		synchronized(lock) {
			return new ArrayList<>(warningOps.values());
		}
	}

	// ------------------------------------------------------------------------
	
	@NoArgsConstructor @AllArgsConstructor
	@Getter @Setter
	public static class BlobStorageOperationsQueueDTO implements Serializable {

		/** */
		private static final long serialVersionUID = 1L;

		public long taskIdGenerator;

		// queuedOps => persisted in file
		// runningOps => transient NOT persisted, lost.. but will be re-executed later 
	
		// warningOps, errorOps => TOADD in separate file?..

		// queuePreEstimateIOCosts => recomputed while re-filling ops
		// runningPreEstimateIOCosts => transient
		// perStoragesIOTimeCounter, perStoragesErrorIOTimeCounter => in separate "statistics.json" file (saved periodically)

	}
	
	public BlobStorageOperationsQueueDTO toDTO() {
		return new BlobStorageOperationsQueueDTO(taskIdGenerator.get());
	}

}
