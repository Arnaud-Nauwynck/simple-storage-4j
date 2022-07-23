package org.simplestorage4j.opsserver.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.GuardedBy;

import org.simplestorage4j.api.BlobStorageId;
import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.ops.BlobStorageOperation;
import org.simplestorage4j.api.ops.MockSleepStorageOperation;
import org.simplestorage4j.api.ops.encoder.BlobStorageOperationDtoResolver;
import org.simplestorage4j.api.ops.executor.BlobStorageJobOperationsExecQueue;
import org.simplestorage4j.api.ops.executor.BlobStorageOperationError;
import org.simplestorage4j.api.ops.executor.BlobStorageOperationExecQueueHook;
import org.simplestorage4j.api.ops.executor.BlobStorageOperationWarning;
import org.simplestorage4j.api.util.BlobStorageUtils;
import org.simplestorage4j.opscommon.dto.queue.AddJobOpsQueueRequestDTO;
import org.simplestorage4j.opscommon.dto.queue.AddJobOpsQueueResponseDTO;
import org.simplestorage4j.opscommon.dto.queue.AddMockOpsToJobQueueRequestDTO;
import org.simplestorage4j.opscommon.dto.queue.AddOpsToJobQueueRequestDTO;
import org.simplestorage4j.opscommon.dto.queue.JobQueueInfoDTO;
import org.simplestorage4j.opscommon.dto.queue.JobQueueStatsDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * statefull service, holding jobOpQueues: jobId -> {@Link BlobStorageOperationExecQueue}
 */
@Service
@Slf4j
public class StorageJobOpsQueueService {

	@Autowired
	private BlobStorageOperationDtoResolver dtoMapper;

	private final Object lock = new Object();

	@GuardedBy("lock")
	private int jobIdGenerator = 0;

	/** all job queues (even finished/errors), fast lookup by jobId */
	@GuardedBy("lock")
	private Map<Long,JobQueueEntry> jobQueueById = new HashMap<>();

	/** currently active job queues: still containing queued operations, and status polling (not suspended) */
	@GuardedBy("lock")
	private List<JobQueueEntry> activeJobQueues = new ArrayList<>();

	@GuardedBy("lock")
	private int jobQueueRoundRobinIndex = 0;

	

	// ------------------------------------------------------------------------

	public long newJobId() {
		synchronized(lock) {
			val res = ++jobIdGenerator;
			// no save.. restored by computing max+1
			return res;
		}
	}

	public AddJobOpsQueueResponseDTO createJobQueue(AddJobOpsQueueRequestDTO req) {
		val opHook = new BlobStorageOperationExecQueueHook(); // TODO

		List<BlobStorageOperation> ops = (req.ops != null)? dtoMapper.dtosToOps(req.ops) : new ArrayList<>();
		val jobId = newJobId();
		boolean keepDoneOps = false;
		val queue = new BlobStorageJobOperationsExecQueue(jobId, opHook, keepDoneOps, ops);
		doAddJobQueue(jobId, req.displayMessage, req.props, queue);
		return new AddJobOpsQueueResponseDTO(jobId);
	}

	public void deleteJobQueue(long jobId) {
		synchronized(lock) {
			val entry = jobQueueById.remove(jobId);
			if (entry == null) {
				log.error("jobId: " + jobId + " not found .. nothing to remove");
				return;
			}
			activeJobQueues.remove(entry);
			saveJobQueues();
		}
	}

	public void suspendJobQueue(long jobId) {
		synchronized(lock) {
			val entry = jobQueueById.get(jobId);
			if (entry == null) {
				throw new IllegalArgumentException("jobId: " + jobId + " not found .. nothing to suspend");
			}
			if (! entry.isPollingActive()) {
				return;
			}
			entry.setPollingActive(false);
			activeJobQueues.remove(entry);
			saveJobQueues();
		}
	}

	public void resumeJobQueue(long jobId) {
		synchronized(lock) {
			val entry = jobQueueById.get(jobId);
			if (entry == null) {
				log.error("jobId: " + jobId + " not found .. nothing to resume");
				return;
			}
			if (entry.isPollingActive()) {
				return;
			}
			entry.setPollingActive(true);
			if (entry.queue.hasRemainOps()) {
				activeJobQueues.add(entry);
			}
			saveJobQueues();
		}
	}

	public void addOpsToJobQueue(AddOpsToJobQueueRequestDTO req) {
		val ops = dtoMapper.dtosToOps(req.ops);
		synchronized(lock) {
			val entry = jobQueueById.get(req.jobId);
			boolean hasRemainOpsBefore = entry.queue.hasRemainOps(); 
			entry.queue.addOps(ops);
			if (entry.isPollingActive() && ! hasRemainOpsBefore) {
				activeJobQueues.add(entry);
			}
		}
	}

	public void addMockOpsToJobQueue(AddMockOpsToJobQueueRequestDTO req) {
		synchronized(lock) {
			val jobId = req.jobId;
			val entry = jobQueueById.get(jobId);
			val hasRemainOpsBefore = entry.queue.hasRemainOps(); 
			int mockOpsCount = req.mockOpsCount;
			val mockOps = new ArrayList<BlobStorageOperation>(mockOpsCount);
			val mockDurationMillis = req.mockOpsDurationMillis;
			val srcStorageId = (req.srcStorageId != null)? BlobStorageId.of(req.srcStorageId) : null;
			val destStorageId = (req.destStorageId != null)? BlobStorageId.of(req.destStorageId) : null;
			val mockSrcFileLen = req.mockSrcFileLen;
			val mockDestFileLen = req.mockDestFileLen;
			for(int i = 0; i < mockOpsCount; i++) {
				val taskId = entry.queue.newTaskId();
				val mockOp = new MockSleepStorageOperation(jobId, taskId, 
						mockDurationMillis, srcStorageId, destStorageId,
						mockSrcFileLen, mockDestFileLen
						);
				mockOps.add(mockOp);
			}
			entry.queue.addOps(mockOps);
			if (entry.isPollingActive() && ! hasRemainOpsBefore) {
				activeJobQueues.add(entry);
			}
		}
	}

	public JobQueueInfoDTO getJobQueueInfo(long jobId) {
		synchronized(lock) {
			val entry = jobQueueById.get(jobId);
			if (entry == null) {
				return null;
			}
			return toJobQueueInfoDTO(entry);
		}
	}

	public List<JobQueueInfoDTO> getJobQueueInfos() {
		synchronized(lock) {
			return BlobStorageUtils.map(jobQueueById.values(), x -> toJobQueueInfoDTO(x));
		}
	}

	protected JobQueueInfoDTO toJobQueueInfoDTO(JobQueueEntry src) {
		return new JobQueueInfoDTO(src.jobId,
				src.createTime, src.displayMessage, src.props);
	}

	public JobQueueStatsDTO getJobQueueStats(long jobId) {
		synchronized(lock) {
			val entry = jobQueueById.get(jobId);
			if (entry == null) {
				return null;
			}
			return entry.toJobQueueStats();
		}
	}

	public List<JobQueueStatsDTO> getActiveJobQueuesStats() {
		synchronized(lock) {
			return BlobStorageUtils.map(activeJobQueues, x -> x.toJobQueueStats());
		}
	}

	public List<JobQueueStatsDTO> getAllJobQueuesStats() {
		synchronized(lock) {
			return BlobStorageUtils.map(jobQueueById.values(), x -> x.toJobQueueStats());
		}
	}

	public BlobStorageOperation pollOp(ExecutorSessionEntry session) {
		BlobStorageOperation res = null;
		synchronized(lock) {
			int len = activeJobQueues.size();
			if (len == 0) {
				return null;
			}
			for(int tryI = 0; tryI < len; tryI++) {
				jobQueueRoundRobinIndex++;
				if (jobQueueRoundRobinIndex >= len) {
					jobQueueRoundRobinIndex = 0;
				}
				val entry = activeJobQueues.get(jobQueueRoundRobinIndex);
				res = entry.queue.poll();
				if (res != null) {
					session.addPolledOp(res);
					if (! entry.queue.hasRemainOps()) {
						activeJobQueues.remove(entry);
						len = activeJobQueues.size();
					}
					return res;
				}
			}
		}
		return res;
	}

	public List<BlobStorageOperation> pollOps(ExecutorSessionEntry session, int count) {
		val res = new ArrayList<BlobStorageOperation>();
		for(int i = 0; i < count; i++) {
			val op = pollOp(session);
			if (op == null) {
				break;
			}
			res.add(op);
		}
		return res;
	}

	public void onExecutorStop_reputPolledTasks(
			ExecutorSessionEntry session, 
			List<PolledBlobStorageOperationEntry> polledJobTasks) {
		synchronized(lock) {
			for(val polled: polledJobTasks) {
				val op = polled.op;
				val jobId = op.jobId;
				val entry = jobQueueById.get(jobId);
				if (entry == null) {
					log.warn("jobId " + jobId + " not found to reput polled task? ..ignore");
					continue;
				}
				session.removePolledOp(op.toId());
				val hasRemainOpsBefore = entry.queue.hasRemainOps();
				entry.queue.onOpRequeue(op);
				if (entry.isPollingActive() && ! hasRemainOpsBefore) {
					activeJobQueues.add(entry);
				}
			}
		}
	}

	public void onOpsFinished(ExecutorSessionEntry session, Collection<BlobStorageOperationResult> opResults) {
		synchronized(lock) {
			long prevJobId = 0;
			JobQueueEntry prevJobEntry = null;
			for(val opResult: opResults) {
				val jobId = opResult.jobId;
				JobQueueEntry jobEntry; 
				if (prevJobId == jobId) {
					jobEntry = prevJobEntry;
				} else {
					jobEntry = jobQueueById.get(jobId);
					prevJobId = jobId;
					prevJobEntry = jobEntry;
				}
				val opId = opResult.toId();
				if (jobEntry == null) {
					log.warn("job not found to handle finished op " + opId + " ? ..ignore");
					continue;
				}
				session.removePolledOp(opId);
				jobEntry.queue.onOpExecuted(opResult);
			}
		}
	}

	public void onOpFinished(ExecutorSessionEntry session, BlobStorageOperationResult opResult) {
		synchronized(lock) {
			val jobId = opResult.jobId;
			val jobEntry = jobQueueById.get(jobId);
			val opId = opResult.toId();
			if (jobEntry == null) {
				log.warn("job not found to handle finished op " + opId + " ? ..ignore");
				return;
			}
			session.removePolledOp(opId);
			jobEntry.queue.onOpExecuted(opResult);
		}
	}

	public List<BlobStorageOperation> listJobQueueRemainOps(long jobId) {
		List<BlobStorageOperation> res;
		synchronized(lock) {
			val jobEntry = jobQueueById.get(jobId);
			if (jobEntry == null) {
				return new ArrayList<>();
			}
			res = jobEntry.queue.listRemainOps();
		}
		return res;
	}

	public List<BlobStorageOperationError> listJobQueueErrors(long jobId) {
		synchronized(lock) {
			val jobEntry = jobQueueById.get(jobId);
			if (jobEntry == null) {
				return new ArrayList<>();
			}
			return jobEntry.queue.listOpErrors();
		}
	}

	public List<BlobStorageOperationWarning> listJobQueueWarnings(long jobId) {
		synchronized(lock) {
			val jobEntry = jobQueueById.get(jobId);
			if (jobEntry == null) {
				return new ArrayList<>();
			}
			return jobEntry.queue.listOpWarnings();
		}
	}

	public List<BlobStorageOperationError> listJobQueuesErrors() {
		val res = new ArrayList<BlobStorageOperationError>();
		synchronized(lock) {
			for(val jobEntry: jobQueueById.values()) {
				val jobErrors = jobEntry.queue.listOpErrors();
				if (jobErrors != null && !jobErrors.isEmpty()) {
					res.addAll(jobErrors);
				}
			}
		}
		return res;
	}

	public List<BlobStorageOperationWarning> listJobQueuesWarnings() {
		val res = new ArrayList<BlobStorageOperationWarning>();
		synchronized(lock) {
			for(val jobEntry: jobQueueById.values()) {
				val jobWarnings = jobEntry.queue.listOpWarnings();
				if (jobWarnings != null && !jobWarnings.isEmpty()) {
					res.addAll(jobWarnings);
				}
			}
		}
		return res;
	}

	// internal
	// ------------------------------------------------------------------------

	protected void doAddJobQueue(
			long jobId,
			String displayMessage,
			Map<String,String> props,
			BlobStorageJobOperationsExecQueue queue) {
		val startTime = System.currentTimeMillis();
		synchronized(lock) {
			val entry = new JobQueueEntry(jobId, startTime, displayMessage, props, queue);
			entry.setPollingActive(true);

			if (queue.hasRemainOps()) {
				activeJobQueues.add(entry);
			}
			jobQueueById.put(jobId, entry);
			saveJobQueues();
		}
	}

	private void saveJobQueues() {
		// TODO
		
	}

}
