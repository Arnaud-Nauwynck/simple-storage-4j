package org.simplestorage4j.opsserver.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.concurrent.GuardedBy;

import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.ops.BlobStorageOperation;
import org.simplestorage4j.api.ops.encoder.BlobStorageOperationDtoResolver;
import org.simplestorage4j.api.ops.executor.BlobStorageJobOperationsExecQueue;
import org.simplestorage4j.api.ops.executor.BlobStorageOperationExecQueueHook;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionStopRequestDTO;
import org.simplestorage4j.opscommon.dto.queue.AddJobOpsQueueRequestDTO;
import org.simplestorage4j.opscommon.dto.queue.AddJobOpsQueueResponseDTO;
import org.simplestorage4j.opscommon.dto.queue.AddOpsToJobQueueRequestDTO;
import org.simplestorage4j.opscommon.dto.queue.JobQueueInfoDTO;
import org.simplestorage4j.opscommon.dto.queue.JobQueueStatsDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
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
	private List<JobOpQueueEntry> jobOpQueues = new ArrayList<>();

	// redundant with jobOpQueues, for fast lookup by jobId
	@GuardedBy("lock")
	private Map<Long,JobOpQueueEntry> jobOpQueueById = new HashMap<>(); 
	
	@GuardedBy("lock")
	private int jobIdGenerator = 0;

	@GuardedBy("lock")
	private int jobOpQueueRoundRobinIndex = 0;

	@RequiredArgsConstructor
	private static class JobOpQueueEntry {
		public final long jobId;
		public final long createTime;
		public final String displayMessage;
		public final Map<String,String> props;
		
		public final BlobStorageJobOperationsExecQueue queue;
		
		boolean pollingActive; // else suspended
		long lastPollingActiveChangedTime;
		long totalElapsedPollingActiveTime;
		long totalElapsedPollingSuspendedTime;
	}
	
	// ------------------------------------------------------------------------
	
	public long newJobId() {
		synchronized(lock) {
			return ++jobIdGenerator;
		}
	}
	
	public AddJobOpsQueueResponseDTO createJobQueue(AddJobOpsQueueRequestDTO req) {
		val opHook = new BlobStorageOperationExecQueueHook(); // TODO

		List<BlobStorageOperation> ops = (req.ops != null)? dtoMapper.dtosToOps(req.ops) : new ArrayList<>();
		val jobId = newJobId();
		val queue = new BlobStorageJobOperationsExecQueue(jobId, opHook, false, ops);
		addJobQueue(jobId, req.displayMessage, req.props, queue);
		return new AddJobOpsQueueResponseDTO(jobId);
	}

	protected void addJobQueue(
			long jobId,
			String displayMessage,
			Map<String,String> props,
			BlobStorageJobOperationsExecQueue queue) {
		val startTime = System.currentTimeMillis();
		synchronized(lock) {
			val entry = new JobOpQueueEntry(jobId, startTime, displayMessage, props, queue);
			entry.pollingActive = true;
			entry.lastPollingActiveChangedTime = System.currentTimeMillis();
			
			jobOpQueues.add(entry);
			jobOpQueueById.put(jobId, entry);
		}
	}

	public void deleteJobQueue(long jobId) {
		synchronized(lock) {
			val entry = jobOpQueueById.remove(jobId);
			if (entry == null) {
				log.error("jobId: " + jobId + " not found .. nothing to remove");
				return;
			}
			jobOpQueues.remove(entry);
		}
	}
	
	public void addOpsToJobQueue(AddOpsToJobQueueRequestDTO req) {
		val ops = dtoMapper.dtosToOps(req.ops);
		synchronized(lock) {
			val entry = jobOpQueueById.get(req.jobId);
			entry.queue.addOps(ops);
		}
	}
	
	public JobQueueInfoDTO getJobQueueInfo(long jobId) {
		synchronized(lock) {
			val entry = jobOpQueueById.get(jobId);
			if (entry == null) {
				return null;
			}
			return toJobQueueInfoDTO(entry);
		}
	}

	public List<JobQueueInfoDTO> getJobQueueInfos() {
		synchronized(lock) {
			return jobOpQueues.stream().map(x -> toJobQueueInfoDTO(x)).collect(Collectors.toList());
		}
	}

	protected JobQueueInfoDTO toJobQueueInfoDTO(JobOpQueueEntry src) {
		return new JobQueueInfoDTO(src.jobId,
				src.createTime, src.displayMessage, src.props);
	}

	public JobQueueStatsDTO getJobQueueStats(long jobId) {
		synchronized(lock) {
			val entry = jobOpQueueById.get(jobId);
			if (entry == null) {
				return null;
			}
			return toJobQueueStats(entry);
		}
	}
	
	public List<JobQueueStatsDTO> getJobQueuesStats() {
		synchronized(lock) {
			return jobOpQueues.stream().map(x -> toJobQueueStats(x)).collect(Collectors.toList());
		}
	}
	
	protected JobQueueStatsDTO toJobQueueStats(JobOpQueueEntry src) {
		val queueStats = src.queue.getQueueStatsDTO();
		val elapsedSinceChanged = System.currentTimeMillis() - src.lastPollingActiveChangedTime; 
		return new JobQueueStatsDTO(src.jobId, 
				src.pollingActive,
				src.lastPollingActiveChangedTime,
				src.totalElapsedPollingActiveTime + ((src.pollingActive)? elapsedSinceChanged : 0),
				src.totalElapsedPollingSuspendedTime + ((! src.pollingActive)? elapsedSinceChanged : 0),
				queueStats);
	}

	public BlobStorageOperation pollOp(String sessionId) {
		BlobStorageOperation res = null;
		synchronized(lock) {
			val len = jobOpQueues.size();
			if (len == 0) {
				return null;
			}
			for(int tryI = 0; tryI < len; tryI++) {
				jobOpQueueRoundRobinIndex++;
				if (jobOpQueueRoundRobinIndex >= len) {
					jobOpQueueRoundRobinIndex = 0;
				}
				val entry = jobOpQueues.get(jobOpQueueRoundRobinIndex);
				res = entry.queue.poll();
				if (res != null) {
					return res;
				}
			}
		}
		return res;
	}

	public List<BlobStorageOperation> pollOps(String sessionId, int count) {
		val res = new ArrayList<BlobStorageOperation>();
		for(int i = 0; i < count; i++) {
			val op = pollOp(sessionId);
			if (op == null) {
				break;
			}
			res.add(op);
		}
		return res;
	}

	
	public void onExecutorStop_reputPolledTasks(ExecutorSessionStopRequestDTO req,
			List<PolledJobTaskEntry> polledJobTasks) {
		synchronized(lock) {
			for(val polled: polledJobTasks) {
				val jobId = polled.jobId;
				val jobOpQueueEntry = jobOpQueueById.get(jobId);
				if (jobOpQueueEntry == null) {
					log.warn("jobId " + jobId + " not found to reput polled task? ..ignore");
					continue;
				}
				jobOpQueueEntry.queue.onOpRequeue(polled.op);
			}
		}
	}

	public void onOpsFinished(Collection<BlobStorageOperationResult> opResults) {
		synchronized(lock) {
			for(val opResult: opResults) {
				val jobId = opResult.jobId;
				val jobEntry = jobOpQueueById.get(jobId);
				if (jobEntry == null) {
					log.warn("jobId " + jobId + " not found to handle finished ops? ..ignore");
					continue;
				}
				jobEntry.queue.onOpExecuted(opResult);
			}
		}
	}

	public void onOpFinished(BlobStorageOperationResult opResult) {
		synchronized(lock) {
			val jobId = opResult.jobId;
			val jobEntry = jobOpQueueById.get(jobId);
			if (jobEntry == null) {
				log.warn("jobId " + jobId + " not found to handle finished ops? ..ignore");
				return;
			}
			jobEntry.queue.onOpExecuted(opResult);
		}
	}

	public List<BlobStorageOperation> listJobQueueRemainOps(long jobId) {
		List<BlobStorageOperation> res;
		synchronized(lock) {
			val jobEntry = jobOpQueueById.get(jobId);
			if (jobEntry == null) {
				return new ArrayList<>();
			}
			res = jobEntry.queue.listRemainOps();
		}
		return res;
	}

}
