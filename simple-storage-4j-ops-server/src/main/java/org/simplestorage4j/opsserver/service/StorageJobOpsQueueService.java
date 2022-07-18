package org.simplestorage4j.opsserver.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.concurrent.GuardedBy;

import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesIOTimeResult;
import org.simplestorage4j.api.ops.BlobStorageOperation;
import org.simplestorage4j.api.ops.executor.BlobStorageOperationExecQueue;
import org.simplestorage4j.api.ops.executor.BlobStorageOperationExecQueueHook;
import org.simplestorage4j.opscommon.dto.executor.ExecutorOpsFinishedRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPollOpRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPollOpResponseDTO;
import org.simplestorage4j.opscommon.dto.ops.BlobStorageOperationDTO;
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
	private BlobStorageDtoMapper dtoMapper;
	
	private final Object lock = new Object();
	
	@GuardedBy("lock")
	private List<JobOpQueueEntry> jobOpQueues = new ArrayList<>();

	// redundant with jobOpQueues, for fast lookup by jobId
	@GuardedBy("lock")
	private Map<Integer,JobOpQueueEntry> jobOpQueueById = new HashMap<>(); 
	
	@GuardedBy("lock")
	private int jobIdGenerator = 0;

	@RequiredArgsConstructor
	private static class JobOpQueueEntry {
		public final int jobId;
		public final long createTime;
		public final String displayMessage;
		public final Map<String,String> props;
		
		public final BlobStorageOperationExecQueue queue;
		
		boolean pollingActive; // else suspended
		long lastPollingActiveChangedTime;
		long totalElapsedPollingActiveTime;
		long totalElapsedPollingSuspendedTime;
	}
	
	// ------------------------------------------------------------------------
	
	public AddJobOpsQueueResponseDTO createJobQueue(AddJobOpsQueueRequestDTO req) {
		val opHook = new BlobStorageOperationExecQueueHook(); // TODO

		List<BlobStorageOperation> ops = (req.ops != null)? dtoMapper.dtosToOps(req.ops) : new ArrayList<>();
		val queue = new BlobStorageOperationExecQueue(opHook, false, ops);
		val jobId = createJobQueue(req.displayMessage, req.props, queue);
		return new AddJobOpsQueueResponseDTO(jobId);
	}

	public int createJobQueue(
			String displayMessage,
			Map<String,String> props,
			BlobStorageOperationExecQueue queue) {
		int jobId;
		val startTime = System.currentTimeMillis();
		synchronized(lock) {
			jobId = ++jobIdGenerator;
			val entry = new JobOpQueueEntry(jobId, startTime, displayMessage, props, queue);
			entry.pollingActive = true;
			entry.lastPollingActiveChangedTime = System.currentTimeMillis();
			
			jobOpQueues.add(entry);
			jobOpQueueById.put(jobId, entry);
		}
		return jobId;
	}

	public void deleteJobQueue(int jobId) {
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
	
	public JobQueueInfoDTO getJobQueueInfo(int jobId) {
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

	public JobQueueStatsDTO getJobQueueStats(int jobId) {
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

	public ExecutorSessionPollOpResponseDTO pollOp(ExecutorSessionPollOpRequestDTO req) {
		synchronized(lock) {
			// TODO
		}
		BlobStorageOperationDTO opDto = null; // TODO
		return new ExecutorSessionPollOpResponseDTO(opDto);
	}

	public void onExecutorStop_reputPolledTasks(ExecutorSessionEntry sessionEntry) {
		synchronized(lock) {
			for(val polled: sessionEntry.getPolledJobTasks().values()) {
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

	public void onOpsFinished(ExecutorOpsFinishedRequestDTO req) {
		val jobId = req.jobId;
		synchronized(lock) {
			val jobEntry = jobOpQueueById.get(req.jobId);
			if (jobEntry == null) {
				log.warn("jobId " + jobId + " not found to handle finished ops? ..ignore");
				return;
			}
			for(val taskResult: req.taskResults) {
				PerBlobStoragesIOTimeResult result = PerBlobStoragesIOTimeResult.fromDTO(taskResult);
				jobEntry.queue.onOpExecuted(result, taskResult.taskId);
				
				// TOADD update op stats per storage... for speed / counters 
			}
		}
	}

}
