package org.simplestorage4j.opsserver.service;

import java.util.Map;

import org.simplestorage4j.api.ops.executor.BlobStorageJobOperationsExecQueue.BlobStorageOperationsQueueDTO;
import org.simplestorage4j.api.ops.executor.BlobStorageJobOperationsPersistedQueue;
import org.simplestorage4j.opscommon.dto.queue.JobQueueStatsDTO;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class StorageJobOpsQueueEntry {
	
	public final long jobId;
	public final long createTime;
	public final String displayMessage;
	public final Map<String,String> props;

	/*pp*/ final BlobStorageJobOperationsPersistedQueue queue;

	@Getter
	private boolean pollingActive; // else suspended
	@Getter
	private long lastPollingActiveChangedTime;
	@Getter
	private long totalElapsedPollingActiveTime;
	@Getter
	private long totalElapsedPollingSuspendedTime;

	// ------------------------------------------------------------------------
	
	public void setPollingActive(boolean p) {
		if (pollingActive == p) {
			return;
		}
		this.pollingActive = p;
		val now = System.currentTimeMillis();
		if (lastPollingActiveChangedTime == 0) {
			lastPollingActiveChangedTime = now; 
			return;
		}
		val elapsed = now - lastPollingActiveChangedTime;
		this.lastPollingActiveChangedTime = now;
		if (pollingActive) {
			this.totalElapsedPollingSuspendedTime += elapsed;
			log.info("resume job: " + jobId + " (was suspended since " + elapsed + " ms)");
		} else {
			this.totalElapsedPollingActiveTime += elapsed;
			log.info("suspend job: " + jobId + " (was active since " + elapsed + " ms)");
		}
	}

	public JobQueueStatsDTO toJobQueueStats() {
		val queueStats = queue.toQueueStatsDTO();
		val elapsedSinceChanged = System.currentTimeMillis() - lastPollingActiveChangedTime;
		return new JobQueueStatsDTO(jobId,
				pollingActive,
				lastPollingActiveChangedTime,
				totalElapsedPollingActiveTime + ((pollingActive)? elapsedSinceChanged : 0),
				totalElapsedPollingSuspendedTime + ((! pollingActive)? elapsedSinceChanged : 0),
				queueStats);
	}

	/** persistent Data / DTO for JobQueueEntry */
	@NoArgsConstructor @AllArgsConstructor
	public static class JobQueueData {
		
		public long jobId;
		public long createTime;
		public String displayMessage;
		public Map<String,String> props;

		public BlobStorageOperationsQueueDTO queueData;
		
		public boolean pollingActive; // else suspended
		public long lastPollingActiveChangedTime;
		public  long totalElapsedPollingActiveTime;
		public  long totalElapsedPollingSuspendedTime;
	}
	
	public JobQueueData toData() {
		val queueData = queue.toQueueDTO();
		return new JobQueueData(jobId, createTime, displayMessage, props, //
				queueData, //
				pollingActive, lastPollingActiveChangedTime, totalElapsedPollingActiveTime, totalElapsedPollingSuspendedTime);
	}
	
	public static StorageJobOpsQueueEntry fromData(JobQueueData src, BlobStorageJobOperationsPersistedQueue queue) {
		val res = new StorageJobOpsQueueEntry(src.jobId, src.createTime, src.displayMessage, src.props, queue);
		res.pollingActive = src.pollingActive;
		res.lastPollingActiveChangedTime = src.lastPollingActiveChangedTime;
		res.totalElapsedPollingActiveTime = src.totalElapsedPollingActiveTime;
		res.totalElapsedPollingSuspendedTime = src.totalElapsedPollingSuspendedTime;
		return res;
	}

	public boolean hasRemainOps() {
		return queue.hasRemainOps();
	}
	
}
