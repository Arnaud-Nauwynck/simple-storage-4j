package org.simplestorage4j.opsserver.service;

import java.util.Map;

import org.simplestorage4j.api.ops.executor.BlobStorageJobOperationsExecQueue;
import org.simplestorage4j.opscommon.dto.queue.JobQueueStatsDTO;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class JobQueueEntry {
	
	public final long jobId;
	public final long createTime;
	public final String displayMessage;
	public final Map<String,String> props;

	public final BlobStorageJobOperationsExecQueue queue;

	@Getter
	private boolean pollingActive; // else suspended
	@Getter
	private long lastPollingActiveChangedTime;
	@Getter
	private long totalElapsedPollingActiveTime;
	@Getter
	private long totalElapsedPollingSuspendedTime;

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
		val queueStats = queue.getQueueStatsDTO();
		val elapsedSinceChanged = System.currentTimeMillis() - lastPollingActiveChangedTime;
		return new JobQueueStatsDTO(jobId,
				pollingActive,
				lastPollingActiveChangedTime,
				totalElapsedPollingActiveTime + ((pollingActive)? elapsedSinceChanged : 0),
				totalElapsedPollingSuspendedTime + ((! pollingActive)? elapsedSinceChanged : 0),
				queueStats);
	}

}
