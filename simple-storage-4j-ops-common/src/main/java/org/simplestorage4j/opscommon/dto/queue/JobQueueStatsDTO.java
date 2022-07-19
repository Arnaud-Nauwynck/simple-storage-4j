package org.simplestorage4j.opscommon.dto.queue;

import java.io.Serializable;

import org.simplestorage4j.api.iocost.dto.QueueStatsDTO;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class JobQueueStatsDTO implements Serializable {
	
	/** */
	private static final long serialVersionUID = 1L;
	
	public long jobId;

	public boolean pollingActive; // else suspended
	public long lastPollingActiveChangedTime;
	public long totalElapsedPollingActiveTime;
	public long totalElapsedPollingSuspendedTime;
	
	public QueueStatsDTO queueStats;

}
