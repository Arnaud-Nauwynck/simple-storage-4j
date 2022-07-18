package org.simplestorage4j.api.iocost.dto;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class QueueStatsDTO implements Serializable {
	
	/** */
	private static final long serialVersionUID = 1L;
	
	// public long startTime;
	// public long elapsedMillis;
	
	public int queuedOpsCount;
	public int runningOpsCount;
	public int doneOpsCount;
	public int errorOpsCount;
	
	public PerBlobStoragesPreEstimateIOCostDTO perStorageQueuedPreEstimateIOCosts;
	
	public PerBlobStoragesPreEstimateIOCostDTO perStorageRunningPreEstimateIOCosts;
	
	public PerBlobStoragesIOTimeResultDTO perStorageDoneStats;
	
	public PerBlobStoragesIOTimeResultDTO perStorageErrorStats;

}
