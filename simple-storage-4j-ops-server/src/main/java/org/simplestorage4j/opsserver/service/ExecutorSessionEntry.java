package org.simplestorage4j.opsserver.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.simplestorage4j.api.iocost.counter.PerBlobStoragesIOTimeCounter;
import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesIOTimeResult;
import org.simplestorage4j.api.ops.BlobStorageOperationId;
import org.simplestorage4j.api.util.BlobStorageUtils;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionUpdatePollingDTO;
import org.simplestorage4j.opscommon.dto.session.ExecutorSessionInfoDTO;
import org.simplestorage4j.opscommon.dto.session.ExecutorSessionPolledOpsDTO;
import org.simplestorage4j.opscommon.dto.session.ExecutorSessionPolledOpsDTO.BlobStoragePolledOperationDTO;
import org.simplestorage4j.opscommon.dto.session.ExecutorSessionPollingStateDTO;
import org.simplestorage4j.opscommon.dto.session.ExecutorSessionRecentIOStatsDTO;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.val;

/**
 * entry info for an Executor session 
 */
@RequiredArgsConstructor
public class ExecutorSessionEntry {
	
	public final String sessionId;
	
	public final String host;
	public final long startTime;

	@Getter
	private final Map<String,String> props;
	
	private Map<BlobStorageOperationId,PolledBlobStorageOperationEntry> polledJobTasks = new HashMap<>();

	private final PerBlobStoragesIOTimeCounter totalIOTimePerStorage = new PerBlobStoragesIOTimeCounter();
	
	// TOADD
//		private final PerBlobStoragesIOTimeCounter[] recentRddIOTimePerStorage;
	
	public long lastPingAliveTime;
	
	@Getter @Setter
	private boolean pollingSuspendRequested;
	@Getter @Setter
	private boolean pollingSuspended;

	@Getter @Setter
	private boolean stopRequested;
	@Getter @Setter
	private boolean stopping;
	
	@Getter @Setter
	private boolean killRequested;
	
	// ------------------------------------------------------------------------

	public void incrIOStats(PerBlobStoragesIOTimeResult add) {
		totalIOTimePerStorage.incr(add);
	}

	public void incrIOStats(BlobStorageOperationResult opResult) {
		incrIOStats(opResult.ioTimePerStorage);
	}

	public void incrIOStats(Collection<BlobStorageOperationResult> opResults) {
		if (opResults != null && !opResults.isEmpty()) {
			for(val opResult: opResults) {
				incrIOStats(opResult);
			}
		}
	}

	// ------------------------------------------------------------------------
	
	public Map<BlobStorageOperationId, PolledBlobStorageOperationEntry> getPolledJobTasks() {
		return polledJobTasks;
	}
	
	public List<PolledBlobStorageOperationEntry> clearGetCopyPolledJobTasks() {
		val res = new ArrayList<>(polledJobTasks.values());
		polledJobTasks.clear();
		return res;
	}

	// ------------------------------------------------------------------------
	
	public ExecutorSessionUpdatePollingDTO toPollingRespDTO() {
		return new 	ExecutorSessionUpdatePollingDTO(pollingSuspendRequested, stopRequested, killRequested);
	}

	public ExecutorSessionInfoDTO toInfoDTO() {
		return new ExecutorSessionInfoDTO(sessionId, host, startTime, props);
	}

	public ExecutorSessionPollingStateDTO toPollingStatesDTO() {
		return new ExecutorSessionPollingStateDTO(sessionId,
			lastPingAliveTime, //
			pollingSuspendRequested, pollingSuspended, // 
			stopRequested, stopping, //		
			killRequested);
	}

	public ExecutorSessionPolledOpsDTO toCurrPolledOpsDTO() {
		val polledOps = BlobStorageUtils.map(polledJobTasks.values(), 
				x -> new BlobStoragePolledOperationDTO(x.polledStartTime, x.op.toDTO()));
		return new ExecutorSessionPolledOpsDTO(sessionId, polledOps);
	}

	public ExecutorSessionRecentIOStatsDTO toRecentIOStatsDTO() {
		val totalIOTimePerStorageDto = totalIOTimePerStorage.toDTO();
		return new ExecutorSessionRecentIOStatsDTO(sessionId, totalIOTimePerStorageDto);
	}

}
