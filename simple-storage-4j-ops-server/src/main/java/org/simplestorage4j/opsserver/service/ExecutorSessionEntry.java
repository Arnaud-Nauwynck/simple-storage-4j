package org.simplestorage4j.opsserver.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.GuardedBy;

import org.simplestorage4j.api.iocost.counter.PerBlobStoragesIOTimeCounter;
import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.iocost.immutable.PerBlobStoragesIOTimeResult;
import org.simplestorage4j.api.ops.BlobStorageOperation;
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

	private final Object lock = new Object();
	
	@GuardedBy("lock")
	private Map<BlobStorageOperationId,PolledBlobStorageOperationEntry> polledOps = new HashMap<>();

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

	public void addPolledOp(BlobStorageOperation op) {
		val opId = op.toId();
		val now = System.currentTimeMillis();
		val polledTask = new PolledBlobStorageOperationEntry(this, op, now);
		synchronized(lock) {
			polledOps.put(opId, polledTask);
		}
	}

	public PolledBlobStorageOperationEntry removePolledOp(BlobStorageOperationId opId) {
		synchronized(lock) {
			return polledOps.remove(opId);
		}
	}
	
	public List<PolledBlobStorageOperationEntry> getCopyPolledJobTasks() {
		synchronized(lock) {
			return new ArrayList<>(polledOps.values());
		}
	}
	
	public List<PolledBlobStorageOperationEntry> clearGetCopyPolledJobTasks() {
		synchronized(lock) {
			val res = new ArrayList<>(polledOps.values());
			polledOps.clear();
			return res;
		}
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
		synchronized(lock) {
			val polledOps = BlobStorageUtils.map(polledOps.values(), 
					x -> new BlobStoragePolledOperationDTO(x.polledTime, x.op.toDTO()));
			return new ExecutorSessionPolledOpsDTO(sessionId, polledOps);
		}
	}

	public ExecutorSessionRecentIOStatsDTO toRecentIOStatsDTO() {
		val totalIOTimePerStorageDto = totalIOTimePerStorage.toDTO();
		return new ExecutorSessionRecentIOStatsDTO(sessionId, totalIOTimePerStorageDto);
	}

	@Override
	public String toString() {
		synchronized(lock) {
			return "{ExecutorSessionEntry" //
					+ " sessionId:" + sessionId //
					+ " polledJobTasks:" + polledOps.size()
					+ "}";
		}
	}

	
}
