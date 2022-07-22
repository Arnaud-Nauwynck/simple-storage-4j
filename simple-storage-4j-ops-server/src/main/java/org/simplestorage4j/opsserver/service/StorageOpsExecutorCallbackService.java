package org.simplestorage4j.opsserver.service;

import java.util.List;

import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.ops.BlobStorageOperation;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPingAliveRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPollOpRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPollOpResponseDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPollOpsResponseDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionStartRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionStopRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionUpdatePollingDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import lombok.val;

/**
 * callback service for Ops Executor
 */
@Service
public class StorageOpsExecutorCallbackService {

	@Autowired
	private ExecutorSessionService executorSessionService;

	@Autowired
	private StorageJobOpsQueueService storageOpsService;
	
	// Executor Session lifecycle: start / ping-alive / stop
	// ------------------------------------------------------------------------

	public void onExecutorStart(ExecutorSessionStartRequestDTO req) {
		executorSessionService.onExecutorStart(req);
	}
	
	public void onExecutorStop(ExecutorSessionStopRequestDTO req) {
		executorSessionService.onExecutorStop(req);
	}

	public ExecutorSessionUpdatePollingDTO onExecutorPingAlive(ExecutorSessionPingAliveRequestDTO req) {
		val sessionId = req.sessionId;
		val session = executorSessionService.updateOrCreateSessionAlive(sessionId);
		val pollingRespDto = session.toPollingRespDTO();
		return pollingRespDto;
	}

	// ------------------------------------------------------------------------
	
	public ExecutorSessionPollOpResponseDTO pollOp(ExecutorSessionPollOpRequestDTO req) {
		val sessionId = req.sessionId;
		val session = executorSessionService.updateOrCreateSessionAlive(sessionId);
		val op = storageOpsService.pollOp(sessionId);
		val opDto = (op != null)? op.toDTO() : null;
		val pollingRespDto = session.toPollingRespDTO();
		return new ExecutorSessionPollOpResponseDTO(opDto, pollingRespDto);
	}

	public void onOpsFinished(String sessionId, List<BlobStorageOperationResult> opResults) {
		val session = executorSessionService.updateOrCreateSessionAlive(sessionId);
		session.incrIOStats(opResults);
		storageOpsService.onOpsFinished(opResults);
	}

	public ExecutorSessionPollOpResponseDTO onOpFinishedPollNext(String sessionId, BlobStorageOperationResult opResult) {
		val session = executorSessionService.updateOrCreateSessionAlive(sessionId);
		session.incrIOStats(opResult);
		storageOpsService.onOpFinished(opResult);
		val op = storageOpsService.pollOp(sessionId);
		val opDto = (op != null)? op.toDTO() : null;
		val pollingRespDto = session.toPollingRespDTO();
		return new ExecutorSessionPollOpResponseDTO(opDto, pollingRespDto);
	}

	public ExecutorSessionPollOpsResponseDTO onOpsFinishedPollNext(String sessionId, List<BlobStorageOperationResult> opResults, int pollingCount) {
		val session = executorSessionService.updateOrCreateSessionAlive(sessionId);
		session.incrIOStats(opResults);
		storageOpsService.onOpsFinished(opResults);
		val ops = storageOpsService.pollOps(sessionId, pollingCount);
		val opDtos = BlobStorageOperation.toDTOs(ops);
		val pollingRespDto = session.toPollingRespDTO();
		return new ExecutorSessionPollOpsResponseDTO(opDtos, pollingRespDto);
	}

}
