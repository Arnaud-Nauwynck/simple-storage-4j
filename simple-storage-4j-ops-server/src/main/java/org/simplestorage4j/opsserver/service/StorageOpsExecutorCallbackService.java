package org.simplestorage4j.opsserver.service;

import java.util.List;

import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.ops.BlobStorageOperation;
import org.simplestorage4j.api.util.LoggingCounter;
import org.simplestorage4j.api.util.LoggingCounter.LoggingCounterParams;
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
import lombok.extern.slf4j.Slf4j;

/**
 * callback service for Ops Executor
 */
@Service
@Slf4j
public class StorageOpsExecutorCallbackService {

	@Autowired
	private ExecutorSessionService executorSessionService;

	@Autowired
	private StorageJobOpsQueueService storageOpsService;

	private final LoggingCounter loggingCounter_startSession = new LoggingCounter( //
			"executorCallback startSession", new LoggingCounterParams(1, 0));
	private final LoggingCounter loggingCounter_stopSession = new LoggingCounter( //
			"executorCallback stopSession", new LoggingCounterParams(1, 0));
	private final LoggingCounter loggingCounter_pingAlive = new LoggingCounter( //
			"executorCallback pingAlive", new LoggingCounterParams(1000, 600_000));
	private final LoggingCounter loggingCounter_pollOp = new LoggingCounter( //
			"executorCallback pollOp", new LoggingCounterParams(1000, 600_000));
	private final LoggingCounter loggingCounter_onOpsFinished = new LoggingCounter( //
			"executorCallback onOpsFinished", new LoggingCounterParams(1000, 600_000));
	private final LoggingCounter loggingCounter_onOpFinishedPollNext = new LoggingCounter( //
			"executorCallback onOpFinishedPollNext", new LoggingCounterParams(1000, 600_000));
	private final LoggingCounter loggingCounter_onOpsFinishedPollNexts = new LoggingCounter( //
			"executorCallback onOpsFinishedPollNexts", new LoggingCounterParams(1000, 600_000));

	// Executor Session lifecycle: start / ping-alive / stop
	// ------------------------------------------------------------------------

	public void onExecutorStart(ExecutorSessionStartRequestDTO req) {
		loggingCounter_startSession.runAndIncr(() -> executorSessionService.onExecutorStart(req),
				logPrefix -> log.info(logPrefix + " " + req.sessionId));
	}

	public void onExecutorStop(ExecutorSessionStopRequestDTO req) {
		loggingCounter_stopSession.runAndIncr(() -> executorSessionService.onExecutorStop(req),
				logPrefix -> log.info(logPrefix + " " + req.sessionId));
	}

	public ExecutorSessionUpdatePollingDTO onExecutorPingAlive(ExecutorSessionPingAliveRequestDTO req) {
		return loggingCounter_pingAlive.runAndIncr(() -> {
			val sessionId = req.sessionId;
			val session = executorSessionService.updateOrCreateSessionAlive(sessionId);
			val pollingRespDto = session.toPollingRespDTO();
			return pollingRespDto;
		}, logPrefix -> log.info(logPrefix + " " + req.sessionId));
	}

	// ------------------------------------------------------------------------

	public ExecutorSessionPollOpResponseDTO pollOp(ExecutorSessionPollOpRequestDTO req) {
		return loggingCounter_pollOp.runAndIncr(() -> {
			val sessionId = req.sessionId;
			val session = executorSessionService.updateOrCreateSessionAlive(sessionId);
			val op = storageOpsService.pollOp(session);
			val opDto = (op != null) ? op.toDTO() : null;
			val pollingRespDto = session.toPollingRespDTO();
			return new ExecutorSessionPollOpResponseDTO(opDto, pollingRespDto);
		}, logPrefix -> log.info(logPrefix + " " + req.sessionId));
	}

	public void onOpsFinished(String sessionId, List<BlobStorageOperationResult> opResults) {
		loggingCounter_onOpsFinished.runAndIncr(() -> {
			val session = executorSessionService.updateOrCreateSessionAlive(sessionId);
			session.incrIOStats(opResults);
			storageOpsService.onOpsFinished(session, opResults);
		}, logPrefix -> log.info(logPrefix + " " + sessionId + " opResults:" + opResults.size()));
	}

	public ExecutorSessionPollOpResponseDTO onOpFinishedPollNext(String sessionId,
			BlobStorageOperationResult opResult) {
		return loggingCounter_onOpFinishedPollNext.runAndIncr(() -> {
			val session = executorSessionService.updateOrCreateSessionAlive(sessionId);
			session.incrIOStats(opResult);
			storageOpsService.onOpFinished(session, opResult);
			val op = storageOpsService.pollOp(session);
			val opDto = (op != null) ? op.toDTO() : null;
			val pollingRespDto = session.toPollingRespDTO();
			return new ExecutorSessionPollOpResponseDTO(opDto, pollingRespDto);
		}, logPrefix -> log.info(logPrefix + " " + sessionId));
	}

	public ExecutorSessionPollOpsResponseDTO onOpsFinishedPollNexts(String sessionId,
			List<BlobStorageOperationResult> opResults, int pollingCount) {
		return loggingCounter_onOpsFinishedPollNexts.runAndIncr(() -> {
			val session = executorSessionService.updateOrCreateSessionAlive(sessionId);
			session.incrIOStats(opResults);
			storageOpsService.onOpsFinished(session, opResults);
			val ops = storageOpsService.pollOps(session, pollingCount);
			val opDtos = BlobStorageOperation.toDTOs(ops);
			val pollingRespDto = session.toPollingRespDTO();
			return new ExecutorSessionPollOpsResponseDTO(opDtos, pollingRespDto);
		}, logPrefix -> log.info(logPrefix + " " + sessionId + " opResults.count:" + opResults.size()));
	}

	// ------------------------------------------------------------------------

}
