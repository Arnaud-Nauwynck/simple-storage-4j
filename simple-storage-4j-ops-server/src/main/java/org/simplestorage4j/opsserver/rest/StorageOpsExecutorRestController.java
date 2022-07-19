package org.simplestorage4j.opsserver.rest;

import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.api.ops.BlobStorageOperation;
import org.simplestorage4j.opscommon.dto.executor.ExecutorOpFinishedPollNextRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorOpsFinishedRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPingAliveRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPollOpRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPollOpResponseDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPollOpsResponseDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionStartRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionStopRequestDTO;
import org.simplestorage4j.opsserver.service.ExecutorSessionService;
import org.simplestorage4j.opsserver.service.StorageJobOpsQueueService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * internal api for Ops Executor
 */
@RestController
@RequestMapping(path="/api/storage-ops/executor")
@Slf4j
public class StorageOpsExecutorRestController {

	@Autowired
	private ExecutorSessionService executorSessionService;

	@Autowired
	private StorageJobOpsQueueService storageOpsService;
	
	// Executor Session lifecycle: start / ping-alive / stop
	// ------------------------------------------------------------------------

	@PutMapping("/onExecutorStart")
	public void onExecutorStart(@RequestBody ExecutorSessionStartRequestDTO req) {
		log.info("http PUT /onExecutorStart " + req.sessionId);
		executorSessionService.onExecutorStart(req);
	}
	
	@PutMapping("/onExecutorStop")
	public void onExecutorStop(ExecutorSessionStopRequestDTO req) {
		log.info("http PUT /onExecutorStop " + req.sessionId);
		executorSessionService.onExecutorStop(req);
	}

	@PutMapping("/onExecutorPingAlive")
	public void onExecutorPingAlive(ExecutorSessionPingAliveRequestDTO req) {
		log.debug("http PUT /onExecutorPingAlive " + req.sessionId);
		executorSessionService.onExecutorPingAlive(req);
	}

	// ------------------------------------------------------------------------
	
	@PutMapping("/poll-op")
	public ExecutorSessionPollOpResponseDTO pollOp(@RequestBody ExecutorSessionPollOpRequestDTO req) {
		val sessionId = req.sessionId;
		log.debug("http PUT /poll-op " + sessionId);
		executorSessionService.onExecutorPingAlive(sessionId);
		val op = storageOpsService.pollOp(sessionId);
		val opDto = (op != null)? op.toDTO() : null;
		return new ExecutorSessionPollOpResponseDTO(opDto);
	}

	@PutMapping("/on-ops-finished")
	public void onOpsFinished(@RequestBody ExecutorOpsFinishedRequestDTO req) {
		val sessionId = req.sessionId;
		val opResults = BlobStorageOperationResult.fromDTOs(req.opResults);
		log.debug("http PUT /on-ops-finished " + sessionId + " opResults:" + opResults);
		executorSessionService.onExecutorPingAlive(req.sessionId);
		storageOpsService.onOpsFinished(opResults);
	}

	@PutMapping("/on-op-finished-poll-next")
	public ExecutorSessionPollOpResponseDTO onOpFinishedPollNext(@RequestBody ExecutorOpFinishedPollNextRequestDTO req) {
		val sessionId = req.sessionId;
		val opResult = BlobStorageOperationResult.fromDTO(req.opResult);
		log.debug("http PUT /on-op-finished-poll-next " + sessionId + " opResult:" + opResult);
		executorSessionService.onExecutorPingAlive(sessionId);
		storageOpsService.onOpFinished(opResult);
		val op = storageOpsService.pollOp(sessionId);
		val opDto = (op != null)? op.toDTO() : null; 
		return new ExecutorSessionPollOpResponseDTO(opDto);
	}

	@PutMapping("/on-ops-finished-poll-nexts")
	public ExecutorSessionPollOpsResponseDTO onOpsFinishedPollNext(@RequestBody ExecutorOpsFinishedRequestDTO req) {
		val sessionId = req.sessionId;
		val opResults = BlobStorageOperationResult.fromDTOs(req.opResults);
		log.debug("http PUT /on-ops-finished-poll-nexts " + sessionId + " opResults:" + opResults);
		executorSessionService.onExecutorPingAlive(sessionId);
		storageOpsService.onOpsFinished(opResults);
		val ops = storageOpsService.pollOps(sessionId, opResults.size());
		val opDtos = BlobStorageOperation.toDTOs(ops);
		return new ExecutorSessionPollOpsResponseDTO(opDtos);
	}

}
