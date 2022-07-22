package org.simplestorage4j.opsserver.rest;

import java.util.Objects;

import org.simplestorage4j.api.iocost.immutable.BlobStorageOperationResult;
import org.simplestorage4j.opscommon.dto.executor.ExecutorOpFinishedPollNextRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorOpsFinishedRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPingAliveRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPollOpRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPollOpResponseDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPollOpsResponseDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionStartRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionStopRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionUpdatePollingDTO;
import org.simplestorage4j.opsserver.service.StorageOpsExecutorCallbackService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * internal api for Ops Executor
 */
@RestController
@RequestMapping(path="/api/storage-ops/executor-callback")
@OpenAPIDefinition(
		tags = { @Tag(name="jobExecutorCallback") }
		)
@Slf4j
public class StorageOpsExecutorCallbackRestController {

	@Autowired
	private StorageOpsExecutorCallbackService delegate;

	// Executor Session lifecycle: start / ping-alive / stop
	// ------------------------------------------------------------------------

	@PutMapping("/onExecutorStart")
	public void onExecutorStart(@RequestBody ExecutorSessionStartRequestDTO req) {
		log.info("http PUT /onExecutorStart " + req.sessionId);
		delegate.onExecutorStart(req);
	}
	
	@PutMapping("/onExecutorStop")
	public void onExecutorStop(@RequestBody ExecutorSessionStopRequestDTO req) {
		log.info("http PUT /onExecutorStop " + req.sessionId);
		delegate.onExecutorStop(req);
	}

	@PutMapping("/onExecutorPingAlive")
	public ExecutorSessionUpdatePollingDTO onExecutorPingAlive(@RequestBody ExecutorSessionPingAliveRequestDTO req) {
		log.debug("http PUT /onExecutorPingAlive " + req.sessionId);
		val res = delegate.onExecutorPingAlive(req);
		return res;
	}

	// ------------------------------------------------------------------------
	
	@PutMapping("/poll-op")
	public ExecutorSessionPollOpResponseDTO pollOp(@RequestBody ExecutorSessionPollOpRequestDTO req) {
		val sessionId = Objects.requireNonNull(req.sessionId);
		log.debug("http PUT /poll-op " + sessionId);
		val res = delegate.pollOp(req);
		return res;
	}

	@PutMapping("/on-ops-finished")
	public void onOpsFinished(@RequestBody ExecutorOpsFinishedRequestDTO req) {
		val sessionId = Objects.requireNonNull(req.sessionId);
		val opResults = BlobStorageOperationResult.fromDTOs(req.opResults);
		log.debug("http PUT /on-ops-finished " + sessionId + " opResults:" + opResults);
		delegate.onOpsFinished(sessionId, opResults);
	}

	@PutMapping("/on-op-finished-poll-next")
	public ExecutorSessionPollOpResponseDTO onOpFinishedPollNext(@RequestBody ExecutorOpFinishedPollNextRequestDTO req) {
		val sessionId = Objects.requireNonNull(req.sessionId);
		val opResult = BlobStorageOperationResult.fromDTO(req.opResult);
		log.debug("http PUT /on-op-finished-poll-next " + sessionId + " opResult:" + opResult);
		val res = delegate.onOpFinishedPollNext(sessionId, opResult);
		return res;
	}

	@PutMapping("/on-ops-finished-poll-nexts")
	public ExecutorSessionPollOpsResponseDTO onOpsFinishedPollNexts(@RequestBody ExecutorOpsFinishedRequestDTO req) {
		val sessionId = req.sessionId;
		val opResults = BlobStorageOperationResult.fromDTOs(req.opResults);
		log.debug("http PUT /on-ops-finished-poll-nexts " + sessionId + " opResults:" + opResults);
		val res = delegate.onOpsFinishedPollNexts(sessionId, opResults, req.pollCount);
		return res;
	}

}
