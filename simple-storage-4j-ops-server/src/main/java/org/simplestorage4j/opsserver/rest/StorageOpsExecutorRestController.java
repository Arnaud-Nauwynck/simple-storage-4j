package org.simplestorage4j.opsserver.rest;

import org.simplestorage4j.opscommon.dto.executor.ExecutorOpsFinishedRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPingAliveRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPollOpRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPollOpResponseDTO;
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
		log.debug("http PUT /poll-op " + req.sessionId);
		executorSessionService.onExecutorPingAlive(req.sessionId);
		val res = storageOpsService.pollOp(req);
		return res;
	}

	@PutMapping("/on-ops-finished")
	public void onOpsFinished(@RequestBody ExecutorOpsFinishedRequestDTO req) {
		log.debug("http PUT /on-ops-finished " + req.sessionId + " jobId:" + req.jobId + " taskResults:" + req.taskResults);
		executorSessionService.onExecutorPingAlive(req.sessionId);
		storageOpsService.onOpsFinished(req);
	}
	
}
