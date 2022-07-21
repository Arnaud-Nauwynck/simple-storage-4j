package org.simplestorage4j.opsserver.rest;

import java.util.List;

import org.simplestorage4j.opscommon.dto.session.ExecutorSessionInfoDTO;
import org.simplestorage4j.opscommon.dto.session.ExecutorSessionPolledOpsDTO;
import org.simplestorage4j.opscommon.dto.session.ExecutorSessionPollingStateDTO;
import org.simplestorage4j.opscommon.dto.session.ExecutorSessionRecentIOStatsDTO;
import org.simplestorage4j.opscommon.dto.session.ExecutorSessionUpdatePollingRequestDTO;
import org.simplestorage4j.opsserver.service.ExecutorSessionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;

/**
 * api for Ops Executor Sessions
 */
@RestController
@RequestMapping(path="/api/storage-ops/executor-session")
@OpenAPIDefinition(
		tags = { @Tag(name="opsExecutorSession") }
		)
@Slf4j
public class StorageOpsExecutorSessionRestController {

	@Autowired
	private ExecutorSessionService executorSessionService;

	// ------------------------------------------------------------------------

	@PutMapping("/update-polling")
	public void executorSessionUpdatePolling(
			@RequestBody ExecutorSessionUpdatePollingRequestDTO req) {
		log.info("http PUT /executor-session/update-polling");
		executorSessionService.executorSessionUpdatePolling(req);
	}

	// ------------------------------------------------------------------------

	@GetMapping("/infos")
	public List<ExecutorSessionInfoDTO> listExecutorSessionInfos() {
		log.info("http GET /executor-session/infos");
		return executorSessionService.listExecutorSessionInfos();
	}

	@GetMapping("/polling-states")
	public List<ExecutorSessionPollingStateDTO> listExecutorSessionPollingStates() {
		log.info("http GET /executor-session/polling-states");
		return executorSessionService.listExecutorSessionPollingStates();
	}

	@GetMapping("/curr-polled-ops")
	public List<ExecutorSessionPolledOpsDTO> listExecutorSessionCurrPolledOps() {
		log.info("http GET /executor-session/curr-polled-ops");
		return executorSessionService.listExecutorSessionCurrPolledOps();
	}

	@GetMapping("/recent-io-stats")
	public List<ExecutorSessionRecentIOStatsDTO> listExecutorSessionRecentIOStats() {
		log.info("http GET /executor-session/recent-io-stats");
		return executorSessionService.listExecutorSessionRecentIOStats();
	}

	@GetMapping("/{sessionId}/infos")
	public ExecutorSessionInfoDTO getExecutorSessionInfo(
			@PathVariable("sessionId") String sessionId) {
		log.info("http GET /executor-session/" + sessionId + "/infos");
		return executorSessionService.getExecutorSessionInfo(sessionId);
	}

	@GetMapping("/{sessionId}/polling-states")
	public ExecutorSessionPollingStateDTO getExecutorSessionPollingState(
			@PathVariable("sessionId") String sessionId) {
		log.info("http GET /executor-session/" + sessionId + "/polling-states");
		return executorSessionService.getExecutorSessionPollingState(sessionId);
	}

	@GetMapping("/{sessionId}/curr-polled-ops")
	public ExecutorSessionPolledOpsDTO getExecutorSessionCurrPolledOps(
			@PathVariable("sessionId") String sessionId) {
		log.info("http GET /executor-session/" + sessionId + "/curr-polled-ops");
		return executorSessionService.getExecutorSessionCurrPolledOps(sessionId);
	}

	@GetMapping("/{sessionId}/recent-io-stats")
	public ExecutorSessionRecentIOStatsDTO executorSessionRecentIOStats(
			@PathVariable("sessionId") String sessionId) {
		log.info("http GET /executor-session/" + sessionId + "/recent-io-stats");
		return executorSessionService.getExecutorSessionRecentIOStats(sessionId);
	}

}
