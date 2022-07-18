package org.simplestorage4j.opsserver.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.concurrent.GuardedBy;

import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionPingAliveRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionStartRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionStopRequestDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ExecutorSessionService {

	@Autowired
	private StorageJobOpsQueueService storageOpsService;
	
	private final Object lock = new Object();
	
	@GuardedBy("lock")
	private final Map<String,ExecutorSessionEntry> sessions = new HashMap<>();
	
	
	
	// handle ExecutorSession Lifecycle: start / pingAlive / stop
	// ------------------------------------------------------------------------
	
	public void onExecutorStart(ExecutorSessionStartRequestDTO req) {
		val sessionId = Objects.requireNonNull(req.sessionId);
		val entry = new ExecutorSessionEntry(sessionId, req.host, req.startTime, req.props); 
		synchronized(lock) {
			val found = sessions.get(sessionId);
			if (found != null) {
				throw new IllegalArgumentException("session already exist: " + sessionId);
			}
			sessions.put(sessionId, entry);
		}
	}

	public void onExecutorStop(ExecutorSessionStopRequestDTO req) {
		val sessionId = Objects.requireNonNull(req.sessionId);
		synchronized(lock) {
			val found = sessions.remove(sessionId);
			if (found == null) {
				log.warn("session '" + sessionId + "' not found.. ignore onExecutorStop()");
				return;
			}
			int polledCount = found.getPolledJobTasks().size();
			if (polledCount != 0) {
				log.info("onExecutorStop " + sessionId + ", found " + polledCount + " polled tasks, re-put to queues");
				storageOpsService.onExecutorStop_reputPolledTasks(found);
			}
		}
	}

	public void onExecutorPingAlive(ExecutorSessionPingAliveRequestDTO req) {
		val sessionId = Objects.requireNonNull(req.sessionId);
		onExecutorPingAlive(sessionId);
	}

	public void onExecutorPingAlive(String sessionId) {
		synchronized(lock) {
			val found = sessions.get(sessionId);
			if (found == null) {
				log.warn("session '" + sessionId + "' not found.. ignore onExecutorPingAlive()");
				return;
			}
			found.lastPingAliveTime = System.currentTimeMillis();
		}
	}

	// ------------------------------------------------------------------------
	
}
