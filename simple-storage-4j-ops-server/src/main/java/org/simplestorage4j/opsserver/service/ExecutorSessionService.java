package org.simplestorage4j.opsserver.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.concurrent.GuardedBy;

import org.simplestorage4j.api.util.BlobStorageUtils;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionStartRequestDTO;
import org.simplestorage4j.opscommon.dto.executor.ExecutorSessionStopRequestDTO;
import org.simplestorage4j.opscommon.dto.session.ExecutorSessionInfoDTO;
import org.simplestorage4j.opscommon.dto.session.ExecutorSessionPolledOpsDTO;
import org.simplestorage4j.opscommon.dto.session.ExecutorSessionPollingStateDTO;
import org.simplestorage4j.opscommon.dto.session.ExecutorSessionRecentIOStatsDTO;
import org.simplestorage4j.opscommon.dto.session.ExecutorSessionUpdatePollingRequestDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
	private final Map<String, ExecutorSessionEntry> sessions = new HashMap<>();

	@Value("${storage-app-server.sessions.maxPingAliveSeconds:240}")
	private long maxPingAliveSeconds = 4 * 60;

	@Value("${storage-app-server.sessions.checkSessionAlivePeriodSeconds:30}")
	private long checkSessionAlivePeriodSeconds = 30;

	private ScheduledExecutorService scheduledExecutor;
	private ScheduledFuture<?> scheduled;

	// ------------------------------------------------------------------------

	@PostConstruct
	public void init() {
		this.scheduledExecutor = createDefaultScheduledExecutor();
		startScheduleCheckPingAlives();
	}

	private static ScheduledExecutorService createDefaultScheduledExecutor() {
		ThreadFactory threadFactory = new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				val res = new Thread(r, "check-session-ping-alive");
				res.setDaemon(true);
				return res;
			}
		};
		return Executors.newScheduledThreadPool(1, threadFactory);
	}

	public void startScheduleCheckPingAlives() {
		if (this.scheduled != null) {
			return;
		}
		this.scheduled = scheduledExecutor.scheduleAtFixedRate(() -> checkSessionsAlive(),
				checkSessionAlivePeriodSeconds, checkSessionAlivePeriodSeconds, TimeUnit.SECONDS);
	}

	public void stopScheduleCheckPingAlives() {
		if (this.scheduled != null) {
			return;
		}
		this.scheduled.cancel(false);
		this.scheduled = null;
	}

	private void checkSessionsAlive() {
		val deadSessions = new ArrayList<ExecutorSessionEntry>();
		val now = System.currentTimeMillis();
		val maxPingAliveMillis = maxPingAliveSeconds * 1000;
		synchronized (lock) {
			for (val session : sessions.values()) {
				val lastPingMillis = now - session.lastPingAliveTime;
				if (lastPingMillis > maxPingAliveMillis) {
					deadSessions.add(session);
				}
			}
			if (! deadSessions.isEmpty()) {
				log.info("detected " + deadSessions.size() + " dead sessions to remove: " + deadSessions);
				for (val deadSession : deadSessions) {
					sessions.remove(deadSession.sessionId);
				}
			}
		}
		if (! deadSessions.isEmpty()) {
			for (val deadSession : deadSessions) {
				onExecutorSessionDead(deadSession);
			}
		}
	}

	// handle ExecutorSession Lifecycle: start / pingAlive / stop
	// ------------------------------------------------------------------------

	public void onExecutorStart(ExecutorSessionStartRequestDTO req) {
		val sessionId = Objects.requireNonNull(req.sessionId);
		val entry = new ExecutorSessionEntry(sessionId, req.host, req.startTime, req.props);
		entry.lastPingAliveTime = System.currentTimeMillis();
		synchronized (lock) {
			val found = sessions.get(sessionId);
			if (found != null) {
				throw new IllegalArgumentException("session already exist: " + sessionId);
			}
			sessions.put(sessionId, entry);
		}
	}

	public void onExecutorStop(ExecutorSessionStopRequestDTO req) {
		val sessionId = Objects.requireNonNull(req.sessionId);
		List<PolledBlobStorageOperationEntry> polledJobTasks;
		synchronized (lock) {
			val found = sessions.remove(sessionId);
			if (found == null) {
				log.warn("session '" + sessionId + "' not found.. ignore onExecutorStop()");
				return;
			}
			polledJobTasks = found.clearGetCopyPolledJobTasks();
		}
		if (!polledJobTasks.isEmpty()) {
			log.info("onExecutorStop " + sessionId + ", found " + polledJobTasks.size()
					+ " polled tasks, re-put to queues");
			storageOpsService.onExecutorStop_reputPolledTasks(polledJobTasks);
		}
	}

	protected void onExecutorSessionDead(ExecutorSessionEntry deadSession) {
		val polledJobTasks = deadSession.clearGetCopyPolledJobTasks();
		if (!polledJobTasks.isEmpty()) {
			log.info("onExecutorSessionDead " + deadSession.sessionId + ", found " + polledJobTasks.size()
					+ " polled tasks, re-put to queues");
			storageOpsService.onExecutorStop_reputPolledTasks(polledJobTasks);
		}
	}

	public ExecutorSessionEntry updateOrCreateSessionAlive(String sessionId) {
		synchronized (lock) {
			ExecutorSessionEntry res = sessions.get(sessionId);
			if (res == null) {
				log.warn("session '" + sessionId + "' not found.. implicit create");
				String host = "?";
				val startTime = System.currentTimeMillis();
				val props = new HashMap<String, String>();
				res = new ExecutorSessionEntry(sessionId, host, startTime, props);
				sessions.put(sessionId, res);
			}
			res.lastPingAliveTime = System.currentTimeMillis();
			return res;
		}
	}

	// handle public Rest api for session managment
	// ------------------------------------------------------------------------

	private ExecutorSessionEntry doGetSession(String sessionId) {
		Objects.requireNonNull(sessionId);
		val res = sessions.get(sessionId);
		if (res == null) {
			throw new IllegalArgumentException("session '" + sessionId + "' not found");
		}
		return res;
	}

	public void executorSessionUpdatePolling(ExecutorSessionUpdatePollingRequestDTO req) {
		synchronized (lock) {
			val session = doGetSession(req.sessionId);
			session.setPollingSuspendRequested(req.pollingSuspendRequested);
			session.setStopRequested(req.stopRequested);
			session.setKillRequested(req.killRequested);
		}
	}

	public List<ExecutorSessionInfoDTO> listExecutorSessionInfos() {
		synchronized (lock) {
			return BlobStorageUtils.map(sessions.values(), x -> x.toInfoDTO());
		}
	}

	public List<ExecutorSessionPollingStateDTO> listExecutorSessionPollingStates() {
		synchronized (lock) {
			return BlobStorageUtils.map(sessions.values(), x -> x.toPollingStatesDTO());
		}
	}

	public List<ExecutorSessionPolledOpsDTO> listExecutorSessionCurrPolledOps() {
		synchronized (lock) {
			return BlobStorageUtils.map(sessions.values(), x -> x.toCurrPolledOpsDTO());
		}
	}

	public List<ExecutorSessionRecentIOStatsDTO> listExecutorSessionRecentIOStats() {
		synchronized (lock) {
			return BlobStorageUtils.map(sessions.values(), x -> x.toRecentIOStatsDTO());
		}
	}

	public ExecutorSessionInfoDTO getExecutorSessionInfo(String sessionId) {
		synchronized (lock) {
			val session = doGetSession(sessionId);
			return session.toInfoDTO();
		}
	}

	public ExecutorSessionPollingStateDTO getExecutorSessionPollingState(String sessionId) {
		synchronized (lock) {
			val session = doGetSession(sessionId);
			return session.toPollingStatesDTO();
		}
	}

	public ExecutorSessionPolledOpsDTO getExecutorSessionCurrPolledOps(String sessionId) {
		synchronized (lock) {
			val session = doGetSession(sessionId);
			return session.toCurrPolledOpsDTO();
		}
	}

	public ExecutorSessionRecentIOStatsDTO getExecutorSessionRecentIOStats(String sessionId) {
		synchronized (lock) {
			val session = doGetSession(sessionId);
			return session.toRecentIOStatsDTO();
		}
	}

}
