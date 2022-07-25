package org.simplestorage4j.opsserver.service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.checkerframework.checker.units.qual.s;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class StorageOpsPeriodicCheckService {

	private ScheduledExecutorService scheduledExecutor;
	private ScheduledFuture<?> scheduled;

	@Value("${storage-app-server.periodicCheckSeconds:30}")
	private long checkSessionAlivePeriodSeconds = 30;

	@Autowired
	protected StorageOpsExecutorSessionService sessionService;
	
	@Autowired
	protected StorageJobOpsQueueService opsQueueService;
	
	
	// ------------------------------------------------------------------------
	
	@PostConstruct
	public void init() {
		this.scheduledExecutor = createDefaultScheduledExecutor();
		startSchedulePeriodicCheck();
	}

	// ------------------------------------------------------------------------

	private static ScheduledExecutorService createDefaultScheduledExecutor() {
		ThreadFactory threadFactory = new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				val res = new Thread(r, "periodic-checks");
				res.setDaemon(true);
				return res;
			}
		};
		return Executors.newScheduledThreadPool(1, threadFactory);
	}

	public void startSchedulePeriodicCheck() {
		if (this.scheduled != null) {
			return;
		}
		log.info("start schedule periodicCheck every " + checkSessionAlivePeriodSeconds + "s ...");
		this.scheduled = scheduledExecutor.scheduleAtFixedRate(() -> periodicCheck(),
				checkSessionAlivePeriodSeconds, checkSessionAlivePeriodSeconds, TimeUnit.SECONDS);
	}

	public void stopSchedulePeriodicCheck() {
		if (this.scheduled != null) {
			return;
		}
		log.info("stop schedule periodicCheck");
		this.scheduled.cancel(false);
		this.scheduled = null;
	}

	public void periodicCheck() {
		sessionService.checkSessionsAlive();
		// flush done file if last flushed time > 30s
		opsQueueService.periodicCheckFlushFiles();
	}

}
