package org.simplestorage4j.opsserver.service;

import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ExecutorSessionEntry {
	public final String sessionId;
	public final String host;
	public final long startTime;

	@Getter
	private final Map<String,String> props;
	
	@Getter
	private Map<JobTaskId,PolledJobTaskEntry> polledJobTasks = new HashMap<>();
	
	public long lastPingAliveTime;

}