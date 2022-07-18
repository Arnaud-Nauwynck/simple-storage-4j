package org.simplestorage4j.opsserver.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

@RequiredArgsConstructor
public class ExecutorSessionEntry {
	
	public final String sessionId;
	
	public final String host;
	public final long startTime;

	@Getter
	private final Map<String,String> props;
	
	private Map<JobTaskId,PolledJobTaskEntry> polledJobTasks = new HashMap<>();
	
	public long lastPingAliveTime;
	
	// ------------------------------------------------------------------------

	public Map<JobTaskId, PolledJobTaskEntry> getPolledJobTasks() {
		return polledJobTasks;
	}
	
	public List<PolledJobTaskEntry> clearGetCopyPolledJobTasks() {
		val res = new ArrayList<>(polledJobTasks.values());
		polledJobTasks.clear();
		return res;
	}
	
}
