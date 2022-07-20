package org.simplestorage4j.opsserver.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.simplestorage4j.api.ops.BlobStorageOperationId;

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
	
	private Map<BlobStorageOperationId,PolledBlobStorageOperationEntry> polledJobTasks = new HashMap<>();
	
	public long lastPingAliveTime;
	
	// ------------------------------------------------------------------------

	public Map<BlobStorageOperationId, PolledBlobStorageOperationEntry> getPolledJobTasks() {
		return polledJobTasks;
	}
	
	public List<PolledBlobStorageOperationEntry> clearGetCopyPolledJobTasks() {
		val res = new ArrayList<>(polledJobTasks.values());
		polledJobTasks.clear();
		return res;
	}
	
}
