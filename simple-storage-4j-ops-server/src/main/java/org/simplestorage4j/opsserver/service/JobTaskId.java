package org.simplestorage4j.opsserver.service;

import lombok.Value;

@Value
public class JobTaskId {
	public final int jobId;
	public final int taskId;
}
