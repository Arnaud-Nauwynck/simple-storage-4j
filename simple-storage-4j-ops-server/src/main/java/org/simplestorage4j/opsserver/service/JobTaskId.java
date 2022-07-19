package org.simplestorage4j.opsserver.service;

import lombok.Value;

@Value
public class JobTaskId {
	public final long jobId;
	public final long taskId;
}
