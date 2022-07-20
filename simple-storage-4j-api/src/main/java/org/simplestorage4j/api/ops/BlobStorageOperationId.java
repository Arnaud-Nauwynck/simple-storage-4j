package org.simplestorage4j.api.ops;

import lombok.Value;

@Value
public class BlobStorageOperationId {
	public final long jobId;
	public final long taskId;
}
