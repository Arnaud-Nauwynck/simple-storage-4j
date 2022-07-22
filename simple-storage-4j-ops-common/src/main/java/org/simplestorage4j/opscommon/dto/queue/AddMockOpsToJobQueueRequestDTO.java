package org.simplestorage4j.opscommon.dto.queue;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor @AllArgsConstructor
public class AddMockOpsToJobQueueRequestDTO implements Serializable {

	/** */
	private static final long serialVersionUID = 1L;

	public long jobId;

	public int mockOpsCount;

	public int mockOpsDurationMillis;
	public String srcStorageId;
	public String destStorageId;
	public long mockSrcFileLen;
	public long mockDestFileLen;

}
