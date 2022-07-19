package org.simplestorage4j.opscommon.dto.queue;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor @AllArgsConstructor
public class AddJobOpsQueueResponseDTO implements Serializable {

	/** */
	private static final long serialVersionUID = 1L;

	public long jobId;
	
}
