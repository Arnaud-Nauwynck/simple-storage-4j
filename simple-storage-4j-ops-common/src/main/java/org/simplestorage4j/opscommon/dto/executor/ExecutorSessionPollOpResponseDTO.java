package org.simplestorage4j.opscommon.dto.executor;

import java.io.Serializable;

import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor @AllArgsConstructor
public class ExecutorSessionPollOpResponseDTO implements Serializable {

	/** */
	private static final long serialVersionUID = 1L;

	public BlobStorageOperationDTO op;

	public ExecutorSessionUpdatePollingDTO pollingResp;

}
