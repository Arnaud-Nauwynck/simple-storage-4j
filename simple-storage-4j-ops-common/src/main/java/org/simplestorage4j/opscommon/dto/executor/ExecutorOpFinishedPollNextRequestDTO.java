package org.simplestorage4j.opscommon.dto.executor;

import java.io.Serializable;

import org.simplestorage4j.api.iocost.dto.BlobStorageOperationResultDTO;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class ExecutorOpFinishedPollNextRequestDTO implements Serializable {

	/** */
	private static final long serialVersionUID = 1L;

	public String sessionId;
	
	public BlobStorageOperationResultDTO opResult;
	
}
