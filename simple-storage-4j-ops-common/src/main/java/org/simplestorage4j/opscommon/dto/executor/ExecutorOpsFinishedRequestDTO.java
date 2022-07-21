package org.simplestorage4j.opscommon.dto.executor;

import java.io.Serializable;
import java.util.List;

import org.simplestorage4j.api.iocost.dto.BlobStorageOperationResultDTO;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class ExecutorOpsFinishedRequestDTO implements Serializable {

	/** */
	private static final long serialVersionUID = 1L;

	public String sessionId;
	
	public List<BlobStorageOperationResultDTO> opResults;

	public int pollCount;

}
