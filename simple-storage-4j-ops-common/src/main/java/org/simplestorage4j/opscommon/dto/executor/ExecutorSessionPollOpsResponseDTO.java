package org.simplestorage4j.opscommon.dto.executor;

import java.io.Serializable;
import java.util.List;

import org.simplestorage4j.opscommon.dto.ops.BlobStorageOperationDTO;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor @AllArgsConstructor
public class ExecutorSessionPollOpsResponseDTO implements Serializable {

	/** */
	private static final long serialVersionUID = 1L;

	public List<BlobStorageOperationDTO> ops;

}
