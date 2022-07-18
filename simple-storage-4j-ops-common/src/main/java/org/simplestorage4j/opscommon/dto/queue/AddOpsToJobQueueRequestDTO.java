package org.simplestorage4j.opscommon.dto.queue;

import java.io.Serializable;
import java.util.List;

import org.simplestorage4j.opscommon.dto.ops.BlobStorageOperationDTO;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor @AllArgsConstructor
public class AddOpsToJobQueueRequestDTO implements Serializable {

	/** */
	private static final long serialVersionUID = 1L;

	public int jobId;

	public List<BlobStorageOperationDTO> ops;

}
