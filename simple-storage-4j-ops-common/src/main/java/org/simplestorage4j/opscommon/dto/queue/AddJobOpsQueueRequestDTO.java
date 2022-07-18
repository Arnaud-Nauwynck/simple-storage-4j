package org.simplestorage4j.opscommon.dto.queue;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor @AllArgsConstructor
public class AddJobOpsQueueRequestDTO implements Serializable {

	/** */
	private static final long serialVersionUID = 1L;

	public String displayMessage;
	public Map<String,String> props;

	public List<BlobStorageOperationDTO> ops;

}
