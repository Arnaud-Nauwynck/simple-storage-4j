package org.simplestorage4j.opscommon.dto.session;

import java.io.Serializable;
import java.util.List;

import org.simplestorage4j.api.ops.dto.BlobStorageOperationDTO;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * DTO for Executor session info
 */
@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class ExecutorSessionPolledOpsDTO implements Serializable {

	/** */
	private static final long serialVersionUID = 1L;

	public String sessionId;

	public List<BlobStoragePolledOperationDTO> polledOps;

	// ------------------------------------------------------------------------
	
	/**
	 * 
	 */
	@NoArgsConstructor @AllArgsConstructor
	@Getter @Setter
	public static class BlobStoragePolledOperationDTO implements Serializable {

		/** */
		private static final long serialVersionUID = 1L;
		
		public long polledTime;
		
		public BlobStorageOperationDTO op;
		
	}

}
