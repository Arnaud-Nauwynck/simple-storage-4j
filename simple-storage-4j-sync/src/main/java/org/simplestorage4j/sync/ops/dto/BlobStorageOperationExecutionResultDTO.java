package org.simplestorage4j.sync.ops.dto;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * immutable class for BlobStorage operation execution result
 */
@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class BlobStorageOperationExecutionResultDTO implements Serializable {

	/** for java.io.Serializable */
	private static final long serialVersionUID = 1L;

	public long taskId;

	public long startTime;
	public long elapsedMillis;

	public String errorMessage;
	public String exception;
	
	public Map<String,BlobStorageIOCountDTO> countPerStorage = new LinkedHashMap<>();

	/**
	 * 
	 */
	@NoArgsConstructor @AllArgsConstructor
	@Getter @Setter
	public static class BlobStorageIOCountDTO implements Serializable {

		/** for java.io.Serializable */
		private static final long serialVersionUID = 1L;

		public long timeMillis;
		public long ioReadLen;
		public long ioWriteLen;
		public int callCount;
		public int metadataReadCount;
		public int metadataWriteCount;
		
		public static BlobStorageIOCountDTO ofSum(BlobStorageIOCountDTO left, BlobStorageIOCountDTO right) {
			return new BlobStorageIOCountDTO(
					left.timeMillis + right.timeMillis,
					left.ioReadLen + right.ioReadLen,
					left.ioWriteLen + right.ioWriteLen,
					left.callCount + right.callCount,
					left.metadataReadCount + right.metadataReadCount,
					left.metadataWriteCount + right.metadataWriteCount
					);
		}
	}

}
