package org.simplestorage4j.api.iocost.dto;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * DTO class for BlobStorageIOTimeResult
 */
@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class BlobStorageIOTimeResultDTO implements Serializable {

	/** for java.io.Serializable */
	private static final long serialVersionUID = 1L;

	public long elapsedTimeMillis;
	// may use ThreadCpuTime, threadUserTime, threadKernelTime ...
	
	public long ioReadLen;
	public long ioWriteLen;
	public int callCount;
	public int metadataReadCount;
	public int metadataWriteCount;
	
	public static BlobStorageIOTimeResultDTO ofSum(BlobStorageIOTimeResultDTO left, BlobStorageIOTimeResultDTO right) {
		return new BlobStorageIOTimeResultDTO(
				left.elapsedTimeMillis + right.elapsedTimeMillis,
				left.ioReadLen + right.ioReadLen,
				left.ioWriteLen + right.ioWriteLen,
				left.callCount + right.callCount,
				left.metadataReadCount + right.metadataReadCount,
				left.metadataWriteCount + right.metadataWriteCount
				);
	}

	@Override
	public String toString() {
		return "{" //
				+ "elapsedTimeMillis=" + elapsedTimeMillis //
				+ ((ioReadLen != 0)? ", ioReadLen=" + ioReadLen : "") //
				+ ((ioWriteLen != 0)? ", ioWriteLen=" + ioWriteLen : "") //
				+ ((callCount > 1)? ", callCount=" + callCount : "") //
				+ ((metadataReadCount != 0)? ", metadataReadCount=" + metadataReadCount : "") //
				+ ((metadataWriteCount != 0)? ", metadataWriteCount=" + metadataWriteCount : "") //
				+ "}";
	}

}