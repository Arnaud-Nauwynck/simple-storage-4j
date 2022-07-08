package org.simplestorage4j.sync.ops.stats;

import org.simplestorage4j.sync.ops.BlobStorageOperationExecutionResult.BlobStorageIOCount;
import org.simplestorage4j.sync.ops.dto.BlobStorageOperationExecutionResultDTO.BlobStorageIOCountDTO;

import lombok.Getter;

@Getter
public class BlobStorageOperationCounter {

	private long timeMillis;
	private long ioReadLen;
	private long ioWriteLen;
	private int callCount;
	private int metadataReadCount;
	private int metadataWriteCount;

	// ------------------------------------------------------------------------
	
	public BlobStorageOperationCounter() {
	}
	
	// ------------------------------------------------------------------------
	
	public synchronized void incr(
			long timeMillis, long ioReadLen, long ioWriteLen, 
			int callCount, int metadataReadCount, int metadataWriteCount) {
		this.timeMillis += timeMillis;
		this.ioReadLen += ioReadLen;
		this.ioWriteLen += ioWriteLen;
		this.callCount += callCount;
		this.metadataReadCount += metadataReadCount;
		this.metadataWriteCount += metadataWriteCount;
	}

	public void incr(BlobStorageIOCount opCount) {
		incr(opCount.timeMillis, opCount.ioReadLen, opCount.ioWriteLen, // 
			opCount.callCount, opCount.metadataReadCount, opCount.metadataWriteCount);
	}

	public synchronized BlobStorageIOCountDTO toDTO() {
		return new BlobStorageIOCountDTO(timeMillis, ioReadLen, ioWriteLen, // 
			callCount, metadataReadCount, metadataWriteCount);
	}

}
