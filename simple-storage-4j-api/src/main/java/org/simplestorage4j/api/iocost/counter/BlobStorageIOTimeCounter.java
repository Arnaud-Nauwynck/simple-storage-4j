package org.simplestorage4j.api.iocost.counter;

import org.simplestorage4j.api.iocost.dto.BlobStorageIOTimeResultDTO;
import org.simplestorage4j.api.iocost.immutable.BlobStorageIOTimeResult;

import lombok.Getter;

@Getter
public class BlobStorageIOTimeCounter {

	private long elapsedTimeMillis;
	// may use ThreadCpuTime, threadUserTime, threadKernelTime ...
	
	private long ioReadLen;
	private long ioWriteLen;
	private int callCount;
	private int metadataReadCount;
	private int metadataWriteCount;

	// ------------------------------------------------------------------------
	
	public BlobStorageIOTimeCounter() {
	}
	
	// ------------------------------------------------------------------------
	
	public synchronized void incr(
			long elapsedTimeMillis, long ioReadLen, long ioWriteLen,
			int callCount, int metadataReadCount, int metadataWriteCount) {
		this.elapsedTimeMillis += elapsedTimeMillis;
		this.ioReadLen += ioReadLen;
		this.ioWriteLen += ioWriteLen;
		this.callCount += callCount;
		this.metadataReadCount += metadataReadCount;
		this.metadataWriteCount += metadataWriteCount;
	}

	public void incr(BlobStorageIOTimeResult opCount) {
		incr(opCount.elapsedTimeMillis, opCount.ioReadLen, opCount.ioWriteLen, //
			opCount.callCount, opCount.metadataReadCount, opCount.metadataWriteCount);
	}

	public void incr(BlobStorageIOTimeResultDTO opCount) {
		incr(opCount.elapsedTimeMillis, opCount.ioReadLen, opCount.ioWriteLen, //
			opCount.callCount, opCount.metadataReadCount, opCount.metadataWriteCount);
	}

	public synchronized BlobStorageIOTimeResultDTO toDTO() {
		return new BlobStorageIOTimeResultDTO(elapsedTimeMillis, ioReadLen, ioWriteLen, //
			callCount, metadataReadCount, metadataWriteCount);
	}

	public synchronized BlobStorageIOTimeResult toImmutable() {
		return new BlobStorageIOTimeResult(elapsedTimeMillis, ioReadLen, ioWriteLen, //
				callCount, metadataReadCount, metadataWriteCount);
	}

}
