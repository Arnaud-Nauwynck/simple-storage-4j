package org.simplestorage4j.api.iocost.immutable;

import org.simplestorage4j.api.BlobStorageId;
import org.simplestorage4j.api.iocost.dto.PerBlobStoragesIOTimeResultDTO;

import com.google.common.collect.ImmutableMap;

import lombok.RequiredArgsConstructor;

/**
 * immutable class for BlobStorage operation execution result
 */
@RequiredArgsConstructor
public class PerBlobStoragesIOTimeResult {

	public final int taskId;

	public final long startTime;
	public final long elapsedMillis;

	public final String errorMessage;
	public final Exception exception;

	public final ImmutableMap<BlobStorageId,BlobStorageIOTimeResult> countPerStorage;

	// ------------------------------------------------------------------------

	public static PerBlobStoragesIOTimeResult of(
			int taskId, long startTime, long elapsedMillis, //
			BlobStorageId storageId, BlobStorageIOTimeResult ioCount
			) {
		return new PerBlobStoragesIOTimeResult(taskId, startTime, elapsedMillis, null, null,
			ImmutableMap.of(storageId, ioCount));
	}

	public static PerBlobStoragesIOTimeResult of(
			int taskId, long startTime, long elapsedMillis, //
			BlobStorageId storageId1, BlobStorageIOTimeResult ioCount1,
			BlobStorageId storageId2, BlobStorageIOTimeResult ioCount2
			) {
		return new PerBlobStoragesIOTimeResult(taskId, startTime, elapsedMillis, null, null,
			ImmutableMap.of(storageId1, ioCount1, storageId2, ioCount2));
	}

	public static PerBlobStoragesIOTimeResult of(
			int taskId, long startTime, long elapsedMillis, //
			BlobStorageId storageId, //
			long timeMillis, long ioReadLen, long ioWriteLen, int callCount, int metadataReadCount, int metadataWriteCount
			) {
		return new PerBlobStoragesIOTimeResult(taskId, startTime, elapsedMillis, null, null,
			ImmutableMap.of(storageId, 
					new BlobStorageIOTimeResult(timeMillis, ioReadLen, ioWriteLen, callCount, metadataReadCount, metadataWriteCount))); 
	}
	
	public static PerBlobStoragesIOTimeResult ofMetadataCall(
			int taskId, long startTime, long elapsedMillis,
			BlobStorageId storageId, int callCount, int metadataReadCount, int metadataWriteCount) {
		return of(taskId, startTime, elapsedMillis, //
				storageId, elapsedMillis, 0L, 0L, callCount, metadataReadCount, metadataWriteCount);
	}

	public static PerBlobStoragesIOTimeResult ofIoRead(
			int taskId, long startTime, long elapsedMillis,
			BlobStorageId storageId, long ioReadLen, int callCount) {
		return of(taskId, startTime, elapsedMillis, //
				storageId, elapsedMillis, ioReadLen, 0L, callCount, 0, 0);
	}

	public static PerBlobStoragesIOTimeResult ofIoWrite(
			int taskId, long startTime, long elapsedMillis,
			BlobStorageId storageId, long ioWriteLen, int callCount) {
		return of(taskId, startTime, elapsedMillis, //
				storageId, elapsedMillis, 0L, ioWriteLen, callCount, 0, 0);
	}

	public static PerBlobStoragesIOTimeResult ofIoRead1(
			int taskId, long startTime, long elapsedMillis,
			BlobStorageId storageId, long ioReadLen) {
		return ofIoRead(taskId, startTime, elapsedMillis, //
				storageId, ioReadLen, 1);
	}

	public static PerBlobStoragesIOTimeResult ofIoWrite1(
			int taskId, long startTime, long elapsedMillis,
			BlobStorageId storageId, long ioWriteLen) {
		return ofIoWrite(taskId, startTime, elapsedMillis, //
				storageId, ioWriteLen, 1);
	}

	public PerBlobStoragesIOTimeResultDTO toDTO() {
		String exceptionText = (exception != null)? exception.getMessage() : null; // TOADD stack trace?
		return new PerBlobStoragesIOTimeResultDTO(
				taskId, startTime, elapsedMillis, errorMessage, exceptionText,
				BlobStorageIOTimeResult.toDTOsMap(countPerStorage));
	}

	public static PerBlobStoragesIOTimeResult fromDTO(PerBlobStoragesIOTimeResultDTO src) {
		Exception exception = (src.exception != null)? new RuntimeException(src.exception) : null; // TOADD stack trace?
		return new PerBlobStoragesIOTimeResult(
				src.taskId, src.startTime, src.elapsedMillis, src.errorMessage, exception,
				BlobStorageIOTimeResult.fromDTOsMap(src.perStorageIOCosts));
	}

}
