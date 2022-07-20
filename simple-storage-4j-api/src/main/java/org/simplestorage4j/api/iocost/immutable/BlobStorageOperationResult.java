package org.simplestorage4j.api.iocost.immutable;

import java.util.Collection;
import java.util.List;

import org.simplestorage4j.api.BlobStorageId;
import org.simplestorage4j.api.iocost.dto.BlobStorageOperationResultDTO;
import org.simplestorage4j.api.util.BlobStorageUtils;

import com.google.common.collect.ImmutableList;

import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * immutable class for BlobStorage operation execution result
 */
@RequiredArgsConstructor
public class BlobStorageOperationResult {

	public final long jobId;
	public final long taskId;

	public final long startTime;
	public final long elapsedMillis;

	/** warning typically for truncated files => only a fragment is available to copy.. even after retries */
	public final ImmutableList<String> warnings;
	
	public final String errorMessage;
	public final Exception exception;

	public final PerBlobStoragesIOTimeResult ioTimePerStorage;

	// ------------------------------------------------------------------------

	public static BlobStorageOperationResult of(
			long jobId, long taskId, long startTime, long elapsedMillis, //
			BlobStorageId storageId, BlobStorageIOTimeResult ioCount
			) {
		return new BlobStorageOperationResult(jobId, taskId, startTime, elapsedMillis, null, null, null, //
				PerBlobStoragesIOTimeResult.of(storageId, ioCount));
	}

	public static BlobStorageOperationResult of(
			long jobId, long taskId, long startTime, long elapsedMillis, //
			BlobStorageId storageId1, BlobStorageIOTimeResult ioCount1,
			BlobStorageId storageId2, BlobStorageIOTimeResult ioCount2
			) {
		return new BlobStorageOperationResult(jobId, taskId, startTime, elapsedMillis, null, null, null, //
				PerBlobStoragesIOTimeResult.of(storageId1, ioCount1, storageId2, ioCount2));
	}

	public static BlobStorageOperationResult of(
			long jobId, long taskId, long startTime, long elapsedMillis, //
			BlobStorageId storageId, //
			long timeMillis, long ioReadLen, long ioWriteLen, int callCount, int metadataReadCount, int metadataWriteCount
			) {
		return new BlobStorageOperationResult(jobId, taskId, startTime, elapsedMillis, null, null, null, //
				PerBlobStoragesIOTimeResult.of(storageId,
					new BlobStorageIOTimeResult(timeMillis, ioReadLen, ioWriteLen, callCount, metadataReadCount, metadataWriteCount)));
	}
	
	public static BlobStorageOperationResult ofMetadataCall(
			long jobId, long taskId, long startTime, long elapsedMillis,
			BlobStorageId storageId, int callCount, int metadataReadCount, int metadataWriteCount) {
		return of(jobId, taskId, startTime, elapsedMillis, //
				storageId, elapsedMillis, 0L, 0L, callCount, metadataReadCount, metadataWriteCount);
	}

	public static BlobStorageOperationResult ofIoRead(
			long jobId, long taskId, long startTime, long elapsedMillis,
			BlobStorageId storageId, long ioReadLen, int callCount) {
		return of(jobId, taskId, startTime, elapsedMillis, //
				storageId, elapsedMillis, ioReadLen, 0L, callCount, 0, 0);
	}

	public static BlobStorageOperationResult ofIoWrite(
			long jobId, long taskId, long startTime, long elapsedMillis,
			BlobStorageId storageId, long ioWriteLen, int callCount) {
		return of(jobId, taskId, startTime, elapsedMillis, //
				storageId, elapsedMillis, 0L, ioWriteLen, callCount, 0, 0);
	}

	public static BlobStorageOperationResult ofIoRead1(
			long jobId, long taskId, long startTime, long elapsedMillis,
			BlobStorageId storageId, long ioReadLen) {
		return ofIoRead(jobId, taskId, startTime, elapsedMillis, //
				storageId, ioReadLen, 1);
	}

	public static BlobStorageOperationResult ofIoWrite1(
			long jobId, long taskId, long startTime, long elapsedMillis,
			BlobStorageId storageId, long ioWriteLen) {
		return ofIoWrite(jobId, taskId, startTime, elapsedMillis, //
				storageId, ioWriteLen, 1);
	}

	public BlobStorageOperationResultDTO toDTO() {
		String exceptionText = (exception != null)? exception.getMessage() : null; // TOADD stack trace?
		return new BlobStorageOperationResultDTO(
				jobId, taskId, startTime, elapsedMillis, warnings, errorMessage, exceptionText,
				ioTimePerStorage.toDTO());
	}

	public static List<BlobStorageOperationResultDTO> toDtos(List<BlobStorageOperationResult> srcs) {
		return BlobStorageUtils.map(srcs, x -> x.toDTO());
	}

	public static BlobStorageOperationResult fromDTO(BlobStorageOperationResultDTO src) {
		val warnings = (src.warnings != null)? ImmutableList.copyOf(src.warnings) : null;
		Exception exception = (src.exception != null)? new RuntimeException(src.exception) : null; // TOADD stack trace?
		return new BlobStorageOperationResult(
				src.jobId, src.taskId, src.startTime, src.elapsedMillis, warnings, src.errorMessage, exception,
				PerBlobStoragesIOTimeResult.fromDTO(src.ioTimePerStorage));
	}

	public static List<BlobStorageOperationResult> fromDTOs(Collection<BlobStorageOperationResultDTO> srcs) {
		return BlobStorageUtils.map(srcs, x -> fromDTO(x));
	}

}
