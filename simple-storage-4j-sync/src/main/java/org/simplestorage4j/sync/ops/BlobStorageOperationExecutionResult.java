package org.simplestorage4j.sync.ops;

import java.util.LinkedHashMap;
import java.util.Map;

import org.simplestorage4j.api.BlobStorageId;
import org.simplestorage4j.sync.ops.dto.BlobStorageOperationExecutionResultDTO;
import org.simplestorage4j.sync.ops.dto.BlobStorageOperationExecutionResultDTO.BlobStorageIOCountDTO;

import com.google.common.collect.ImmutableMap;

import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * immutable class for BlobStorage operation execution result
 */
@RequiredArgsConstructor
public class BlobStorageOperationExecutionResult {

	public final long taskId;

	public final long startTime;
	public final long elapsedMillis;

	public final String errorMessage;
	public final Exception exception;
	
	public final ImmutableMap<BlobStorageId,BlobStorageIOCount> countPerStorage;

	@RequiredArgsConstructor
	public static class BlobStorageIOCount {

		public final long timeMillis;
		public final long ioReadLen;
		public final long ioWriteLen;
		public final int callCount;
		public final int metadataReadCount;
		public final int metadataWriteCount;
		
		public static BlobStorageIOCount ofSum(BlobStorageIOCount left, BlobStorageIOCount right) {
			return new BlobStorageIOCount(
					left.timeMillis + right.timeMillis,
					left.ioReadLen + right.ioReadLen,
					left.ioWriteLen + right.ioWriteLen,
					left.callCount + right.callCount,
					left.metadataReadCount + right.metadataReadCount,
					left.metadataWriteCount + right.metadataWriteCount
					);
		}
		
		public BlobStorageIOCountDTO toDTO() {
			return new BlobStorageIOCountDTO(
					timeMillis, ioReadLen, ioWriteLen, callCount, metadataReadCount, metadataWriteCount);
		}

		public static BlobStorageIOCount fromDTO(BlobStorageIOCountDTO src) {
			return new BlobStorageIOCount(
					src.timeMillis, src.ioReadLen, src.ioWriteLen, src.callCount, src.metadataReadCount, src.metadataWriteCount);
		}

		public static Map<String,BlobStorageIOCountDTO> toDTOsMap(Map<BlobStorageId,BlobStorageIOCount> src) {
			val res = new LinkedHashMap<String,BlobStorageIOCountDTO>();
			for(val e: src.entrySet()) {
				val id = e.getKey().id;
				val dto = e.getValue().toDTO();
				res.put(id, dto);
			}
			return res;
		}

		public static ImmutableMap<BlobStorageId,BlobStorageIOCount> fromDTOsMap(
				Map<String,BlobStorageIOCountDTO> src) {
			val res = ImmutableMap.<BlobStorageId,BlobStorageIOCount>builder();
			for(val e: src.entrySet()) {
				val id = new BlobStorageId(e.getKey());
				val value = fromDTO(e.getValue());
				res.put(id, value);
			}
			return res.build();
		}
		
	}

	// ------------------------------------------------------------------------

	public static BlobStorageOperationExecutionResult of(
			int taskId, long startTime, long elapsedMillis, //
			BlobStorageId storageId, //
			long timeMillis, long ioReadLen, long ioWriteLen, int callCount, int metadataReadCount, int metadataWriteCount
			) {
		return new BlobStorageOperationExecutionResult(taskId, startTime, elapsedMillis, null, null,
			ImmutableMap.of(storageId, 
					new BlobStorageIOCount(timeMillis, ioReadLen, ioWriteLen, callCount, metadataReadCount, metadataWriteCount))); 
	}
	
	public static BlobStorageOperationExecutionResult ofMetadataCall(
			int taskId, long startTime, long elapsedMillis,
			BlobStorageId storageId, int callCount, int metadataReadCount, int metadataWriteCount) {
		return of(taskId, startTime, elapsedMillis, //
				storageId, elapsedMillis, 0L, 0L, callCount, metadataReadCount, metadataWriteCount);
	}

	public static BlobStorageOperationExecutionResult ofIoRead(
			int taskId, long startTime, long elapsedMillis,
			BlobStorageId storageId, long ioReadLen, int callCount) {
		return of(taskId, startTime, elapsedMillis, //
				storageId, elapsedMillis, ioReadLen, 0L, callCount, 0, 0);
	}

	public static BlobStorageOperationExecutionResult ofIoWrite(
			int taskId, long startTime, long elapsedMillis,
			BlobStorageId storageId, long ioWriteLen, int callCount) {
		return of(taskId, startTime, elapsedMillis, //
				storageId, elapsedMillis, 0L, ioWriteLen, callCount, 0, 0);
	}

	public static BlobStorageOperationExecutionResult ofIoRead1(
			int taskId, long startTime, long elapsedMillis,
			BlobStorageId storageId, long ioReadLen) {
		return ofIoRead(taskId, startTime, elapsedMillis, //
				storageId, ioReadLen, 1);
	}

	public static BlobStorageOperationExecutionResult ofIoWrite1(
			int taskId, long startTime, long elapsedMillis,
			BlobStorageId storageId, long ioWriteLen) {
		return ofIoWrite(taskId, startTime, elapsedMillis, //
				storageId, ioWriteLen, 1);
	}

	public BlobStorageOperationExecutionResultDTO toDTO() {
		String exceptionText = (exception != null)? exception.getMessage() : null; // TOADD stack trace?
		return new BlobStorageOperationExecutionResultDTO(
				taskId, startTime, elapsedMillis, errorMessage, exceptionText,
				BlobStorageIOCount.toDTOsMap(countPerStorage));
	}

	public static BlobStorageOperationExecutionResult fromDTO(BlobStorageOperationExecutionResultDTO src) {
		Exception exception = (src.exception != null)? new RuntimeException(src.exception) : null; // TOADD stack trace?
		return new BlobStorageOperationExecutionResult(
				src.taskId, src.startTime, src.elapsedMillis, src.errorMessage, exception,
				BlobStorageIOCount.fromDTOsMap(src.countPerStorage));
	}

}
