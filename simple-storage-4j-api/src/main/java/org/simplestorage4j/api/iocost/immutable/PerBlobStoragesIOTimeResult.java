package org.simplestorage4j.api.iocost.immutable;

import org.simplestorage4j.api.BlobStorageId;
import org.simplestorage4j.api.iocost.dto.PerBlobStoragesIOTimeResultDTO;

import com.google.common.collect.ImmutableMap;

import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * immutable class for BlobStorage operation execution result
 */
@RequiredArgsConstructor
public class PerBlobStoragesIOTimeResult {

	public final ImmutableMap<BlobStorageId,BlobStorageIOTimeResult> countPerStorage;

	// ------------------------------------------------------------------------

	public static PerBlobStoragesIOTimeResult of(
			BlobStorageId storageId, BlobStorageIOTimeResult ioCount
			) {
		return new PerBlobStoragesIOTimeResult(ImmutableMap.of(storageId, ioCount));
	}

	public static PerBlobStoragesIOTimeResult of(
			BlobStorageId storageId1, BlobStorageIOTimeResult ioCount1,
			BlobStorageId storageId2, BlobStorageIOTimeResult ioCount2
			) {
		return new PerBlobStoragesIOTimeResult(ImmutableMap.of(storageId1, ioCount1, storageId2, ioCount2));
	}

	public static PerBlobStoragesIOTimeResult of(
			BlobStorageId storageId, //
			long elapsedMillis, long ioReadLen, long ioWriteLen, int callCount, int metadataReadCount, int metadataWriteCount
			) {
		return new PerBlobStoragesIOTimeResult(ImmutableMap.of(storageId,
					new BlobStorageIOTimeResult(elapsedMillis, ioReadLen, ioWriteLen, callCount, metadataReadCount, metadataWriteCount)));
	}
	
	public static PerBlobStoragesIOTimeResult ofMetadataCall(
			BlobStorageId storageId, long elapsedMillis, int callCount, int metadataReadCount, int metadataWriteCount) {
		return of(storageId, elapsedMillis, 0L, 0L, callCount, metadataReadCount, metadataWriteCount);
	}

	public static PerBlobStoragesIOTimeResult ofIoRead(
			BlobStorageId storageId, long elapsedMillis, long ioReadLen, int callCount) {
		return of(storageId, elapsedMillis, ioReadLen, 0L, callCount, 0, 0);
	}

	public static PerBlobStoragesIOTimeResult ofIoWrite(
			BlobStorageId storageId, long elapsedMillis, long ioWriteLen, int callCount) {
		return of(storageId, elapsedMillis, 0L, ioWriteLen, callCount, 0, 0);
	}

	public static PerBlobStoragesIOTimeResult ofIoRead1(
			BlobStorageId storageId, long elapsedMillis, long ioReadLen) {
		return ofIoRead(storageId, elapsedMillis, ioReadLen, 1);
	}

	public static PerBlobStoragesIOTimeResult ofIoWrite1(
			BlobStorageId storageId, long elapsedMillis, long ioWriteLen) {
		return ofIoWrite(storageId, elapsedMillis, ioWriteLen, 1);
	}

	public PerBlobStoragesIOTimeResultDTO toDTO() {
		return new PerBlobStoragesIOTimeResultDTO(
				BlobStorageIOTimeResult.toDTOsMap(countPerStorage));
	}

	public static PerBlobStoragesIOTimeResult fromDTO(PerBlobStoragesIOTimeResultDTO src) {
		return new PerBlobStoragesIOTimeResult(
				BlobStorageIOTimeResult.fromDTOsMap(src.perStorageIOCosts));
	}

	public void toTextLine(StringBuilder sb, char storageSep, char sep) {
		for(val e : countPerStorage.entrySet()) {
			sb.append(e.getKey().id);
			sb.append(storageSep);
			e.getValue().toTextLine(sb, sep);
		}
	}
	
	public String toTextLine(char storageSep, char sep) {
		val sb = new StringBuilder(150);
		toTextLine(sb, storageSep, sep);
		return sb.toString();
	}

	@Override
	public String toString() {
		return "{PerBlobStoragesIOTimeResult " + toTextLine('=', ',') + "}";
	}

	
}
