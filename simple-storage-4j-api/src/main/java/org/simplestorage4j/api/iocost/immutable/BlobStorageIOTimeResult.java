package org.simplestorage4j.api.iocost.immutable;

import java.util.LinkedHashMap;
import java.util.Map;

import org.simplestorage4j.api.BlobStorageId;
import org.simplestorage4j.api.iocost.dto.BlobStorageIOTimeResultDTO;

import com.google.common.collect.ImmutableMap;

import lombok.AllArgsConstructor;
import lombok.val;

@AllArgsConstructor
public class BlobStorageIOTimeResult {

	public final long elapsedTimeMillis;
	// may use ThreadCpuTime, threadUserTime, threadKernelTime ...
	
	public final long ioReadLen;
	public final long ioWriteLen;
	public final int callCount;
	public final int metadataReadCount;
	public final int metadataWriteCount;
	
	public static BlobStorageIOTimeResult ofSum(BlobStorageIOTimeResult left, BlobStorageIOTimeResult right) {
		return new BlobStorageIOTimeResult(
				left.elapsedTimeMillis + right.elapsedTimeMillis,
				left.ioReadLen + right.ioReadLen,
				left.ioWriteLen + right.ioWriteLen,
				left.callCount + right.callCount,
				left.metadataReadCount + right.metadataReadCount,
				left.metadataWriteCount + right.metadataWriteCount
				);
	}
	
	public BlobStorageIOTimeResultDTO toDTO() {
		return new BlobStorageIOTimeResultDTO(
				elapsedTimeMillis, ioReadLen, ioWriteLen, callCount, metadataReadCount, metadataWriteCount);
	}

	public static BlobStorageIOTimeResult fromDTO(BlobStorageIOTimeResultDTO src) {
		return new BlobStorageIOTimeResult(
				src.elapsedTimeMillis, src.ioReadLen, src.ioWriteLen, src.callCount, src.metadataReadCount, src.metadataWriteCount);
	}

	public static Map<String,BlobStorageIOTimeResultDTO> toDTOsMap(Map<BlobStorageId,BlobStorageIOTimeResult> src) {
		val res = new LinkedHashMap<String,BlobStorageIOTimeResultDTO>();
		for(val e: src.entrySet()) {
			val id = e.getKey().id;
			val dto = e.getValue().toDTO();
			res.put(id, dto);
		}
		return res;
	}

	public static ImmutableMap<BlobStorageId,BlobStorageIOTimeResult> fromDTOsMap(
			Map<String,BlobStorageIOTimeResultDTO> src) {
		val res = ImmutableMap.<BlobStorageId,BlobStorageIOTimeResult>builder();
		for(val e: src.entrySet()) {
			val id = new BlobStorageId(e.getKey());
			val value = fromDTO(e.getValue());
			res.put(id, value);
		}
		return res.build();
	}
	
}
